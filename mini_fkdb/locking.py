"""
行级锁管理器（严格两阶段锁的锁部分）。

- 每个锁键 (table, rowid) 支持 S / X 两种模式，FIFO 公平授予，防止写者饿死。
- 死锁检测：等待时构建 wait-for 图，发现环立即把当前事务作为牺牲者中止。
- 统计每个事务的锁等待次数和累计等待时间，供并发实验对比。
- 事务结束时一次性释放全部锁（strict 2PL），这保证别的事务在任何时刻
  都不可能读到“父行已删、子行还挂着”的中间态。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .errors import LockTimeout

S = "S"
X = "X"


def _conflict(held: str, want: str) -> bool:
    return held == X or want == X


@dataclass
class _Entry:
    holders: Dict[int, str] = field(default_factory=dict)  # txn_id -> mode
    waiters: List[int] = field(default_factory=list)       # txn_id FIFO


class LockManager:
    def __init__(self, deadlock_interval: float = 0.05,
                 lock_timeout: float = 30.0):
        self._mu = threading.RLock()
        self._cond = threading.Condition(self._mu)
        self._entries: Dict[Tuple[str, int], _Entry] = {}
        # txn -> set(keys)，事务结束统一释放
        self._txn_keys: Dict[int, Set[Tuple[str, int]]] = {}
        # 统计
        self._wait_count: Dict[int, int] = {}
        self._wait_nanos: Dict[int, int] = {}
        self._deadlock_interval = deadlock_interval
        self._lock_timeout = lock_timeout

    # ------------------------------------------------------------ 加锁
    # 分块大小：批量加/放锁时每处理这么多 key 就释放一次全局互斥量，
    # 让并发业务线程有机会拿到自己的短锁，避免百万行删除长期独占 mutex。
    CHUNK = 2000

    def acquire_many(self, txn_id: int, keys, mode: str = X):
        """
        批量加锁快路径。按 key 排序后分块授予：
        - 每块在全局互斥量内检查冲突，无冲突即时授予，无线程切换；
        - 块间释放互斥量，业务线程可穿插拿自己的短锁；
        - 行锁一旦授予仍持有到事务结束（严格 2PL），分块只影响 mutex
          的持有时间，不影响隔离性。真冲突时退化到逐键等待/死锁检测。
        """
        ordered = sorted(set(keys))
        for start in range(0, len(ordered), self.CHUNK):
            chunk = ordered[start:start + self.CHUNK]
            conflict = False
            with self._cond:
                self._txn_keys.setdefault(txn_id, set())
                for key in chunk:
                    entry = self._entries.get(key)
                    if entry is None:
                        continue
                    if entry.waiters:
                        conflict = True
                        break
                    for holder, held_mode in entry.holders.items():
                        if holder != txn_id and _conflict(held_mode, mode):
                            conflict = True
                            break
                    if conflict:
                        break
            if conflict:
                # 整块退化为逐键获取（保留 FIFO/死锁检测语义）
                for key in chunk:
                    self.acquire(txn_id, key, mode)
                continue
            with self._cond:
                txn_set = self._txn_keys.setdefault(txn_id, set())
                for key in chunk:
                    entry = self._entries.get(key)
                    if entry is None:
                        entry = _Entry()
                        self._entries[key] = entry
                    entry.holders[txn_id] = mode  # 快路径无 S 锁，直接 X
                    txn_set.add(key)

    def acquire(self, txn_id: int, key: Tuple[str, int], mode: str):
        deadline = time.monotonic() + self._lock_timeout
        waited = False
        start = time.monotonic()
        with self._cond:
            self._txn_keys.setdefault(txn_id, set())
            entry = self._entries.get(key)
            if entry is None:
                entry = _Entry()
                self._entries[key] = entry

            if not self._can_grant(entry, txn_id, mode):
                entry.waiters.append(txn_id)
                waited = True
                next_check = time.monotonic()
                try:
                    while not self._can_grant(entry, txn_id, mode):
                        timeout = max(0.0, deadline - time.monotonic())
                        if timeout <= 0.0:
                            raise LockTimeout(
                                "事务 %d 等待锁 %s 超时" % (txn_id, key))
                        # 周期性做死锁检测；也靠超时兜底
                        until = min(timeout, self._deadlock_interval)
                        self._cond.wait(timeout=until)
                        now = time.monotonic()
                        if now >= next_check:
                            next_check = now + self._deadlock_interval
                            if self._has_deadlock(txn_id):
                                raise LockTimeout(
                                    "检测到死锁，事务 %d 被选为牺牲者"
                                    % txn_id)
                finally:
                    if txn_id in entry.waiters:
                        entry.waiters.remove(txn_id)
                    self._cond.notify_all()

            # 授予
            old = entry.holders.get(txn_id)
            if old is None:
                entry.holders[txn_id] = mode
            elif old == S and mode == X:
                entry.holders[txn_id] = X  # 升级
            self._txn_keys[txn_id].add(key)

            if waited:
                self._wait_count[txn_id] = self._wait_count.get(txn_id, 0) + 1
                self._wait_nanos[txn_id] = self._wait_nanos.get(txn_id, 0) + \
                    int((time.monotonic() - start) * 1e9)

    def _can_grant(self, entry: _Entry, txn_id: int, mode: str) -> bool:
        # FIFO：前面有人排队就不能插队（重入自己已持有的锁除外）
        if entry.waiters and entry.waiters[0] != txn_id:
            return False
        for holder, held_mode in entry.holders.items():
            if holder == txn_id:
                continue
            if _conflict(held_mode, mode):
                return False
        return True

    # ------------------------------------------------------------ 释放
    def release_all(self, txn_id: int):
        # 先把该事务的全部锁键取出，再分块释放，块间让出互斥量
        with self._cond:
            keys = list(self._txn_keys.pop(txn_id, set()))
        for start in range(0, len(keys), self.CHUNK):
            chunk = keys[start:start + self.CHUNK]
            with self._cond:
                for key in chunk:
                    entry = self._entries.get(key)
                    if entry is None:
                        continue
                    entry.holders.pop(txn_id, None)
                    if not entry.holders and not entry.waiters:
                        del self._entries[key]
                self._cond.notify_all()

    # -------------------------------------------------------- 死锁检测
    def _has_deadlock(self, root: int) -> bool:
        """
        wait-for 图：waiter -> 所有挡住它的 holder。
        从 root 出发 DFS，能回到 root 就是死锁。
        """
        # 先把每个事务当前在等的键算出来
        waiting_on: Dict[int, Set[int]] = {}
        for entry in self._entries.values():
            for w in entry.waiters:
                blockers = {h for h, hm in entry.holders.items()
                            if h != w and _conflict(hm, X)}
                if blockers:
                    waiting_on.setdefault(w, set()).update(blockers)
        if root not in waiting_on:
            return False
        stack = list(waiting_on[root])
        seen: Set[int] = set()
        while stack:
            cur = stack.pop()
            if cur == root:
                return True
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(waiting_on.get(cur, ()))
        return False

    # ------------------------------------------------------------ 统计
    def stats(self, txn_id: int) -> Dict[str, float]:
        with self._mu:
            return {
                "wait_count": self._wait_count.get(txn_id, 0),
                "wait_ms": self._wait_nanos.get(txn_id, 0) / 1e6,
            }

    def all_stats(self) -> Dict[int, Dict[str, float]]:
        with self._mu:
            return {t: {"wait_count": self._wait_count.get(t, 0),
                        "wait_ms": self._wait_nanos.get(t, 0) / 1e6}
                    for t in set(self._wait_count) | set(self._wait_nanos)}

    def reset_stats(self):
        with self._mu:
            self._wait_count.clear()
            self._wait_nanos.clear()

    def held_locks(self, txn_id: int) -> List[Tuple[str, int, str]]:
        with self._mu:
            return [(k[0], k[1], e.holders[txn_id])
                    for k, e in self._entries.items() if txn_id in e.holders]
