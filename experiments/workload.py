"""
并发工作负载：在删除进行期间模拟正常电商业务。

每个 worker 线程循环执行短事务：
- 下单（insert order + order_items，引用其他客户，制造外键检查/锁等待）
- 查询自己的订单（读事务，测读等待）
- 浏览商品（纯读）
- 更新库存（写不相关的 products 行）

每个操作记录排队/持锁等待时间，由 LockManager 的统计 + 操作耗时汇总。
"""
from __future__ import annotations

import random
import threading
import time
from typing import Dict, List

from mini_fkdb import Database, LockTimeout, FKDBError


class WorkerStats:
    def __init__(self):
        self._mu = threading.Lock()
        self.ops: List[dict] = []
        self.errors: List[str] = []
        self.dangling_observed = 0

    def record(self, kind: str, latency_ms: float, wait_ms: float,
               ok: bool):
        with self._mu:
            self.ops.append({"kind": kind, "latency_ms": latency_ms,
                             "wait_ms": wait_ms, "ok": ok})

    def record_error(self, msg: str):
        with self._mu:
            self.errors.append(msg)

    def summary(self) -> dict:
        with self._mu:
            if not self.ops:
                return {"total": 0}
            by_kind: Dict[str, List[dict]] = {}
            for o in self.ops:
                by_kind.setdefault(o["kind"], []).append(o)
            out = {"total": len(self.ops),
                   "failed": sum(1 for o in self.ops if not o["ok"]),
                   "errors": len(self.errors),
                   "dangling_observed": self.dangling_observed,
                   "by_kind": {}}
            waits = [o["wait_ms"] for o in self.ops]
            lats = [o["latency_ms"] for o in self.ops]
            out["wait_ms"] = _stats(waits)
            out["latency_ms"] = _stats(lats)
            for k, lst in by_kind.items():
                out["by_kind"][k] = {
                    "count": len(lst),
                    "wait_ms": _stats([o["wait_ms"] for o in lst]),
                    "latency_ms": _stats([o["latency_ms"] for o in lst])}
            return out


def _stats(xs: List[float]) -> dict:
    if not xs:
        return {"min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "sum": 0}
    s = sorted(xs)
    n = len(s)
    return {
        "min": round(s[0], 3),
        "max": round(s[-1], 3),
        "avg": round(sum(s) / n, 3),
        "p50": round(s[n // 2], 3),
        "p95": round(s[min(n - 1, int(n * 0.95))], 3),
        "sum": round(sum(s), 3)}


class BusinessWorkload:
    """
    在 stop_event 被置位前，让 n_workers 个线程持续跑业务短事务。
    所有业务只碰 safe_customer_ids（不被删除的客户），因此理论上不该
    被级联删除阻塞；若锁粒度过粗/持有过久，等待时间会明显上升。
    """

    def __init__(self, db: Database, safe_customer_ids: List[int],
                 product_ids: List[int], address_ids: List[int],
                 n_workers: int = 4, seed: int = 77,
                 hot_row_ids: List[tuple] = None):
        self.db = db
        self.safe = list(safe_customer_ids)
        self.products = list(product_ids)
        self.addresses = list(address_ids)
        # 热行：删除目标本身（如大客户 (customers,1)）。访问它会与删除
        # 事务的行 X 锁直接竞争，用来测量“别人被卡多久”。
        self.hot_rows = list(hot_row_ids or [])
        self.n_workers = n_workers
        self.stats = WorkerStats()
        self._stop = threading.Event()
        self._rng = random.Random(seed)
        self._threads: List[threading.Thread] = []

    def start(self):
        for i in range(self.n_workers):
            t = threading.Thread(target=self._loop, args=(i,), daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=30)

    def _loop(self, worker_id: int):
        rng = random.Random(worker_id * 1009 + 13)
        while not self._stop.is_set():
            roll = rng.random()
            try:
                if roll < 0.25:
                    self._read_hot_row(rng)
                elif roll < 0.55:
                    self._place_order(rng)
                elif roll < 0.8:
                    self._query_orders(rng)
                elif roll < 0.92:
                    self._browse_products(rng)
                else:
                    self._update_stock(rng)
            except (LockTimeout, FKDBError) as e:
                self.stats.record_error(type(e).__name__ + ": " + str(e)[:120])
            except Exception as e:  # pragma: no cover
                self.stats.record_error(repr(e))

    def _read_hot_row(self, rng: random.Random):
        """读删除目标热行：与级联删除的行 X 锁直接竞争。"""
        if not self.hot_rows:
            return self._browse_products(rng)
        tname, rid = rng.choice(self.hot_rows)

        def work(txn):
            txn.get(tname, rid)  # 拿 S 锁，删除持 X 锁时会等待
        self._time_txn("hot_row_read", work)

    def _time_txn(self, kind: str, fn):
        t0 = time.monotonic()
        txn_id = None
        with self.db.transaction() as txn:
            txn_id = txn.txn_id
            fn(txn)
        latency = (time.monotonic() - t0) * 1000
        wait = self.db.locker.stats(txn_id).get("wait_ms", 0.0)
        self.stats.record(kind, latency, wait, ok=True)

    def _place_order(self, rng: random.Random):
        cid = rng.choice(self.safe)
        addrs = self.db.storage.find_by_fk("addresses",
                                           "fk_address_customer", cid)
        if not addrs:
            return self._browse_products(rng)
        addr = rng.choice(addrs)
        pid = rng.choice(self.products)
        prod = self.db.get("products", pid)

        def work(txn):
            oid = txn.insert("orders", {
                "customer_id": cid, "approver_id": None,
                "address_id": addr, "status": "pending",
                "total": prod["price"], "created_at": "2026-11-28T00:00:00"})
            txn.insert("order_items", {
                "order_id": oid, "product_id": pid, "qty": 1,
                "unit_price": prod["price"]})
        self._time_txn("place_order", work)

    def _query_orders(self, rng: random.Random):
        cid = rng.choice(self.safe)

        def work(txn):
            txn.find_by_fk("orders", "fk_order_customer", cid)
            # 顺带核对引用完整性：读到的订单客户必须存在
            for o in txn.find_by_fk("orders", "fk_order_customer", cid)[:3]:
                if txn.get("customers", o["customer_id"]) is None:
                    self.stats.dangling_observed += 1
        self._time_txn("query_orders", work)

    def _browse_products(self, rng: random.Random):
        pid = rng.choice(self.products)

        def work(txn):
            txn.get("products", pid)
            txn.scan("categories")
        self._time_txn("browse", work)

    def _update_stock(self, rng: random.Random):
        pid = rng.choice(self.products)

        def work(txn):
            row = txn.must_get("products", pid)
            txn.update("products", pid,
                       {"stock": max(0, row["stock"] - 1)})
        self._time_txn("update_stock", work)
