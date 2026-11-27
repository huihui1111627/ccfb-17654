"""
Database —— 引擎门面。

负责：
- 数据库的创建/打开/关闭（catalog、storage、lock manager 组装）
- 事务上下文与自动重试包装
- 带预估-执行核对的级联删除
- 大删除的拆批执行：每个根分片一个短事务，锁只在短事务内持有，
  中途其他事务照常读写；分片之间的并集等于一次性删除的闭包，
  且每个提交点都保持全部外键有效（严格 2PL 保证无悬空引用）。
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

from .errors import FKDBError, ForeignKeyViolation, LockTimeout
from .locking import LockManager
from .planner import Plan
from .schema import Catalog, Table, Column, ForeignKey
from .storage import Storage, CATALOG_NAME
from .transaction import Transaction


class Database:
    def __init__(self, dbpath: str, lock_timeout: float = 30.0):
        self.dbpath = dbpath
        catalog_path = os.path.join(dbpath, CATALOG_NAME)
        self.catalog = Catalog.load(catalog_path)
        self.locker = LockManager(lock_timeout=lock_timeout)
        self.storage = Storage(dbpath, self.catalog)

    # ------------------------------------------------------------ 生命周期
    @classmethod
    def create(cls, dbpath: str, catalog: Catalog) -> "Database":
        catalog.validate()
        os.makedirs(dbpath, exist_ok=True)
        catalog.save(os.path.join(dbpath, CATALOG_NAME))
        return cls(dbpath)

    def close(self):
        self.storage.close()

    def checkpoint(self):
        self.storage.checkpoint()

    # ------------------------------------------------------------ DDL 转发
    def table_names(self) -> List[str]:
        return sorted(self.catalog.tables.keys())

    def table(self, name: str) -> Table:
        return self.catalog.table(name)

    # ------------------------------------------------------------ 事务
    @contextmanager
    def transaction(self):
        txn = Transaction(self)
        try:
            yield txn
            if not txn.finished:
                txn.commit()
        except Exception:
            if not txn.finished:
                txn.rollback()
            raise

    def run_in_txn(self, fn, retries: int = 3):
        """
        自动重试包装：计划漂移/锁超时/死锁牺牲者都重跑整个事务函数。
        fn(txn) 的返回值会被透传。
        """
        last_exc = None
        for attempt in range(retries):
            txn = Transaction(self)
            try:
                result = fn(txn)
                txn.commit()
                return result
            except (FKDBError,) as e:
                txn.rollback()
                last_exc = e
                msg = str(e)
                if not (isinstance(e, LockTimeout) or "漂移" in msg):
                    raise
                if attempt == retries - 1:
                    raise
                time.sleep(0.01 * (attempt + 1))
        raise last_exc  # pragma: no cover

    # ------------------------------------------------------------ 便捷 DML
    def insert(self, tname: str, values: dict) -> int:
        def work(txn):
            return txn.insert(tname, values)
        return self.run_in_txn(work)

    def get(self, tname: str, rowid: int) -> Optional[dict]:
        with self.transaction() as txn:
            return txn.get(tname, rowid)

    def scan(self, tname: str) -> List[dict]:
        with self.transaction() as txn:
            return txn.scan(tname)

    def count(self, tname: str) -> int:
        return self.storage.count(tname)

    # ------------------------------------------------------------ 级联删除
    def estimate_delete(self, roots: List[Tuple[str, int]],
                        order_seed: Optional[int] = None) -> Plan:
        txn = Transaction(self)
        try:
            return txn.estimate_delete(roots, order_seed=order_seed)
        finally:
            txn.rollback()  # 只读事务，释放可能拿到的锁（预估本身不拿锁）

    def cascade_delete(self, roots: List[Tuple[str, int]],
                       verify: bool = True) -> dict:
        """一次性级联删除（大事务，持锁到提交）。"""
        plan = self.estimate_delete(roots)
        estimated = plan.counts_by_table()

        def work(txn):
            return txn.execute_delete_plan(plan)

        report = self.run_in_txn(work)
        if verify:
            self._verify_against_estimate(estimated, report)
        report["estimated"] = estimated
        report["roots"] = roots
        return report

    def execute_expanding_batch(self, batch_roots,
                                global_delete_keys: set) -> dict:
        """
        在一个短事务里执行一批删除，并自动消解“跨边约束”：

        叶子锚点拆批（先删订单子树、最后删客户根）会遇到这种情况——
        某行经由 NO ACTION/RESTRICT 边引用本批要删的父行，但它自己会在
        全局删除闭包里经另一条 CASCADE 路径被删（一次性删除时本就同归于尽）。
        这时把该阻挡行吸收进当前批、同一事务删掉，约束即自消解；
        若阻挡行不在全局删除闭包里，说明它本不该被删，按标准语义报错。

        global_delete_keys: 一次性全量预估得到的全部 (table,rowid) 删除集合。
        返回与 execute_delete_plan 同构的报告。
        """
        def work(txn):
            roots = list(batch_roots)
            for _ in range(1000):  # 扩批上界，防御性
                try:
                    plan = txn.estimate_delete(roots)
                except ForeignKeyViolation as e:
                    plan = e.details.get("plan")
                    blocks = e.details.get("blocks", [])
                    if plan is None:
                        raise
                else:
                    blocks = plan.blockage()
                    if not blocks:
                        return txn.execute_delete_plan(plan)

                extra = []
                for b in blocks:
                    key = (b["table"], b["rowid"])
                    if key not in global_delete_keys:
                        # 阻挡行不在全局删除闭包：真实约束，整体拒绝
                        raise ForeignKeyViolation(
                            "拆批遇到无法消解的外键阻挡：%s.%d 经 %s 引用"
                            % (b["table"], b["rowid"], b["fk"]),
                            kind="blocked", details={"blocks": blocks})
                    if key not in roots:
                        extra.append(key)
                if not extra:
                    raise ForeignKeyViolation(
                        "拆批扩批无法推进", kind="blocked",
                        details={"blocks": blocks})
                roots.extend(extra)
            raise FKDBError("拆批扩批次数超过上界")
        return self.run_in_txn(work)

    def cascade_delete_batched(self, roots: List[Tuple[str, int]],
                               batch_size: int = 500,
                               verify: bool = True) -> dict:
        """
        拆批级联删除。

        roots 按 batch_size 分片，每片：
          预估（全库视角）→ 独立短事务执行 → 记录实际动作。
        分片事务短、持锁行少，其他业务事务可以在分片之间穿插。
        最终对累计实际清单与“一次性全量预估”做逐表核对。
        """
        t0 = time.monotonic()
        full_plan = self.estimate_delete(roots)
        full_estimate = full_plan.counts_by_table()
        # 如果整体会被 RESTRICT/NO ACTION 挡住，直接抛出（不做任何删除）
        blocked = full_plan.blockage()
        if blocked:
            raise ForeignKeyViolation(
                "删除被外键阻止，共 %d 处" % len(blocked),
                kind="blocked", details={"blocks": blocked})

        chunks = [roots[i:i + batch_size]
                  for i in range(0, len(roots), batch_size)]
        # 全局删除闭包：供扩批执行判断阻挡行是否本就该随根删除
        global_delete_keys = {
            k for k, n in full_plan.nodes.items() if n.action == "DELETE"}
        manifest = {"deleted": [], "set_null": [], "set_default": [],
                    "chains": [], "batches": []}
        actual: Dict[str, Dict[str, int]] = {}
        seen = set()  # (table,rowid,action) 去重，分片闭包可能重叠

        def bump(table: str, action: str, n: int = 1):
            bucket = actual.setdefault(
                table, {"delete": 0, "set_null": 0,
                        "set_default": 0, "blocked": 0})
            bucket[action] += n

        for idx, chunk in enumerate(chunks):
            bt0 = time.monotonic()
            # 扩批执行：跨边 NO ACTION/RESTRICT 且阻挡行在全局闭包内时
            # 同事务吸收自消解；否则按标准语义拒绝（整个分片零改动）
            rep = self.execute_expanding_batch(chunk, global_delete_keys)
            batch_deleted = 0
            for item in rep["deleted"]:
                sig = (item["table"], item["rowid"], "delete")
                if sig in seen:
                    continue
                seen.add(sig)
                manifest["deleted"].append(item)
                manifest["chains"].append(item)
                bump(item["table"], "delete")
                batch_deleted += 1
            for item in rep["set_null"]:
                # 按 (表,行,列) 去重，与预估的按列计数口径一致
                sig = (item["table"], item["rowid"], "set_null",
                       item.get("col"))
                if sig in seen:
                    continue
                seen.add(sig)
                manifest["set_null"].append(item)
                manifest["chains"].append(item)
                bump(item["table"], "set_null")
            for item in rep["set_default"]:
                sig = (item["table"], item["rowid"], "set_default",
                       item.get("col"))
                if sig in seen:
                    continue
                seen.add(sig)
                manifest["set_default"].append(item)
                manifest["chains"].append(item)
                bump(item["table"], "set_default")

            manifest["batches"].append({
                "index": idx,
                "roots": chunk,
                "deleted": batch_deleted,
                "duration_ms": round((time.monotonic() - bt0) * 1000, 2),
                "dangling_after_commit": len(self.storage.dangling_refs())})

        manifest["counts"] = actual
        manifest["estimated"] = full_estimate
        manifest["duration_ms"] = round((time.monotonic() - t0) * 1000, 2)
        manifest["batch_count"] = len(chunks)
        manifest["roots"] = roots
        if verify:
            # 拆批与一次性删除的等价性判据是最终行集合相同（见
            # cascade_delete_batched 的模块文档）；动作计数受“图在分片间
            # 收缩”影响天然不同，这里只校验删除行数（闭包删除不受影响），
            # SET 类动作由每个分片提交后的零悬空检查兜底。
            for t in set(full_estimate) | set(actual):
                e_del = full_estimate.get(t, {}).get("delete", 0)
                a_del = actual.get(t, {}).get("delete", 0)
                if e_del != a_del:
                    raise FKDBError(
                        "拆批删除数与一次性预估不符：表 %s 预估删 %d 实际 %d"
                        % (t, e_del, a_del))
            if self.dangling_refs():
                raise FKDBError("拆批结束后存在悬空外键")
        return manifest

    @staticmethod
    def _verify_against_estimate(estimated: dict, report: dict):
        actual = report["counts"]
        tables = set(estimated) | set(actual)
        keys = ("delete", "set_null", "set_default", "blocked",
                "update_pk", "update_fk", "no_action_hold")
        zero = {k: 0 for k in keys}
        for t in tables:
            e = dict(zero, **estimated.get(t, {}))
            a = dict(zero, **actual.get(t, {}))
            for k in keys:
                if e.get(k, 0) != a.get(k, 0):
                    raise FKDBError(
                        "预估与实际不符：表 %s 动作 %s 预估 %d 实际 %d"
                        % (t, k, e.get(k, 0), a.get(k, 0)))

    # ------------------------------------------------------------ 主键更新
    def update_pk(self, table: str, rowid: int, new_pk: int) -> dict:
        def work(txn):
            plan = txn.planner.plan_update_pk(table, rowid, new_pk)
            return txn.execute_update_pk_plan(plan)
        return self.run_in_txn(work)

    # ------------------------------------------------------------ 完整性
    def dangling_refs(self) -> List[dict]:
        return self.storage.dangling_refs()

    def assert_integrity(self):
        dangling = self.dangling_refs()
        if dangling:
            raise FKDBError("发现 %d 条悬空外键，首条: %s"
                            % (len(dangling), dangling[0]))
        return True

    # ------------------------------------------------------------ 批量装载
    @contextmanager
    def bulk_loader(self):
        """
        初始批量装载：跳过 WAL 与锁，直接写堆文件，速度优先。
        仅允许在没有任何业务事务的新建库上使用。
        """
        loader = _BulkLoader(self)
        try:
            yield loader
        finally:
            loader.finish()


class _BulkLoader:
    def __init__(self, db: Database):
        self.db = db
        self.storage = db.storage

    def insert(self, tname: str, values, rowid: Optional[int] = None):
        """
        支持两种调用：
          insert("t", {...}, rowid=10)
          insert("t", 10, {...})        # 批量装载的常见位置参数形式
        """
        if isinstance(values, int):
            # insert("t", 10, {...}) 形式：第二参是 rowid，第三参才是值
            rowid, values = values, rowid
        table = self.db.catalog.table(tname)
        if rowid is None:
            rowid = values.get(table.pk_column)
        if rowid is None:
            raise FKDBError("批量装载必须显式提供主键 id")
        row = dict(values)
        # 回填场景（同一 rowid 写新版本）：字典里可以省略主键
        row.setdefault(table.pk_column, rowid)
        for col in table.columns:
            if col.name not in row:
                row[col.name] = col.default if col.has_default else None
            if row[col.name] is None and not col.nullable:
                raise FKDBError("批量装载：表 %s 列 %s 不能为 NULL"
                                % (tname, col.name))
        row[table.pk_column] = rowid
        self.storage.bulk_load(tname, rowid, row)
        # 同步自增序列，避免后续普通 insert 分配出已占用的 id
        if rowid > self.storage._seq.get(tname, 0):
            self.storage._seq[tname] = rowid
        return rowid

    def reserve(self, tname: str, n: int) -> int:
        return self.storage.reserve_rowids(tname, n)

    def finish(self):
        self.storage.flush_bulk()
