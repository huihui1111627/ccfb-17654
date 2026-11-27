"""
事务对象与 DML 执行。

隔离级别：严格两阶段锁（strict 2PL）下的可串行化。
- 读拿 S 锁、写拿 X 锁，全部锁持有到事务提交/回滚才释放。
- 插入/更新子行时对父行加 S 锁并做外键存在性检查；删除父行要 X 锁。
  两类操作在同一把行锁上互斥，因此“删掉父行的同时插入悬空子行”在
  锁层面就不可能发生——任何时刻别的事务读到的数据都不会有悬空外键。
- 事务自己的改动放在 overlay 里，提交时一次性走 WAL；回滚只丢 overlay。
"""
from __future__ import annotations

import itertools
import threading
from typing import Dict, List, Optional, Tuple

from . import codec
from .errors import (FKDBError, ForeignKeyViolation, NotFound,
                     TransactionAborted, UniqueViolation)
from .locking import LockManager, S, X
from .planner import Plan, Planner
from .schema import Catalog, Table

_TOMB = object()  # overlay 中的删除标记


class Transaction:
    _id_counter = itertools.count(1)
    _id_lock = threading.Lock()

    def __init__(self, db: "Database"):
        with Transaction._id_lock:
            self.txn_id = next(Transaction._id_counter)
        self.db = db
        self.storage = db.storage
        self.catalog = db.catalog
        self.locker: LockManager = db.locker
        self.planner = Planner(db.catalog, db.storage)
        # overlay: table -> rowid -> dict(行) 或 _TOMB
        self.overlay: Dict[str, Dict[int, object]] = {}
        # 轻量变更：(table, rowid, op, payload_or_None)，提交时才分配 lsn 打包
        self._pending: List[Tuple[str, int, int, Optional[bytes]]] = []
        # (table,rowid) -> 在 _pending 中的下标，避免大删除时 O(n^2) 查找
        self._pending_idx: Dict[Tuple[str, int], int] = {}
        self.mutations: List[Tuple[int, str, int, int, Optional[bytes]]] = []
        self.finished = False
        self.failed = False
        self._dirty: Dict[str, set] = {}
        # 可延迟外键检查：环插入时先登记，提交前统一校验
        self._deferred_checks: List[Tuple[Table, dict]] = []
        self._deferred = False

    # ============================================================ 内部工具
    def _mark_dirty(self, tname: str, rowid: int):
        self._dirty.setdefault(tname, set()).add(rowid)

    def _overlay_get(self, tname: str, rowid: int):
        """返回 row dict / _TOMB / None（未触碰）。"""
        return self.overlay.get(tname, {}).get(rowid)

    def _current(self, tname: str, rowid: int):
        ov = self._overlay_get(tname, rowid)
        if ov is _TOMB:
            return None
        if ov is not None:
            return ov
        return self.storage.get(tname, rowid)

    def _fail(self, exc: Exception) -> Exception:
        self.failed = True
        return exc

    # ============================================================ 锁辅助
    def lock_read(self, tname: str, rowid: int):
        self.locker.acquire(self.txn_id, (tname, rowid), S)

    def lock_write(self, tname: str, rowid: int):
        self.locker.acquire(self.txn_id, (tname, rowid), X)

    # ============================================================ 读
    def get(self, tname: str, rowid: int, lock: str = S):
        if self.finished:
            raise TransactionAborted("事务已结束")
        if lock == S:
            self.lock_read(tname, rowid)
        row = self._current(tname, rowid)
        if row is None:
            return None
        return dict(row)

    def must_get(self, tname: str, rowid: int) -> dict:
        row = self.get(tname, rowid)
        if row is None:
            raise self._fail(NotFound("%s.%d 不存在" % (tname, rowid)))
        return row

    def scan(self, tname: str) -> List[dict]:
        """整表读（拿全表行的 S 锁，保证扫描期间别人不能改这些行）。"""
        if self.finished:
            raise TransactionAborted("事务已结束")
        rows = []
        for rowid in self.storage.all_rowids(tname):
            self.lock_read(tname, rowid)
        for rowid in self.storage.all_rowids(tname):
            cur = self._current(tname, rowid)
            if cur is not None:
                rows.append(dict(cur))
        # overlay 里本事务新插入的行
        for rowid, ov in self.overlay.get(tname, {}).items():
            if ov is not _TOMB and rowid not in self.storage.pk[tname]:
                rows.append(dict(ov))
        return rows

    def find_by_fk(self, tname: str, fk_name: str, value: object,
                   lock: str = S) -> List[dict]:
        ids = set(self.storage.find_by_fk(tname, fk_name, value))
        # overlay 里新增/改过的同值行
        for rowid, ov in self.overlay.get(tname, {}).items():
            if ov is not _TOMB and ov.get(
                    self.catalog.fk_by_name(fk_name)[1].columns[0]) == value:
                ids.add(rowid)
        out = []
        for rid in sorted(ids):
            row = self.get(tname, rid, lock=lock)
            if row is not None:
                out.append(row)
        return out

    # ============================================================ 写（基础）
    def _validate_row(self, table: Table, values: dict):
        unknown = set(values) - {c.name for c in table.columns}
        if unknown:
            raise self._fail(FKDBError("表 %s 未知列: %s"
                                       % (table.name, sorted(unknown))))
        out = {}
        for col in table.columns:
            if col.name in values:
                v = values[col.name]
            elif col.has_default:
                v = col.default
            else:
                v = None
            if v is None and not col.nullable:
                raise self._fail(FKDBError(
                    "表 %s 列 %s 不允许 NULL" % (table.name, col.name)))
            if v is not None:
                if col.ctype in ("INTEGER", "BIGINT") and not isinstance(v, int):
                    raise self._fail(FKDBError("列 %s 需要整数" % col.name))
                if col.ctype == "REAL" and not isinstance(v, (int, float)):
                    raise self._fail(FKDBError("列 %s 需要数值" % col.name))
                if col.ctype == "BOOLEAN" and not isinstance(v, bool):
                    raise self._fail(FKDBError("列 %s 需要布尔" % col.name))
            out[col.name] = v
        return out

    def _check_fk_refs(self, table: Table, row: dict, old_row: Optional[dict],
                       defer: bool = False):
        """插入或更新后，检查本行所有外键都指向现存父行。"""
        for fk in table.fks:
            col = fk.columns[0]
            v = row[col]
            if v is None:
                if fk.not_null_fk:
                    raise self._fail(ForeignKeyViolation(
                        "表 %s 外键 %s 是 NOT NULL" % (table.name, fk.name)))
                continue
            if old_row is not None and old_row[col] == v:
                continue  # 外键值没动
            if defer:
                # 延迟模式：只登记，commit 前再查（支持成环插入）
                self._deferred_checks.append((table, dict(row)))
                continue
            # 对父行加 S 锁，与父行删除的 X 锁互斥
            parent = self.get(fk.ref_table, v, lock=S)
            if parent is None:
                raise self._fail(ForeignKeyViolation(
                    "表 %s 的外键 %s 引用了不存在的 %s.%d"
                    % (table.name, fk.name, fk.ref_table, v),
                    kind="orphan",
                    details={"table": table.name, "fk": fk.name, "value": v}))

    def set_deferred(self, flag: bool = True):
        """开启后，本事务的外键存在性检查延迟到提交前（环插入用）。"""
        self._deferred = flag

    def _run_deferred_checks(self):
        if not self._deferred_checks:
            return
        for table, row in self._deferred_checks:
            for fk in table.fks:
                v = row[fk.columns[0]]
                if v is None:
                    continue
                parent = self._current(fk.ref_table, v)
                if parent is None:
                    raise self._fail(ForeignKeyViolation(
                        "提交前延迟检查失败：表 %s 外键 %s 引用不存在的 %s.%d"
                        % (table.name, fk.name, fk.ref_table, v),
                        kind="deferred",
                        details={"table": table.name, "fk": fk.name,
                                 "value": v}))
        self._deferred_checks.clear()

    def insert(self, tname: str, values: dict, rowid: Optional[int] = None) -> int:
        if self.finished:
            raise TransactionAborted("事务已结束")
        table = self.catalog.table(tname)
        if rowid is None:
            rowid = self.storage.allocate_rowid(tname)
        values = dict(values)
        values.setdefault(table.pk_column, rowid)
        row = self._validate_row(table, values)
        rowid = row[table.pk_column]
        self.lock_write(tname, rowid)
        if self._current(tname, rowid) is not None:
            raise self._fail(UniqueViolation(
                "表 %s 主键 %d 已存在" % (tname, rowid)))
        self._check_fk_refs(table, row, None, defer=self._deferred)
        self._stage_upsert(tname, rowid, row)
        return rowid

    def update(self, tname: str, rowid: int, changes: dict) -> dict:
        if self.finished:
            raise TransactionAborted("事务已结束")
        table = self.catalog.table(tname)
        self.lock_write(tname, rowid)
        old = self._current(tname, rowid)
        if old is None:
            raise self._fail(NotFound("%s.%d 不存在" % (tname, rowid)))
        if table.pk_column in changes and changes[table.pk_column] != rowid:
            raise self._fail(FKDBError(
                "主键更新请走 update_pk（需要 ON UPDATE 级联语义）"))
        merged = dict(old)
        merged.update(changes)
        row = self._validate_row(table, merged)
        row[table.pk_column] = rowid
        self._check_fk_refs(table, row, old, defer=self._deferred)
        self._stage_upsert(tname, rowid, row)
        return row

    def _stage_upsert(self, tname: str, rowid: int, row: dict):
        table = self.catalog.table(tname)
        payload = codec.encode_row(table, row)
        # 同一行多次变更：覆盖待写项，最终只落一帧
        self._replace_pending(tname, rowid, codec.OP_UPSERT, payload)
        self.overlay.setdefault(tname, {})[rowid] = row
        self._mark_dirty(tname, rowid)

    def _stage_delete(self, tname: str, rowid: int):
        self._replace_pending(tname, rowid, codec.OP_DELETE, None)
        self.overlay.setdefault(tname, {})[rowid] = _TOMB
        self._mark_dirty(tname, rowid)

    def _replace_pending(self, tname, rowid, op, payload):
        """同一 (表,行) 的多次改动合并为最后一次，减少落盘帧数。"""
        key = (tname, rowid)
        idx = self._pending_idx.get(key)
        if idx is not None:
            self._pending[idx] = (tname, rowid, op, payload)
            return
        self._pending_idx[key] = len(self._pending)
        self._pending.append((tname, rowid, op, payload))

    def _stage_reinsert(self, tname: str, old_id: int, new_id: int,
                        row: dict):
        """主键改值：旧 id 删墓碑，新 id 插新行（提交时按序应用）。"""
        table = self.catalog.table(tname)
        payload = codec.encode_row(table, row)
        self._replace_pending(tname, old_id, codec.OP_DELETE, None)
        self._replace_pending(tname, new_id, codec.OP_UPSERT, payload)
        self.overlay.setdefault(tname, {})[old_id] = _TOMB
        self.overlay[tname][new_id] = row
        self._mark_dirty(tname, old_id)
        self._mark_dirty(tname, new_id)

    def _materialize_mutations(self):
        """提交前一次性预分配 lsn，把轻量变更转成带 lsn 的 WAL 变更。"""
        if not self._pending:
            return []
        start = self.storage.reserve_lsns(len(self._pending))
        self.mutations = []
        for i, (tname, rowid, op, payload) in enumerate(self._pending):
            self.mutations.append((start + i, tname, rowid, op, payload))
        return self.mutations

    def delete_row(self, tname: str, rowid: int):
        """单行删除（无显式级联，受外键 RESTRICT 保护由调用方规划）。"""
        if self.finished:
            raise TransactionAborted("事务已结束")
        self.lock_write(tname, rowid)
        if self._current(tname, rowid) is None:
            raise self._fail(NotFound("%s.%d 不存在" % (tname, rowid)))
        lsn = self.storage.next_lsn()
        self._lsns.append(lsn)
        self.mutations.append((lsn, tname, rowid, codec.OP_DELETE, None))
        self.overlay.setdefault(tname, {})[rowid] = _TOMB
        self._mark_dirty(tname, rowid)

    # ==================================================== 级联计划执行
    def estimate_delete(self, roots: List[Tuple[str, int]],
                        order_seed: Optional[int] = None) -> Plan:
        """预估（只读，不加锁）。"""
        if self.finished:
            raise TransactionAborted("事务已结束")
        return self.planner.plan_delete(roots, order_seed=order_seed)

    def execute_delete_plan(self, plan: Plan) -> dict:
        """
        在本事务内执行一份删除计划。返回实际动作清单（与 plan 可逐表核对）。
        流程：按 (表,rowid) 排序一次性拿全部 X 锁（消除死锁）→ 重算计划
        检查漂移 → 应用 SET 动作和删除。
        """
        if self.finished:
            raise TransactionAborted("事务已结束")
        keys = sorted(plan.nodes.keys())
        # 批量有序加锁快路径（无冲突时一次拿全，避免逐行线程同步）
        self.locker.acquire_many(self.txn_id, keys, X)

        # 加锁后重算：并发事务可能已经改了图，漂移则让上层重试。
        # 轻量模式只算动作闭包，不构造外键链（比对不需要链）。
        fresh = self.planner.plan_delete(plan.roots, lightweight=True)
        if not self._plans_equivalent(plan, fresh):
            raise self._fail(FKDBError(
                "加锁后数据发生变化，删除计划漂移，需要重试"))

        report = {"deleted": [], "set_null": [], "set_default": [],
                  "chains": []}
        # 先做 SET NULL / SET DEFAULT（更新行），再删除，顺序不影响最终态
        for key in keys:
            node = fresh.nodes[key]
            if node.action in ("SET_NULL", "SET_DEFAULT"):
                cur = self._current(node.table, node.rowid)
                if cur is None:
                    continue
                new_row = dict(cur)
                new_row.update(node.set_cols)
                # SET NULL/DEFAULT 是外键规则自身的动作，其目标值合法性
                # 已在建表期（NOT NULL/默认值存在性由调用方保证）校验，
                # 这里不能再走普通外键检查——父行可能正在同事务被删。
                self._stage_upsert(node.table, node.rowid, new_row)
                base = {"table": node.table, "rowid": node.rowid,
                        "fk": node.via_fk,
                        "chain": [h.fk_name for h in node.chain]}
                # 一行可能同时有多列动作，按列拆成清单项，与预估口径一致
                for col_name, val in node.set_cols.items():
                    item = dict(base, col=col_name, value=val)
                    if val is None:
                        report["set_null"].append(item)
                    else:
                        report["set_default"].append(item)
                    report["chains"].append(item)

        for key in keys:
            node = fresh.nodes[key]
            if node.action != "DELETE":
                continue
            if self._current(node.table, node.rowid) is None:
                continue
            self._stage_delete(node.table, node.rowid)
            item = {"table": node.table, "rowid": node.rowid,
                    "fk": node.via_fk,
                    "chain": [h.fk_name for h in node.chain]}
            report["deleted"].append(item)
            report["chains"].append(item)

        report["counts"] = fresh.counts_by_table()
        # 不在执行路径渲染全量影响树（百万节点时极贵）；每行的外键链
        # 已逐条写进 deleted/set_null/set_default 清单，需要树时单独预估。
        report["node_count"] = len(fresh.nodes)
        report["max_depth"] = max(
            (n.depth for n in fresh.nodes.values()), default=0)
        return report

    def execute_update_pk_plan(self, plan: Plan) -> dict:
        root_table, root_id = plan.roots[0]
        root_table_obj = self.catalog.table(root_table)
        new_pk = plan.nodes[(root_table, root_id)].set_cols[
            root_table_obj.pk_column]
        # 新 id 也要锁住，防止并发事务抢先插入
        self.lock_write(root_table, root_id)
        self.lock_write(root_table, new_pk)
        for key, node in plan.nodes.items():
            if key == (root_table, root_id):
                continue
            self.lock_write(key[0], key[1])

        fresh = self.planner.plan_update_pk(root_table, root_id, new_pk)
        if not self._plans_equivalent(plan, fresh):
            raise self._fail(FKDBError("更新计划漂移，需要重试"))

        report = {"updated": [], "chains": [], "reinserted": []}
        # 子行：外键列改值，rowid 不变，普通 upsert
        for key in sorted(fresh.nodes.keys()):
            node = fresh.nodes[key]
            if key == (root_table, root_id):
                continue
            cur = self._current(node.table, node.rowid)
            if cur is None:
                continue
            new_row = dict(cur)
            new_row.update(node.set_cols)
            # 级联更新的新外键值就是父行的新主键，合法性由规划保证，
            # 且父行可能在本事务稍后才完成重插，因此跳过即时外键检查。
            self._stage_upsert(node.table, node.rowid, new_row)
            item = {"table": node.table, "rowid": node.rowid,
                    "fk": node.via_fk, "cols": node.set_cols,
                    "chain": [h.fk_name for h in node.chain]}
            report["updated"].append(item)
            report["chains"].append(item)

        # 根行：删旧 id、插新 id
        root_row = self._current(root_table, root_id)
        if root_row is None:
            raise self._fail(NotFound("%s.%d 不存在" % (root_table, root_id)))
        new_root = dict(root_row)
        new_root[root_table_obj.pk_column] = new_pk
        if self._current(root_table, new_pk) is not None:
            raise self._fail(UniqueViolation(
                "表 %s 主键 %d 已存在" % (root_table, new_pk)))
        self._stage_reinsert(root_table, root_id, new_pk, new_root)
        report["reinserted"] = [{"table": root_table,
                                 "old_rowid": root_id, "new_rowid": new_pk}]
        report["counts"] = fresh.counts_by_table()
        return report

    @staticmethod
    def _plans_equivalent(a: Plan, b: Plan) -> bool:
        if set(a.nodes) != set(b.nodes):
            return False
        for k, na in a.nodes.items():
            nb = b.nodes.get(k)
            if nb is None or na.action != nb.action or na.set_cols != nb.set_cols:
                return False
        return True

    # ============================================================ 提交/回滚
    def commit(self):
        if self.finished:
            return
        if self.failed:
            self.rollback()
            raise TransactionAborted("事务此前已失败，已自动回滚")
        try:
            self._run_deferred_checks()
            if self.failed:
                self.rollback()
                raise TransactionAborted("延迟外键检查失败，已回滚")
            mutations = self._materialize_mutations()
            if mutations:
                self.storage.commit_txn(self.txn_id, mutations)
        finally:
            self._finish()

    def rollback(self):
        if self.finished:
            return
        self._finish()

    def _finish(self):
        self.finished = True
        self.locker.release_all(self.txn_id)
        self.overlay.clear()
        self.mutations.clear()
        self._pending.clear()
        self._pending_idx.clear()
        self._dirty.clear()

    def lock_stats(self) -> dict:
        return self.locker.stats(self.txn_id)
