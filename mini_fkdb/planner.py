"""
外键级联规划器 —— 本引擎语义正确性的核心。

设计原则
========
1. 删除/更新的传播按 SQL 标准的“最小不动点”计算：从根行集合出发，
   沿着所有入边外键反复扩张，直到没有新行可到达。每个 (表,rowid)
   只在第一次被访问时定型（delete 优先级最高），因此无论 BFS 从哪条
   路径先走到它，最终 Plan 完全相同 —— 满足“同一次删除结果不变”。
2. 预估（plan_*）和执行（execute_plan）共用同一份遍历代码，差别只在
   执行期会真正加锁、写 WAL。预估返回的行数清单与执行结果逐表可核对。
3. 动作语义：
   - CASCADE     子行加入删除/更新集合，继续向外传播
   - SET NULL    子行的外键列置 NULL，行不删，不再沿该外键传播
   - SET DEFAULT 同上，置为列默认值（执行期校验默认父行存在）
   - RESTRICT    立即报告违反（标准语义：语句开始即检查）
   - NO ACTION   延迟到本语句所有级联走完再检查；如果违反行后来被
                 级联删掉（环里的自消解），则不算违反。这是它与
                 RESTRICT 在环场景下唯一的可观察差异。
4. 自引用、同表多外键、多表成环都只是图上的普通边，不需要特殊处理。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .errors import ForeignKeyViolation
from .schema import (Catalog, ForeignKey, Table,
                     CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION)


# 外键链上的一跳：经由哪个外键、从哪张表哪行过来
@dataclass(frozen=True)
class Hop:
    fk_name: str
    from_table: str
    from_rowid: int


@dataclass
class PlanNode:
    table: str
    rowid: int
    action: str                      # DELETE / SET_NULL / SET_DEFAULT / BLOCKED
    via_fk: Optional[str] = None     # 第一次到达它的外键
    depth: int = 0                   # 到根的最短跳数
    chain: Tuple[Hop, ...] = ()      # 首次到达路径（完整外键链）
    # 所有到达过它的外键链（多分支证据），去重
    all_chains: List[Tuple[Hop, ...]] = field(default_factory=list)
    set_cols: Dict[str, object] = field(default_factory=dict)  # 列 -> 新值


@dataclass
class Plan:
    kind: str                        # "delete" / "update"
    roots: List[Tuple[str, int]]
    nodes: Dict[Tuple[str, int], PlanNode]
    # 延迟检查的 NO ACTION 违反（执行前还可能被级联消解）
    deferred: List[dict] = field(default_factory=list)
    # 立即违反的 RESTRICT（一经发现，语句必然失败）
    immediate_blocks: List[dict] = field(default_factory=list)
    update_map: Optional[Dict[Tuple[str, int], dict]] = None
    lightweight: bool = False

    # ------------------------------------------------------ 统计与核对
    def tables_touched(self) -> List[str]:
        names = sorted({n.table for n in self.nodes.values()})
        return names

    def counts_by_table(self) -> Dict[str, Dict[str, int]]:
        """
        每张表的动作计数。delete/blocked 按行计；set_null/set_default
        按“列动作”计（同一行可能一列置空、另一列置默认，两边都计数）。
        """
        out: Dict[str, Dict[str, int]] = {}
        for n in self.nodes.values():
            bucket = out.setdefault(
                n.table, {"delete": 0, "set_null": 0,
                          "set_default": 0, "blocked": 0,
                          "update_pk": 0, "update_fk": 0,
                          "no_action_hold": 0})
            if n.action == "DELETE":
                bucket["delete"] += 1
            elif n.action == "BLOCKED":
                bucket["blocked"] += 1
            elif n.action == "UPDATE_PK":
                bucket["update_pk"] += 1
            elif n.action == "UPDATE_FK":
                bucket["update_fk"] += 1
            elif n.action == "NO_ACTION_HOLD":
                bucket["no_action_hold"] += 1
            else:  # SET_NULL / SET_DEFAULT：按列动作细分
                for col, val in n.set_cols.items():
                    bucket["set_null" if val is None
                           else "set_default"] += 1
        return out

    def deleted_rows(self, table: str) -> List[int]:
        return sorted(n.rowid for n in self.nodes.values()
                      if n.table == table and n.action == "DELETE")

    def updated_rows(self, table: str) -> List[int]:
        return sorted(n.rowid for n in self.nodes.values()
                      if n.table == table and n.action != "DELETE")

    def is_blocked(self) -> bool:
        return bool(self.immediate_blocks or self.deferred)

    def blockage(self) -> List[dict]:
        return self.immediate_blocks + self.deferred

    def impact_tree(self) -> dict:
        """供页面展示的级联影响树（按 表->动作 聚合 + 根出发的树）。"""
        summary = self.counts_by_table()
        children: Dict[Tuple[str, int], List[PlanNode]] = {}
        roots: List[PlanNode] = []
        for n in self.nodes.values():
            if n.depth == 0:
                roots.append(n)
            elif n.chain:
                prev = n.chain[-1]
                children.setdefault((prev.from_table, prev.from_rowid),
                                    []).append(n)
        for lst in children.values():
            lst.sort(key=lambda x: (x.table, x.rowid))

        def render(node: PlanNode) -> dict:
            kids = children.get((node.table, node.rowid), [])
            return {
                "table": node.table,
                "rowid": node.rowid,
                "action": node.action,
                "via_fk": node.via_fk,
                "depth": node.depth,
                "chain": [{"fk": h.fk_name, "from_table": h.from_table,
                           "from_rowid": h.from_rowid} for h in node.chain],
                "set_cols": node.set_cols,
                "children": [render(k) for k in kids],
            }

        return {"kind": self.kind,
                "summary": summary,
                "tree": [render(r) for r in sorted(roots, key=lambda x:
                                                    (x.table, x.rowid))]}


class Planner:
    def __init__(self, catalog: Catalog, storage):
        self.catalog = catalog
        self.storage = storage
        # 预建图索引，避免删除期间对每张被删表反复做全目录线性扫描。
        # incoming[table] = 所有指向该表的外键；owner[fk_name] = 所属表名。
        self._incoming: Dict[str, List[ForeignKey]] = {}
        self._owner: Dict[str, str] = {}
        for tname, t in self.catalog.tables.items():
            for fk in t.fks:
                self._owner[fk.name] = tname
                self._incoming.setdefault(fk.ref_table, []).append(fk)

    def _incoming_fks(self, table_name: str) -> List[ForeignKey]:
        return self._incoming.get(table_name, ())

    def _owner_table_of(self, fk: ForeignKey) -> str:
        return self._owner[fk.name]

    # ==================================================== 删除规划（预估）
    def plan_delete(self, roots: List[Tuple[str, int]],
                    order_seed: Optional[int] = None,
                    lightweight: bool = False) -> Plan:
        """
        roots: 要删除的 (表, rowid) 列表。
        order_seed: 测试用——打乱边/队列顺序，验证结果与遍历路径无关。
        lightweight: 只算节点闭包、动作和 set_cols，不构造外键链，
                     供加锁后的漂移检测使用（比对不需要链，省大量元组）。
        """
        plan = Plan(kind="delete", roots=list(roots), nodes={})
        plan.lightweight = lightweight
        queue: List[Tuple[str, int]] = []

        for tname, rowid in roots:
            key = (tname, rowid)
            if key in plan.nodes:
                continue
            if self.storage.get(tname, rowid) is None:
                continue  # 不存在的根行：幂等跳过
            node = PlanNode(table=tname, rowid=rowid, action="DELETE",
                            depth=0, chain=(),
                            all_chains=[()] if not lightweight else [])
            plan.nodes[key] = node
            queue.append(key)

        head = 0
        while head < len(queue):
            cur_key = queue[head]
            head += 1
            cur = plan.nodes[cur_key]
            if cur.action != "DELETE":
                continue  # 被置空/置默认的行不再向外传播
            cur_table = self.catalog.table(cur.table)

            edges = list(self._incoming_fks(cur.table))
            if order_seed is not None:  # 确定性“打乱”：按种子反转，便于测试
                edges.sort(key=lambda fk: (fk.name, order_seed))
                if order_seed % 2:
                    edges.reverse()

            for fk in edges:
                child_table = self.catalog.table(
                    self._owner_table_of(fk))
                child_ids = self.storage.find_by_fk(
                    child_table.name, fk.name, cur.rowid)
                child_ids.sort()
                if order_seed is not None and order_seed % 3 == 0:
                    child_ids.reverse()

                # 轻量模式不构造 Hop/chain，传 None 让 _handle_child 跳过
                hop = None if lightweight else Hop(
                    fk.name, cur.table, cur.rowid)
                for cid in child_ids:
                    self._handle_child(plan, queue, fk, child_table,
                                       cid, cur, hop, for_update=False,
                                       lightweight=lightweight)

        # 语句末尾：NO ACTION 统一检查（此时环内自消解的行已经被删掉）
        self._check_no_action(plan)
        return plan

    def _handle_child(self, plan: Plan, queue, fk: ForeignKey,
                      child_table: Table, cid: int,
                      parent_node: PlanNode, hop, for_update: bool,
                      lightweight: bool = False):
        key = (child_table.name, cid)
        # 轻量模式（加锁后漂移检测）不保留外键链，chain 恒为空，
        # 避免百万级元组拼接；动作与 set_cols 仍然精确。
        chain = () if lightweight else parent_node.chain + (hop,)
        existing = plan.nodes.get(key)
        col = fk.columns[0]
        action = fk.on_update if for_update else fk.on_delete

        if existing is not None:
            # 已定型：非轻量模式才记录备选到达链（多分支证据）
            if not lightweight and chain not in existing.all_chains:
                existing.all_chains.append(chain)
            # 同一行可能经多条外键到达，按动作优先级合并：
            #   DELETE（CASCADE）最高，一旦定型整行删除，其他边全部被吸收
            #   否则 SET 动作按“列”累积：一行可以同时把 A 列置空、B 列置默认
            if existing.action == "DELETE":
                return
            if action == CASCADE:
                existing.action = "DELETE"
                existing.set_cols = {}
                existing.via_fk = fk.name
                existing.depth = min(existing.depth, parent_node.depth + 1)
                queue.append(key)
                return
            if action in (SET_NULL, SET_DEFAULT, NO_ACTION):
                self._merge_set_action(existing, fk, child_table, action)
            # RESTRICT 已在首次到达时登记 immediate_blocks
            return

        if action == RESTRICT:
            node = PlanNode(table=child_table.name, rowid=cid,
                            action="BLOCKED", via_fk=fk.name,
                            depth=parent_node.depth + 1, chain=chain,
                            all_chains=[chain] if not lightweight else [])
            plan.nodes[key] = node
            plan.immediate_blocks.append({
                "table": child_table.name, "rowid": cid, "fk": fk.name,
                "action": "RESTRICT",
                "parent_table": parent_node.table,
                "parent_rowid": parent_node.rowid,
                "chain": [] if lightweight else [h.fk_name for h in chain]})
            return

        if action == NO_ACTION:
            # NO ACTION 不传播任何动作，只登记延迟检查：
            # 语句末若该子行仍存活且父行将被删 → 违反；
            # 若它同时被另一条 CASCADE 边定型为删除/置值 → 自消解。
            node = PlanNode(table=child_table.name, rowid=cid,
                            action="NO_ACTION_HOLD", via_fk=fk.name,
                            depth=parent_node.depth + 1, chain=chain,
                            all_chains=[chain] if not lightweight else [])
            plan.nodes[key] = node
            plan.deferred.append({
                "table": child_table.name, "rowid": cid, "fk": fk.name,
                "action": "NO ACTION",
                "parent_table": parent_node.table,
                "parent_rowid": parent_node.rowid,
                "chain": [] if lightweight else [h.fk_name for h in chain]})
            return

        if action == CASCADE:
            node = PlanNode(table=child_table.name, rowid=cid,
                            action="DELETE", via_fk=fk.name,
                            depth=parent_node.depth + 1, chain=chain,
                            all_chains=[chain] if not lightweight else [])
            plan.nodes[key] = node
            queue.append(key)
            return

        # SET NULL / SET DEFAULT：首次到达，建立 SET 节点
        if action == SET_DEFAULT:
            default = child_table.col(fk.columns[0]).default
            # 默认父行若也在本次删除闭包里，置默认会制造悬空引用——
            # 在规划期就拦下，而不是写出坏数据再靠自检发现
            if default is not None and (fk.ref_table, default) in plan.nodes \
                    and plan.nodes[(fk.ref_table, default)].action == "DELETE":
                node = PlanNode(table=child_table.name, rowid=cid,
                                action="BLOCKED", via_fk=fk.name,
                                depth=parent_node.depth + 1, chain=chain,
                                all_chains=[chain] if not lightweight else [])
                plan.nodes[key] = node
                plan.immediate_blocks.append({
                    "table": child_table.name, "rowid": cid, "fk": fk.name,
                    "action": "SET_DEFAULT_TARGET_DYING",
                    "parent_table": parent_node.table,
                    "parent_rowid": parent_node.rowid,
                    "chain": [] if lightweight
                    else [h.fk_name for h in chain]})
                return
        node = PlanNode(table=child_table.name, rowid=cid,
                        action="SET_NULL" if action == SET_NULL
                        else "SET_DEFAULT",
                        via_fk=fk.name,
                        depth=parent_node.depth + 1, chain=chain,
                        all_chains=[chain] if not lightweight else [])
        self._apply_set_to_node(node, fk, child_table, action)
        plan.nodes[key] = node

    @staticmethod
    def _apply_set_to_node(node: PlanNode, fk: ForeignKey,
                           child_table: Table, action: str):
        col = fk.columns[0]
        if action == SET_NULL:
            node.set_cols[col] = None
        else:
            node.set_cols[col] = child_table.col(col).default

    def _merge_set_action(self, node: PlanNode, fk: ForeignKey,
                          child_table: Table, action: str):
        """把一条新到达的 SET/NO_ACTION 边合并进已有节点（按列累积）。"""
        col = fk.columns[0]
        if action == NO_ACTION:
            if node.action == "NO_ACTION_HOLD":
                return  # 两条 NO ACTION 边，保持 HOLD，等语句末判定
            # 已有 SET 动作会把本行外键值处理掉，NO ACTION 冲突被消解
            return
        # SET NULL / SET DEFAULT
        if node.action == "NO_ACTION_HOLD":
            node.action = "SET_NULL" if action == SET_NULL else "SET_DEFAULT"
        elif node.action == "SET_DEFAULT" and action == SET_NULL:
            node.action = "SET_NULL"  # 行级标签取更强的动作
        self._apply_set_to_node(node, fk, child_table, action)

    def _check_no_action(self, plan: Plan):
        remaining = []
        for v in plan.deferred:
            key = (v["table"], v["rowid"])
            node = plan.nodes.get(key)
            if node is None:
                # 不在计划里意味着行不会被动 —— 仍然存活，违反成立
                remaining.append(v)
                continue
            if node.action == "NO_ACTION_HOLD":
                # 没有别的边救它：行将原样存活但父行被删 → 违反
                remaining.append(v)
                continue
            if node.action in ("SET_NULL", "SET_DEFAULT"):
                # 外键值被其他边改掉 → 自消解
                continue
            if node.action == "DELETE":
                # 被其他 CASCADE 边带走 → 自消解（环场景）
                continue
            if node.action == "BLOCKED":
                remaining.append(v)
        plan.deferred = remaining
        # HOLD 节点若最终构成违反，转成 BLOCKED 以便统计
        violate_keys = {(v["table"], v["rowid"]) for v in remaining}
        for key in violate_keys:
            node = plan.nodes.get(key)
            if node is not None and node.action == "NO_ACTION_HOLD":
                node.action = "BLOCKED"
        if plan.immediate_blocks or plan.deferred:
            blocks = plan.immediate_blocks + plan.deferred
            first = blocks[0]
            raise ForeignKeyViolation(
                "外键 %s 阻止了操作：%s.%d 仍引用 %s.%d"
                % (first["fk"], first["table"], first["rowid"],
                   first["parent_table"], first["parent_rowid"]),
                kind="blocked", details={"blocks": blocks,
                                         "plan": plan})

    # ==================================================== 更新规划（预估）
    def plan_update_pk(self, table: str, rowid: int, new_pk: int) -> Plan:
        """
        更新被引用键（主键）。ON UPDATE 的四种动作按标准语义传播；
        级联更新只改子行的外键列值，不会删除行，因此传播一跳即可
        （被改的是外键列而不是别人的主键，不会再引发新的引用变化）。
        """
        plan = Plan(kind="update", roots=[(table, rowid)], nodes={},
                    update_map={})
        cur = self.storage.get(table, rowid)
        if cur is None:
            return plan
        t = self.catalog.table(table)
        pk_col = t.pk_column
        root = PlanNode(table=table, rowid=rowid, action="UPDATE_PK",
                        depth=0, chain=(), all_chains=[()],
                        set_cols={pk_col: new_pk})
        plan.nodes[(table, rowid)] = root
        plan.update_map[(table, rowid)] = {pk_col: new_pk}

        queue = [root]
        head = 0
        while head < len(queue):
            parent_node = queue[head]
            head += 1
            parent_table = self.catalog.table(parent_node.table)
            for fk in self._incoming_fks(parent_node.table):
                child_tname = self._owner_table_of(fk)
                child_table = self.catalog.table(child_tname)
                # 父行的哪个值被改了？根是主键；深层节点是外键列，
                # 但能被深层引用到的前提是该列也是某张表的主键——
                # 单列外键场景下只有根主键需要向外传播。
                ref_value = parent_node.rowid
                child_ids = sorted(self.storage.find_by_fk(
                    child_tname, fk.name, ref_value))
                hop = Hop(fk.name, parent_node.table, parent_node.rowid)
                for cid in child_ids:
                    self._handle_update_child(plan, fk, child_table, cid,
                                              parent_node, hop, new_pk,
                                              queue)
        self._check_no_action(plan)
        return plan

    def _handle_update_child(self, plan: Plan, fk: ForeignKey,
                             child_table: Table, cid: int,
                             parent_node: PlanNode, hop: Hop,
                             new_pk: int, queue):
        key = (child_table.name, cid)
        chain = parent_node.chain + (hop,)
        if key in plan.nodes:
            node = plan.nodes[key]
            if chain not in node.all_chains:
                node.all_chains.append(chain)
            return
        action = fk.on_update
        col = fk.columns[0]
        if action == RESTRICT:
            node = PlanNode(child_table.name, cid, "BLOCKED", fk.name,
                            parent_node.depth + 1, chain, [chain])
            plan.nodes[key] = node
            plan.immediate_blocks.append({
                "table": child_table.name, "rowid": cid, "fk": fk.name,
                "action": "RESTRICT",
                "parent_table": parent_node.table,
                "parent_rowid": parent_node.rowid,
                "chain": [h.fk_name for h in chain]})
            return
        if action == NO_ACTION:
            # ON UPDATE 的 NO ACTION：更新不会删除子行，无自消解路径，
            # 语句末检查时这些行仍然存活即构成违反
            node = PlanNode(child_table.name, cid, "BLOCKED", fk.name,
                            parent_node.depth + 1, chain, [chain])
            plan.nodes[key] = node
            plan.deferred.append({
                "table": child_table.name, "rowid": cid, "fk": fk.name,
                "action": "NO ACTION",
                "parent_table": parent_node.table,
                "parent_rowid": parent_node.rowid,
                "chain": [h.fk_name for h in chain]})
            return
        if action == CASCADE:
            node = PlanNode(child_table.name, cid, "UPDATE_FK", fk.name,
                            parent_node.depth + 1, chain, [chain],
                            set_cols={col: new_pk})
            plan.nodes[key] = node
            plan.update_map[key] = {col: new_pk}
            return
        if action == SET_NULL:
            node = PlanNode(child_table.name, cid, "SET_NULL", fk.name,
                            parent_node.depth + 1, chain, [chain],
                            set_cols={col: None})
            plan.nodes[key] = node
            plan.update_map[key] = {col: None}
            return
        if action == SET_DEFAULT:
            default = child_table.col(col).default
            node = PlanNode(child_table.name, cid, "SET_DEFAULT", fk.name,
                            parent_node.depth + 1, chain, [chain],
                            set_cols={col: default})
            plan.nodes[key] = node
            plan.update_map[key] = {col: default}
            return
        raise RuntimeError("未知 ON UPDATE 动作 %s" % action)  # pragma: no cover
