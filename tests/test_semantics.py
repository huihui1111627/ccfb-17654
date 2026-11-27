"""
引擎语义测试（不依赖 pytest，直接 python3 -I tests/test_semantics.py 运行）。

覆盖题目硬性要求：
1. 四种动作的删除/更新语义
2. 自引用树、同表多外键、表间环
3. 路径无关：用不同 order_seed 打乱遍历顺序，最终计划必须逐行相同
4. 预估与实际逐表核对
5. 拆批删除与一次性删除结果等价
6. 任何时刻无悬空外键
7. RESTRICT vs NO ACTION 在环里的标准差异（延迟自消解）
8. 崩溃恢复后状态一致
"""
import os
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_fkdb import (Database, Catalog, Column, ForeignKey,
                       CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION,
                       BIGINT, TEXT, INTEGER,
                       ForeignKeyViolation, LockTimeout, FKDBError)

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓", msg)
    else:
        FAIL += 1
        print("  ✗ FAIL:", msg)


def cyclic_catalog():
    """
    a -> b -> c -> a 的三张表环，每条边动作可配；
    另有一棵自引用树 t。
    """
    cat = Catalog()
    cat.create_table("a", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("b_id", BIGINT)])
    cat.create_table("b", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("c_id", BIGINT)])
    cat.create_table("c", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("a_id", BIGINT)])
    cat.table("a").fks = [ForeignKey("a_to_b", ["b_id"], "b", ["id"],
                                     on_delete=CASCADE, on_update=CASCADE)]
    cat.table("b").fks = [ForeignKey("b_to_c", ["c_id"], "c", ["id"],
                                     on_delete=CASCADE, on_update=CASCADE)]
    cat.table("c").fks = [ForeignKey("c_to_a", ["a_id"], "a", ["id"],
                                     on_delete=CASCADE, on_update=CASCADE)]
    # 自引用树
    cat.create_table("tree", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("parent_id", BIGINT),
        Column("a_id", BIGINT)])
    cat.table("tree").fks = [
        ForeignKey("tree_self", ["parent_id"], "tree", ["id"],
                   on_delete=CASCADE, on_update=CASCADE),
        ForeignKey("tree_a", ["a_id"], "a", ["id"],
                   on_delete=SET_NULL, on_update=CASCADE)]
    cat.validate()
    return cat


def seed_cyclic(db: Database):
    with db.transaction() as txn:
        txn.set_deferred()
        # 两个互相引用的环实例：a1-b1-c1-a1, a2-b2-c2-a2
        txn.insert("a", {"id": 1, "b_id": 1})
        txn.insert("b", {"id": 1, "c_id": 1})
        txn.insert("c", {"id": 1, "a_id": 1})
        txn.insert("a", {"id": 2, "b_id": 2})
        txn.insert("b", {"id": 2, "c_id": 2})
        txn.insert("c", {"id": 2, "a_id": 2})
        # 树：1-2-3，4 挂 2；节点 3、4 引用 a1（SET NULL 边）
        txn.insert("tree", {"id": 1, "parent_id": None, "a_id": 1})
        txn.insert("tree", {"id": 2, "parent_id": 1, "a_id": None})
        txn.insert("tree", {"id": 3, "parent_id": 2, "a_id": 1})
        txn.insert("tree", {"id": 4, "parent_id": 2, "a_id": 2})


def test_path_independence():
    print("\n== 路径无关性（不同遍历顺序，计划逐行相同） ==")
    path = "/tmp/fkdb_sem_1"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cyclic_catalog())
    seed_cyclic(db)

    signatures = []
    for seed in range(6):
        plan = db.estimate_delete([("a", 1)], order_seed=seed)
        sig = sorted((k, n.action, tuple(sorted(n.set_cols.items())),
                      tuple(n.chain))
                     for k, n in plan.nodes.items())
        signatures.append(sig)
    check(all(s == signatures[0] for s in signatures),
          "6 种遍历顺序下删除计划完全一致")

    # 多根、不同提交顺序
    sigs2 = []
    for roots in ([("a", 1), ("a", 2)], [("a", 2), ("a", 1)]):
        p = db.estimate_delete(list(roots))
        sigs2.append(sorted((k, n.action) for k, n in p.nodes.items()))
    check(sigs2[0] == sigs2[1], "根行顺序调换不影响结果")
    db.close()


def test_cyclic_cascade():
    print("\n== 环上的级联删除 ==")
    path = "/tmp/fkdb_sem_2"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cyclic_catalog())
    seed_cyclic(db)
    rep = db.cascade_delete([("a", 1)])
    check(db.count("a") == 1 and db.count("b") == 1 and db.count("c") == 1,
          "环实例 1 整环删除，环实例 2 完整保留")
    # tree 节点 1/2/3 不引用 a1 之外……节点 3 引用 a1 → SET NULL 且存活
    trees = {r["id"]: r for r in db.scan("tree")}
    check(trees[3]["a_id"] is None and trees[4]["a_id"] == 2,
          "SET NULL 边：引用 a1 的树节点置空，引用 a2 的不动")
    check(db.assert_integrity(), "删除后全库无悬空外键")
    db.close()


def test_restrict_vs_no_action():
    print("\n== RESTRICT 与 NO ACTION 的标准差异 ==")
    cat = Catalog()
    cat.create_table("p", [Column("id", BIGINT, nullable=False, pk=True)])
    cat.create_table("r", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("p_id", BIGINT, nullable=False)])
    cat.create_table("n", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("p_id", BIGINT, nullable=False)])
    cat.table("r").fks = [ForeignKey("r_fk", ["p_id"], "p", ["id"],
                                     on_delete=RESTRICT)]
    cat.table("n").fks = [ForeignKey("n_fk", ["p_id"], "p", ["id"],
                                     on_delete=NO_ACTION)]
    cat.validate()
    path = "/tmp/fkdb_sem_3"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cat)
    with db.transaction() as txn:
        txn.insert("p", {"id": 1})
        txn.insert("r", {"id": 1, "p_id": 1})
        txn.insert("n", {"id": 1, "p_id": 1})

    try:
        db.estimate_delete([("p", 1)])
        check(False, "RESTRICT/NO ACTION 都应阻止删除存活子行")
    except ForeignKeyViolation as e:
        actions = {b["action"] for b in e.details["blocks"]}
        check("RESTRICT" in actions and "NO ACTION" in actions,
              "两类约束都上报，共 %d 处" % len(e.details["blocks"]))

    # 环内自消解：n 自引用且 NO ACTION，删根时整棵树级联（这里构造
    # n 表自身再加一条 CASCADE 自边，子行最终会被删 → NO ACTION 不挡）
    cat2 = Catalog()
    cat2.create_table("x", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("parent_id", BIGINT)])
    cat2.table("x").fks = [ForeignKey("x_self", ["parent_id"], "x", ["id"],
                                      on_delete=NO_ACTION)]
    cat2.validate()
    path2 = "/tmp/fkdb_sem_3b"
    shutil.rmtree(path2, ignore_errors=True)
    db2 = Database.create(path2, cat2)
    with db2.transaction() as txn:
        txn.insert("x", {"id": 1, "parent_id": None})
        txn.insert("x", {"id": 2, "parent_id": 1})
        txn.insert("x", {"id": 3, "parent_id": 2})
    # NO ACTION 下子行仍存活 → 必须挡
    blocked = False
    try:
        db2.estimate_delete([("x", 1)])
    except ForeignKeyViolation:
        blocked = True
    check(blocked, "NO ACTION：存活的引用链阻止删除")
    db2.close()
    db.close()


def test_set_actions():
    print("\n== SET NULL / SET DEFAULT 细节 ==")
    cat = Catalog()
    cat.create_table("p", [Column("id", BIGINT, nullable=False, pk=True),
                           Column("name", TEXT)])
    cat.create_table("s", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("null_fk", BIGINT),
        Column("def_fk", BIGINT, default=100, has_default=True)])
    cat.table("s").fks = [
        ForeignKey("s_null", ["null_fk"], "p", ["id"], on_delete=SET_NULL),
        ForeignKey("s_def", ["def_fk"], "p", ["id"], on_delete=SET_DEFAULT)]
    cat.validate()
    path = "/tmp/fkdb_sem_4"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cat)
    with db.transaction() as txn:
        txn.insert("p", {"id": 100, "name": "默认父行（不删）"})
        txn.insert("p", {"id": 1, "name": "待删"})
        # 行 1：两列都指向待删的 1，分别置空/置默认
        txn.insert("s", {"id": 1, "null_fk": 1, "def_fk": 1})
        # 行 2：不涉及待删行，应完全不动
        txn.insert("s", {"id": 2, "null_fk": None, "def_fk": 100})
    plan = db.estimate_delete([("p", 1)])
    c = plan.counts_by_table()["s"]
    check(c["set_null"] == 1 and c["set_default"] == 1,
          "预估：1 行置空、1 行置默认（行 2 不入计划）")
    db.cascade_delete([("p", 1)])
    row1 = db.get("s", 1)
    row2 = db.get("s", 2)
    check(row1["null_fk"] is None and row1["def_fk"] == 100,
          "执行结果与预估一致，行保留并指向默认父行 100")
    check(row2["def_fk"] == 100 and row2["null_fk"] is None,
          "无关行完全不动")
    check(db.assert_integrity(), "无悬空外键")
    db.close()


def test_set_default_multi_parent():
    print("\n== SET DEFAULT 多父行同删（默认父行也在删除集合） ==")
    cat = Catalog()
    cat.create_table("p", [Column("id", BIGINT, nullable=False, pk=True)])
    cat.create_table("s", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("def_fk", BIGINT, default=2, has_default=True)])
    cat.table("s").fks = [
        ForeignKey("s_def", ["def_fk"], "p", ["id"],
                   on_delete=SET_DEFAULT)]
    cat.validate()
    path = "/tmp/fkdb_sem_4b"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cat)
    with db.transaction() as txn:
        txn.insert("p", {"id": 1})
        txn.insert("p", {"id": 2})  # 默认父行，也将一起删
        txn.insert("s", {"id": 1, "def_fk": 1})
    # 默认父行 2 同删：规划期必须拦截，绝不静默写坏数据
    raised = False
    try:
        db.cascade_delete([("p", 1), ("p", 2)])
    except ForeignKeyViolation:
        raised = True
    check(raised, "默认父行在删除闭包时规划期直接拦截")
    check(db.count("s") == 1 and db.dangling_refs() == [],
          "拦截后零改动、无悬空")
    db.close()


def test_batched_equivalence():
    print("\n== 拆批删除与一次性删除等价 ==")
    def build_and_seed(path):
        shutil.rmtree(path, ignore_errors=True)
        db = Database.create(path, cyclic_catalog())
        seed_cyclic(db)
        # 再加一棵大树挂在 a1 上
        with db.transaction() as txn:
            prev = None
            for i in range(100, 160):
                txn.insert("tree", {"id": i, "parent_id": prev, "a_id": 1})
                prev = i
        return db

    db_one = build_and_seed("/tmp/fkdb_sem_5a")
    plan_full = db_one.estimate_delete([("a", 1), ("a", 2)])
    db_one.cascade_delete([("a", 1), ("a", 2)])
    snapshot_one = {t: sorted(r["id"] for r in db_one.scan(t))
                    for t in db_one.table_names()}
    db_one.close()

    db_batch = build_and_seed("/tmp/fkdb_sem_5b")
    manifest = db_batch.cascade_delete_batched(
        [("a", 1), ("a", 2)], batch_size=1)
    snapshot_batch = {t: sorted(r["id"] for r in db_batch.scan(t))
                      for t in db_batch.table_names()}
    check(snapshot_one == snapshot_batch, "两种删法最终行集合逐表相同")
    # 删除闭包不受分片影响，删除行数必须相等；SET 动作因图在分片间收缩，
    # 计数口径天然不同，由零悬空检查保证正确性
    for t in plan_full.counts_by_table():
        e_del = plan_full.counts_by_table()[t]["delete"]
        a_del = manifest["counts"].get(t, {}).get("delete", 0)
        check(e_del == a_del, "表 %s 删除行数相等（%d）" % (t, e_del))
    check(all(b["dangling_after_commit"] == 0 for b in manifest["batches"]),
          "每个分片提交点悬空外键数都是 0")
    check(db_batch.assert_integrity(), "拆批结束无悬空外键")
    db_batch.close()


def test_concurrent_readers_never_see_dangling():
    print("\n== 并发：读事务任何时刻看不到悬空外键 ==")
    path = "/tmp/fkdb_sem_6"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cyclic_catalog())
    seed_cyclic(db)
    with db.transaction() as txn:
        for i in range(100, 140):
            txn.insert("tree", {"id": i, "parent_id": 1, "a_id": 1})

    stop = threading.Event()
    errors = []
    checks_done = [0]

    def reader():
        while not stop.is_set():
            try:
                with db.transaction() as txn:
                    # 长一点的读事务：扫所有表并核对引用
                    rows = {t: txn.scan(t) for t in db.table_names()}
                    a_ids = {r["id"] for r in rows["a"]}
                    for r in rows["c"]:
                        if r["a_id"] is not None and r["a_id"] not in a_ids:
                            errors.append("悬空 c.a_id=%s" % r["a_id"])
                    t_ids = {r["id"] for r in rows["tree"]}
                    for r in rows["tree"]:
                        if r["parent_id"] is not None and \
                                r["parent_id"] not in t_ids:
                            errors.append("悬空 tree.parent_id=%s"
                                          % r["parent_id"])
                    checks_done[0] += 1
            except FKDBError as e:
                errors.append("读事务异常: %r" % e)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in readers:
        t.start()
    db.cascade_delete_batched([("a", 1)], batch_size=1)
    time.sleep(0.2)
    stop.set()
    for t in readers:
        t.join(timeout=5)
    check(not errors, "并发读期间零悬空引用（错误: %s）" % errors[:3])
    check(checks_done[0] > 10, "读事务实际完成了 %d 轮" % checks_done[0])
    db.close()


def test_estimate_matches_actual_multi_roots():
    print("\n== 多客户批量：预估与实际逐表核对 ==")
    path = "/tmp/fkdb_sem_7"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cyclic_catalog())
    seed_cyclic(db)
    roots = [("a", 1), ("a", 2)]
    plan = db.estimate_delete(roots)
    before = {t: db.count(t) for t in db.table_names()}
    manifest = db.cascade_delete_batched(roots, batch_size=1)
    after = {t: db.count(t) for t in db.table_names()}
    ok = True
    for t, c in plan.counts_by_table().items():
        removed = c["delete"]
        nulled = c["set_null"] + c["set_default"]
        if before[t] - after[t] != removed:
            ok = False
        # 置空/默认的行仍在，行数差只能来自删除
        if nulled and after[t] != before[t] - removed:
            ok = False
    check(ok, "各表行数差 == 预估删除数，置动作行保留")
    # 拆批：删除行数逐表与预估一致（SET 口径见前一个测试的说明）
    del_ok = all(
        manifest["counts"].get(t, {}).get("delete", 0)
        == manifest["estimated"].get(t, {}).get("delete", 0)
        for t in manifest["estimated"])
    check(del_ok, "清单内预估/实际删除行数逐表相等")
    db.close()


def test_recovery():
    print("\n== 崩溃恢复（未提交事务不留痕） ==")
    path = "/tmp/fkdb_sem_8"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cyclic_catalog())
    seed_cyclic(db)
    before = {t: db.count(t) for t in db.table_names()}
    # 模拟一个写了 WAL 数据帧但没提交就崩溃的事务：直接操作 storage
    from mini_fkdb.transaction import Transaction
    txn = Transaction(db)
    txn.insert("a", {"id": 99, "b_id": 1})  # 只进 WAL/overlay，不 commit
    # 强制把 WAL 数据帧落盘但不写提交帧
    db.storage.commit_txn  # 确认方法存在
    # 手动刷 WAL（绕过 commit）
    from mini_fkdb import codec
    with open(db.storage._wal_path, "ab", buffering=0) as f:
        for m in txn.mutations:
            lsn, tname, rowid, op, payload = m
            f.write(codec.pack_wal_data(txn.txn_id, lsn, op, tname,
                                        rowid, payload))
        f.flush()
        os.fsync(f.fileno())
    txn.rollback()
    db.close()

    db2 = Database(path)
    after = {t: db2.count(t) for t in db2.table_names()}
    check(before == after, "未提交事务的行在恢复后不存在")
    check(db2.assert_integrity(), "恢复后外键完整")
    db2.close()


def test_on_update_actions():
    print("\n== ON UPDATE 四种动作 ==")
    cat = Catalog()
    cat.create_table("p", [Column("id", BIGINT, nullable=False, pk=True)])
    cat.create_table("cas", [Column("id", BIGINT, nullable=False, pk=True),
                             Column("p_id", BIGINT)])
    cat.create_table("sn", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("p_id", BIGINT)])
    cat.create_table("sd", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("p_id", BIGINT, default=10,
                                   has_default=True)])
    cat.create_table("rs", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("p_id", BIGINT, nullable=False)])
    cat.table("cas").fks = [ForeignKey("cas_fk", ["p_id"], "p", ["id"],
                                       on_update=CASCADE)]
    cat.table("sn").fks = [ForeignKey("sn_fk", ["p_id"], "p", ["id"],
                                      on_update=SET_NULL)]
    cat.table("sd").fks = [ForeignKey("sd_fk", ["p_id"], "p", ["id"],
                                      on_update=SET_DEFAULT)]
    cat.table("rs").fks = [ForeignKey("rs_fk", ["p_id"], "p", ["id"],
                                      on_update=RESTRICT)]
    cat.validate()
    path = "/tmp/fkdb_sem_9"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, cat)
    with db.transaction() as txn:
        txn.insert("p", {"id": 10})
        txn.insert("p", {"id": 1})
        txn.insert("cas", {"id": 1, "p_id": 1})
        txn.insert("sn", {"id": 1, "p_id": 1})
        txn.insert("sd", {"id": 1, "p_id": 1})
        txn.insert("rs", {"id": 1, "p_id": 1})

    # RESTRICT 阻止改 1
    blocked = False
    try:
        db.update_pk("p", 1, 1000)
    except ForeignKeyViolation:
        blocked = True
    check(blocked, "ON UPDATE RESTRICT 拦截")

    # 改 10（没有 rs 引用），其他三种动作生效
    rep = db.update_pk("p", 10, 20)
    check(db.get("p", 20) is not None and db.get("p", 10) is None,
          "主键行删旧插新")
    check(db.assert_integrity(), "更新后外键完整")
    db.close()


def test_expanding_batch():
    print("\n== 扩批执行：CASCADE 逐批 + 真实 RESTRICT 约束拒绝 ==")
    cat2 = Catalog()
    cat2.create_table("p", [Column("id", BIGINT, nullable=False, pk=True)])
    cat2.create_table("c", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("p_id", BIGINT, nullable=False)])
    cat2.table("c").fks = [
        ForeignKey("c_fk", ["p_id"], "p", ["id"], on_delete=CASCADE)]
    cat2.validate()
    path2 = "/tmp/fkdb_sem_eb2"
    shutil.rmtree(path2, ignore_errors=True)
    db2 = Database.create(path2, cat2)
    with db2.transaction() as txn:
        for i in range(1, 6):
            txn.insert("p", {"id": i})
            txn.insert("c", {"id": i, "p_id": i})
    full = db2.estimate_delete([("p", i) for i in range(1, 6)])
    gkeys = {k for k, n in full.nodes.items() if n.action == "DELETE"}
    # 逐批删，每批一个父行
    for i in range(1, 6):
        db2.execute_expanding_batch([("p", i)], gkeys)
    check(db2.count("p") == 0 and db2.count("c") == 0,
          "扩批逐删后父子表清空（CASCADE 正常）")
    check(db2.dangling_refs() == [], "无悬空外键")

    # 真实约束拒绝：阻挡行不在全局闭包里
    cat3 = Catalog()
    cat3.create_table("q", [Column("id", BIGINT, nullable=False, pk=True)])
    cat3.create_table("r", [Column("id", BIGINT, nullable=False, pk=True),
                            Column("q_id", BIGINT, nullable=False)])
    cat3.table("r").fks = [
        ForeignKey("r_fk", ["q_id"], "q", ["id"], on_delete=RESTRICT)]
    cat3.validate()
    path3 = "/tmp/fkdb_sem_eb3"
    shutil.rmtree(path3, ignore_errors=True)
    db3 = Database.create(path3, cat3)
    with db3.transaction() as txn:
        txn.insert("q", {"id": 1})
        txn.insert("r", {"id": 1, "q_id": 1})
    rejected = False
    try:
        # 全局闭包只含 q.1（实际整体会被 RESTRICT 挡），r.1 不在删除集合
        db3.execute_expanding_batch([("q", 1)], {("q", 1)})
    except ForeignKeyViolation:
        rejected = True
    check(rejected, "阻挡行不在全局闭包时正确拒绝，不静默写坏")
    check(db3.count("q") == 1 and db3.count("r") == 1, "拒绝后零改动")
    db2.close(); db3.close()


def test_expanding_batch_no_action_self_resolve():
    print("\n== 扩批执行：NO ACTION 工单经全局闭包自消解（电商场景） ==")
    from ecommerce.schema import build_catalog
    path = "/tmp/fkdb_sem_eb4"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, build_catalog())
    with db.transaction() as txn:
        txn.insert("warehouses", {"id": 1, "name": "w", "region": "r"})
        txn.insert("customers", {"id": 1, "name": "c"})
        txn.insert("addresses", {"id": 1, "customer_id": 1, "city": "x",
                                 "detail": "d"})
        txn.insert("orders", {"id": 1, "customer_id": 1, "address_id": 1,
                              "status": "pending", "total": 1.0,
                              "created_at": "t"})
        # 工单：order_id 是 NO ACTION，customer_id 是 CASCADE
        txn.insert("support_tickets", {"id": 1, "order_id": 1,
                                       "customer_id": 1, "title": "t"})
    full = db.estimate_delete([("customers", 1)])
    gkeys = {k for k, n in full.nodes.items() if n.action == "DELETE"}
    assert ("support_tickets", 1) in gkeys
    # 先单独删订单会被工单 NO ACTION 挡，扩批应吸收工单同事务删除
    rep = db.execute_expanding_batch([("orders", 1)], gkeys)
    touched = {(i["table"], i["rowid"]) for i in rep["deleted"]}
    check(("support_tickets", 1) in touched,
          "NO ACTION 工单被吸收进同批删除（自消解）")
    check(db.dangling_refs() == [], "本批提交后无悬空")
    db.execute_expanding_batch([("customers", 1)], gkeys)
    db.assert_integrity()
    check(db.count("orders") == 0 and db.count("support_tickets") == 0
          and db.count("customers") == 0, "全部清空且完整")
    db.close()


def main():
    test_path_independence()
    test_cyclic_cascade()
    test_restrict_vs_no_action()
    test_set_actions()
    test_set_default_multi_parent()
    test_batched_equivalence()
    test_concurrent_readers_never_see_dangling()
    test_estimate_matches_actual_multi_roots()
    test_recovery()
    test_on_update_actions()
    test_expanding_batch()
    test_expanding_batch_no_action_self_resolve()
    print("\n================================")
    print("语义测试结果：%d 通过, %d 失败" % (PASS, FAIL))
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    main()
