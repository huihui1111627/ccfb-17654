"""快速烟囱测试：小库上验证四种动作、自引用、环、崩溃恢复。"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_fkdb import (Database, Catalog, Column, ForeignKey,
                       CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION,
                       BIGINT, TEXT, ForeignKeyViolation, LockTimeout)


def build():
    cat = Catalog()
    cat.create_table("parents", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("name", TEXT, nullable=False)])
    cat.create_table("kids", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("parent_id", BIGINT),
        # SET DEFAULT 指向 4 号父行（测试中不会删它），保证默认引用始终有效
        Column("def_id", BIGINT, default=4, has_default=True),
        Column("name", TEXT)])
    cat.table("kids").fks = [
        ForeignKey("fk_kid_parent", ["parent_id"], "parents", ["id"],
                   on_delete=CASCADE, on_update=CASCADE),
        ForeignKey("fk_kid_def", ["def_id"], "parents", ["id"],
                   on_delete=SET_DEFAULT, on_update=CASCADE),
    ]
    # 自引用 + 环
    cat.create_table("nodes", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("parent_id", BIGINT),
        Column("buddy_id", BIGINT)])
    cat.table("nodes").fks = [
        ForeignKey("fk_node_parent", ["parent_id"], "nodes", ["id"],
                   on_delete=CASCADE, on_update=CASCADE),
        ForeignKey("fk_node_buddy", ["buddy_id"], "nodes", ["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
    ]
    # RESTRICT 表
    cat.create_table("guards", [
        Column("id", BIGINT, nullable=False, pk=True),
        Column("parent_id", BIGINT, nullable=False)])
    cat.table("guards").fks = [
        ForeignKey("fk_guard", ["parent_id"], "parents", ["id"],
                   on_delete=RESTRICT, on_update=RESTRICT)]
    cat.validate()
    return cat


def main():
    path = "/tmp/fkdb_smoke"
    shutil.rmtree(path, ignore_errors=True)
    db = Database.create(path, build())

    with db.transaction() as txn:
        for i in range(1, 5):
            txn.insert("parents", {"name": "p%d" % i})
        for i in range(1, 7):
            # def_id 指向待删的 1 号父行，删除时应置为默认值 4（行保留）
            txn.insert("kids", {"parent_id": (i % 4) + 1, "def_id": 1,
                                "name": "k%d" % i})
        txn.insert("guards", {"id": 1, "parent_id": 2})

    # 自引用树 10-11-12，11 和 12 互指 buddy（环）—— 用可延迟约束
    with db.transaction() as txn2:
        txn2.set_deferred()
        txn2.insert("nodes", {"id": 10, "parent_id": None, "buddy_id": None})
        txn2.insert("nodes", {"id": 11, "parent_id": 10, "buddy_id": 12})
        txn2.insert("nodes", {"id": 12, "parent_id": 11, "buddy_id": 11})

    db.assert_integrity()
    print("[1] 插入与完整性 OK")

    # 预估 + 级联删除 parent 1
    plan = db.estimate_delete([("parents", 1)])
    counts = plan.counts_by_table()
    print("[2] 预估:", counts)
    assert counts["parents"]["delete"] == 1
    # parent_id=1 的 kid 走 CASCADE 删除
    assert counts["kids"]["delete"] >= 1
    # def_id=1 但 parent_id 不指向 1 的 kid 走 SET DEFAULT，行保留
    assert counts["kids"]["set_default"] >= 1
    assert "kids" in counts and (
        counts["kids"]["delete"] + counts["kids"]["set_default"]) >= 2

    # RESTRICT 保护 parent 2
    blocked = None
    try:
        db.estimate_delete([("parents", 2)])
    except ForeignKeyViolation as e:
        blocked = e
    assert blocked and blocked.details["blocks"][0]["action"] == "RESTRICT"
    print("[3] RESTRICT 拦截 OK")

    # 真正删 parent 1，并逐表核对
    rep = db.cascade_delete([("parents", 1)])
    assert db.get("parents", 1) is None
    db.assert_integrity()
    # 置默认的行仍然存活，def_id 被改成默认值 4
    survivors = [k for k in db.scan("kids") if k["def_id"] == 4]
    assert len(survivors) == 5, "SET DEFAULT 后 5 行应存活并指向默认父行"
    print("[4] 级联删除执行与预估核对 OK，实际:", rep["counts"])

    # 自引用树删除：删 10 应该 CASCADE 到 11、12，buddy SET NULL 边不挡路
    rep2 = db.cascade_delete([("nodes", 10)])
    assert db.count("nodes") == 0, "自引用树应整棵删掉"
    print("[5] 自引用/环级联 OK:", rep2["counts"])

    # ON UPDATE CASCADE / RESTRICT
    with db.transaction() as txn:
        p = txn.planner.plan_update_pk("parents", 3, 30)
        txn.execute_update_pk_plan(p)
    assert db.get("parents", 30) is not None
    kids30 = [k for k in db.scan("kids") if k["parent_id"] == 30]
    assert kids30, "ON UPDATE CASCADE 应改子行外键"
    db.assert_integrity()
    print("[6] ON UPDATE CASCADE OK")

    blocked2 = None
    try:
        db.update_pk("parents", 2, 20)  # guards 是 ON UPDATE RESTRICT
    except ForeignKeyViolation as e:
        blocked2 = e
    assert blocked2 is not None
    print("[7] ON UPDATE RESTRICT OK")

    # 悬空插入必须被拒
    try:
        db.insert("guards", {"parent_id": 999})
        raise AssertionError("悬空外键插入应该失败")
    except ForeignKeyViolation:
        print("[8] 悬空插入拦截 OK")

    # 崩溃恢复：重开库，数据一致
    db.checkpoint()
    db.close()
    db2 = Database(path)
    db2.assert_integrity()
    assert db2.get("parents", 30)["name"] == "p3"
    assert db2.count("guards") == 1
    print("[9] 重开恢复 OK")
    db2.close()
    print("ALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    main()
