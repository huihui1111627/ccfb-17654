"""
实验编排：把题目的验收项跑成一份可核对的报告。

阶段 0  建库（若不存在）并打印规模
阶段 1  对一批客户分别做预估，再实际删除，逐表核对预估 == 实际
阶段 2  RESTRICT 拦截演示（尝试删被引用的仓库/品牌，应被挡住且零改动）
阶段 3  大客户一次性删除 vs 拆批删除，各自在并发业务负载下测等待时间
阶段 4  全程悬空外键监测 + 最终外键完整性
输出    experiments/out/report.json（机器可读）+ 控制台摘要
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sys
import time
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_fkdb import Database, ForeignKeyViolation, FKDBError
from ecommerce.generate import generate
from .workload import BusinessWorkload


def _clone_db(src: str, dst: str):
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)


def _table_snapshot(db: Database) -> Dict[str, List[int]]:
    return {t: sorted(db.storage.all_rowids(t)) for t in db.table_names()}


def stage1_estimate_vs_actual(db_path: str, victim_ids: List[int],
                              results: dict):
    """逐客户：预估 → 拆批实删 → 逐表核对。"""
    print("\n[阶段1] 逐客户预估 vs 实际删除（%d 个客户）" % len(victim_ids))
    rows = []
    for cid in victim_ids:
        db = Database(db_path)
        plan = db.estimate_delete([("customers", cid)])
        est = plan.counts_by_table()
        depth = max((n.depth for n in plan.nodes.values()), default=0)
        t0 = time.monotonic()
        manifest = db.cascade_delete_batched([("customers", cid)],
                                             batch_size=1)
        elapsed = (time.monotonic() - t0) * 1000
        db.assert_integrity()
        # 核对删除行数
        match = True
        mismatch = []
        for t in set(est) | set(manifest["counts"]):
            e = est.get(t, {}).get("delete", 0)
            a = manifest["counts"].get(t, {}).get("delete", 0)
            if e != a:
                match = False
                mismatch.append({"table": t, "estimate": e, "actual": a})
        rows.append({
            "customer_id": cid,
            "depth": depth,
            "estimated": est,
            "actual": manifest["counts"],
            "delete_match": match,
            "mismatch": mismatch,
            "set_null_total": len(manifest["set_null"]),
            "set_default_total": len(manifest["set_default"]),
            "elapsed_ms": round(elapsed, 2)})
        db.close()
    results["stage1_per_customer"] = rows
    ok = all(r["delete_match"] for r in rows)
    print("  逐表核对结果：%s（%d/%d 客户预估与实际一致）"
          % ("全部通过" if ok else "存在不一致",
             sum(1 for r in rows if r["delete_match"]), len(rows)))
    return ok


def stage2_restrict_block(db_path: str, results: dict):
    print("\n[阶段2] RESTRICT / NO ACTION 拦截演示")
    db = Database(db_path)
    before = _table_snapshot(db)
    blocks = []

    # 仓库被 shipments 引用（SET DEFAULT 不挡，但品牌被 products RESTRICT）
    brand = db.scan("brands")[0]
    try:
        db.estimate_delete([("brands", brand["id"])])
    except ForeignKeyViolation as e:
        blocks.append({"target": ("brands", brand["id"]),
                       "kind": e.kind,
                       "first_block": e.details["blocks"][0]})

    # 优惠券被 coupon_redemptions RESTRICT
    coupon = next(c for c in db.scan("coupons")
                  if db.storage.find_by_fk("coupon_redemptions",
                                           "fk_redeem_coupon", c["id"]))
    try:
        db.estimate_delete([("coupons", coupon["id"])])
    except ForeignKeyViolation as e:
        blocks.append({"target": ("coupons", coupon["id"]),
                       "kind": e.kind,
                       "first_block": e.details["blocks"][0]})

    after = _table_snapshot(db)
    unchanged = before == after
    results["stage2_blocks"] = blocks
    results["stage2_zero_change"] = unchanged
    db.close()
    print("  拦截次数：%d，被挡后数据库零改动：%s" % (len(blocks), unchanged))
    for b in blocks:
        print("   -", b["target"], "被", b["first_block"]["fk"], "挡住")
    return len(blocks) >= 2 and unchanged


def _run_deletion_with_workload(db_path: str, root_ids: List[int],
                                batched: bool, batch_size: int,
                                safe_customer: int, n_workers: int):
    db = Database(db_path)
    # 记录删除前的行 id 集合，用于剔除并发 worker 运行期新增的行，
    # 保证两种模式只比较“删除动作本身”的结果
    pre_snapshot = _table_snapshot(db)
    safe_ids = [r["id"] for r in db.scan("customers")
                if r["id"] not in root_ids]
    products = [r["id"] for r in db.scan("products")]
    addrs = [r["id"] for r in db.scan("addresses")]
    # 热行 = 删除根（大客户）。读它会与删除事务的行 X 锁竞争，
    # 用于测量“其他客户被卡多久”这个最贴近事故的指标。
    hot_rows = [("customers", r) for r in root_ids]
    wl = BusinessWorkload(db, safe_ids, products, addrs,
                          n_workers=n_workers, hot_row_ids=hot_rows)
    # 先让业务跑一会儿建立稳定基线
    wl.start()
    time.sleep(0.5)
    db.locker.reset_stats()

    t0 = time.monotonic()
    if batched:
        # 正确的拆批：客户根只在最后一批删。前面各批以订单为叶子锚点
        # 独立删子树（订单的入边级联到明细/物流/发票…），完全不碰客户根，
        # 因此每批闭包有界、热行(客户)在绝大部分时间不被加锁；
        # 最后客户根名下订单已清空，它那一批的级联闭包也很小。
        if len(root_ids) == 1:
            cid = root_ids[0]
            # 一次性全量预估，得到全局删除闭包：拆批时用来判断某个挡住
            # 当前批的子行是不是“本来就要随大客户一起删”（NO ACTION 自消解）
            global_plan = db.estimate_delete([("customers", cid)])
            if global_plan.blockage():
                raise ForeignKeyViolation(
                    "全局删除被阻止", kind="blocked",
                    details={"blocks": global_plan.blockage()})
            global_delete_keys = {
                k for k, n in global_plan.nodes.items()
                if n.action == "DELETE"}

            order_ids = sorted(db.storage.find_by_fk(
                "orders", "fk_order_customer", cid))
            chunks = [order_ids[i:i + batch_size]
                      for i in range(0, len(order_ids), batch_size)]
            roots_batches = [[("orders", o) for o in c] for c in chunks]
            roots_batches.append([("customers", cid)])  # 最后才删根
            manifest = {"deleted": [], "set_null": [], "set_default": [],
                        "chains": [], "batches": [], "counts": {}}
            seen = set()
            actual = manifest["counts"]

            def bump(table, action):
                b = actual.setdefault(
                    table, {"delete": 0, "set_null": 0,
                            "set_default": 0, "blocked": 0})
                b[action] += 1

            for idx, roots in enumerate(roots_batches):
                bt0 = time.monotonic()
                # 扩批执行：被 NO ACTION/RESTRICT 挡住且属于全局闭包的行，
                # 吸收进本批同事务删除（自消解）；否则按约束报错
                rep = db.execute_expanding_batch(roots, global_delete_keys)
                for item in rep["deleted"]:
                    sig = (item["table"], item["rowid"], "delete")
                    if sig in seen:
                        continue
                    seen.add(sig)
                    manifest["deleted"].append(item)
                    bump(item["table"], "delete")
                for item in rep["set_null"]:
                    sig = (item["table"], item["rowid"], "set_null",
                           item.get("col"))
                    if sig in seen:
                        continue
                    seen.add(sig)
                    manifest["set_null"].append(item)
                    bump(item["table"], "set_null")
                for item in rep["set_default"]:
                    sig = (item["table"], item["rowid"], "set_default",
                           item.get("col"))
                    if sig in seen:
                        continue
                    seen.add(sig)
                    manifest["set_default"].append(item)
                    bump(item["table"], "set_default")
                manifest["batches"].append({
                    "index": idx,
                    "roots": roots,
                    "duration_ms": round((time.monotonic() - bt0) * 1000, 2),
                    "dangling_after_commit": len(db.dangling_refs())})
            manifest["batch_count"] = len(roots_batches)
        else:
            manifest = db.cascade_delete_batched(
                [("customers", c) for c in root_ids],
                batch_size=batch_size, verify=True)
        mode = "batched"
    else:
        manifest = db.cascade_delete(
            [("customers", c) for c in root_ids], verify=True)
        mode = "one_shot"
    delete_ms = (time.monotonic() - t0) * 1000

    time.sleep(0.3)
    wl.stop()
    final_dangling = db.dangling_refs()
    post_snapshot = _table_snapshot(db)
    db.assert_integrity()
    db.close()

    # 只比较删除前就存在的行：每个预存 id 最终在/不在，两模式必须一致
    pre_delta = {t: set(pre_snapshot[t]) for t in pre_snapshot}
    post_delta = {t: set(post_snapshot.get(t, [])) for t in pre_snapshot}
    surviving_preexisting = {
        t: sorted(pre_delta[t] & post_delta[t]) for t in pre_delta}

    return {
        "mode": mode,
        "batch_size": batch_size if batched else None,
        "delete_elapsed_ms": round(delete_ms, 2),
        "manifest_summary": manifest.get("counts", {}),
        "batch_count": manifest.get("batch_count", 1),
        "per_batch_ms": [b["duration_ms"]
                         for b in manifest.get("batches", [])],
        "per_batch_dangling": [b["dangling_after_commit"]
                               for b in manifest.get("batches", [])],
        "workload": wl.stats.summary(),
        "final_dangling_count": len(final_dangling),
        "surviving_preexisting": surviving_preexisting}


def stage3_concurrency(base_db: str, ka_id: int, results: dict,
                       n_workers: int, batch_size: int):
    print("\n[阶段3] 一次性删除 vs 拆批删除（并发 %d 个业务线程）"
          % n_workers)
    path_one = base_db + "_oneshot"
    path_batch = base_db + "_batch"
    _clone_db(base_db, path_one)
    _clone_db(base_db, path_batch)

    print("  跑一次性删除 ...")
    r_one = _run_deletion_with_workload(
        path_one, [ka_id], batched=False, batch_size=batch_size,
        safe_customer=2, n_workers=n_workers)
    print("  跑拆批删除（batch_size=%d）..." % batch_size)
    r_batch = _run_deletion_with_workload(
        path_batch, [ka_id], batched=True, batch_size=batch_size,
        safe_customer=2, n_workers=n_workers)

    # 最终行集合等价性：只比删除前已存在的行（剔除 worker 运行期新增）
    snap_equal = r_one["surviving_preexisting"] == \
        r_batch["surviving_preexisting"]

    results["stage3"] = {"one_shot": r_one, "batched": r_batch,
                         "snapshot_equal": snap_equal}
    w1, w2 = r_one["workload"], r_batch["workload"]
    print("  一次性：删除耗时 %.0fms，业务操作 %d 个，等待 p95 %.2fms，"
          "等待总和 %.1fms"
          % (r_one["delete_elapsed_ms"], w1["total"],
             w1["wait_ms"]["p95"], w1["wait_ms"]["sum"]))
    print("  拆批：  删除耗时 %.0fms（%d 批），业务操作 %d 个，等待 p95 %.2fms，"
          "等待总和 %.1fms"
          % (r_batch["delete_elapsed_ms"], r_batch["batch_count"],
             w2["total"], w2["wait_ms"]["p95"], w2["wait_ms"]["sum"]))
    print("  两种删法最终行集合一致：%s；全程悬空外键：one_shot=%d batch=%d"
          % (snap_equal, r_one["final_dangling_count"],
             r_batch["final_dangling_count"]))
    return snap_equal and not r_one["final_dangling_count"] \
        and not r_batch["final_dangling_count"]


def stage4_chain_manifest(db_path: str, cid: int, results: dict):
    """导出一条完整外键链清单样例，验证'从哪条链传过来'可追溯。"""
    print("\n[阶段4] 外键链清单抽样（customer=%d）" % cid)
    db = Database(db_path)
    plan = db.estimate_delete([("customers", cid)])
    samples = []
    for n in plan.nodes.values():
        if n.chain and len(samples) < 8:
            samples.append({
                "table": n.table, "rowid": n.rowid, "action": n.action,
                "via_fk": n.via_fk,
                "chain": [{"fk": h.fk_name, "from": "%s.%d"
                           % (h.from_table, h.from_rowid)}
                          for h in n.chain],
                "alt_paths": len(n.all_chains)})
    results["stage4_chain_samples"] = samples
    db.close()
    for s in samples[:4]:
        print("  %s.%d [%s] via %s 链长%d 备选路径%d"
              % (s["table"], s["rowid"], s["action"], s["via_fk"],
                 len(s["chain"]), s["alt_paths"]))


def main():
    ap = argparse.ArgumentParser(description="跑全套验收实验")
    ap.add_argument("--db", default="data/shop")
    ap.add_argument("--out", default="experiments/out")
    ap.add_argument("--customers", type=int, default=300)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--victims", type=int, default=5,
                    help="阶段1 逐客户核对的客户数")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument("--keep-db", action="store_true",
                    help="库已存在则不重新生成")
    ap.add_argument("--ka-orders", type=int, default=0,
                    help="大客户固定订单数（百万行场景）")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    if not (args.keep_db and os.path.exists(args.db)):
        print("[阶段0] 生成电商库 ...")
        generate(args.db, n_customers=args.customers, scale=args.scale,
                 ka_orders=args.ka_orders)

    db = Database(args.db)
    meta = {t: db.count(t) for t in db.table_names()}
    ka_id = 1
    ka_orders = len(db.storage.find_by_fk(
        "orders", "fk_order_customer", ka_id))
    print("  库规模：总行数 %d，大客户(ka=%d)订单 %d"
          % (sum(meta.values()), ka_id, ka_orders))
    db.close()

    results = {"db_path": os.path.abspath(args.db),
               "table_counts": meta,
               "key_account": {"id": ka_id, "orders": ka_orders},
               "params": vars(args)}

    # 阶段1 会真删数据，所以在副本上做，保留主库给阶段3
    stage1_path = args.db + "_stage1"
    _clone_db(args.db, stage1_path)
    victim_db = Database(stage1_path)
    victim_ids = [r["id"] for r in victim_db.scan("customers")
                  if r["id"] != ka_id][:args.victims]
    victim_db.close()
    ok1 = stage1_estimate_vs_actual(stage1_path, victim_ids, results)

    ok2 = stage2_restrict_block(args.db, results)
    ok3 = stage3_concurrency(args.db, ka_id, results,
                             args.workers, args.batch_size)
    stage4_chain_manifest(args.db, ka_id, results)

    results["verdict"] = {
        "stage1_estimate_matches_actual": ok1,
        "stage2_restrict_blocks_and_zero_change": ok2,
        "stage3_batch_equiv_and_no_dangling": ok3}
    results["all_passed"] = ok1 and ok2 and ok3

    out_file = os.path.join(args.out, "report.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\n报告已写入 %s" % out_file)
    print("================ 总结 ================")
    for k, v in results["verdict"].items():
        print("  [%s] %s" % ("PASS" if v else "FAIL", k))
    if not results["all_passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
