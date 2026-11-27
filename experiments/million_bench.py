"""
百万行级并发删除基准（可复跑）。

构造一个大客户承载约百万级关联行的库，在 4 个并发业务线程（含直接读
大客户热行）下对比：
  - 一次性大事务级联删除
  - 扩批拆删（订单叶子锚点先行、客户根最后删，NO ACTION 自动消解）

输出 experiments/out/million_bench.json。核心看热行读最大等待：
一次性删除会长时间持有客户行 X 锁，读者被卡；拆批每批短持锁，等待骤降。

用法：
  python3 -I run.py gen --db data/million --customers 2000 --ka-orders 150000
  python3 -I experiments/million_bench.py --db data/million --out experiments/out
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from experiments.run_experiments import _run_deletion_with_workload


def main():
    ap = argparse.ArgumentParser(description="百万行并发删除基准")
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="experiments/out")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    path_one = args.db.rstrip("/") + "_mb_one"
    path_bat = args.db.rstrip("/") + "_mb_bat"
    shutil.rmtree(path_one, ignore_errors=True)
    shutil.rmtree(path_bat, ignore_errors=True)
    shutil.copytree(args.db, path_one)
    shutil.copytree(args.db, path_bat)

    print("一次性大事务删除 ...", flush=True)
    t0 = time.monotonic()
    one = _run_deletion_with_workload(
        path_one, [1], batched=False, batch_size=args.batch_size,
        safe_customer=2, n_workers=args.workers)
    one["wall_including_workload_s"] = round(time.monotonic() - t0, 2)

    print("扩批拆删 ...", flush=True)
    t0 = time.monotonic()
    bat = _run_deletion_with_workload(
        path_bat, [1], batched=True, batch_size=args.batch_size,
        safe_customer=2, n_workers=args.workers)
    bat["wall_including_workload_s"] = round(time.monotonic() - t0, 2)

    equivalent = one["surviving_preexisting"] == \
        bat["surviving_preexisting"]
    hot1 = one["workload"]["by_kind"].get("hot_row_read", {}).get(
        "wait_ms", {})
    hot2 = bat["workload"]["by_kind"].get("hot_row_read", {}).get(
        "wait_ms", {})

    result = {
        "one_shot": one, "batched": bat,
        "equivalent_final_state": equivalent,
        "every_batch_zero_dangling": set(
            bat.get("per_batch_dangling", [])) <= {0},
        "headline": {
            "one_shot_delete_s": one["delete_elapsed_ms"] / 1000,
            "batched_delete_s": bat["delete_elapsed_ms"] / 1000,
            "batch_count": bat["batch_count"],
            "one_shot_hot_read_max_wait_ms": hot1.get("max"),
            "batched_hot_read_max_wait_ms": hot2.get("max"),
            "one_shot_total_wait_ms": one["workload"]["wait_ms"]["sum"],
            "batched_total_wait_ms": bat["workload"]["wait_ms"]["sum"],
            "one_shot_business_ops": one["workload"]["total"],
            "batched_business_ops": bat["workload"]["total"]}}

    out_file = os.path.join(args.out, "million_bench.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    h = result["headline"]
    print("\n================ 百万行并发删除对比 ================")
    print("一次性：删除 %.1fs，热行读最大等待 %.0fms，业务操作 %d，总等待 %.0fms"
          % (h["one_shot_delete_s"], h["one_shot_hot_read_max_wait_ms"],
             h["one_shot_business_ops"], h["one_shot_total_wait_ms"]))
    print("拆批：  删除 %.1fs（%d 批），热行读最大等待 %.0fms，业务操作 %d，"
          "总等待 %.0fms"
          % (h["batched_delete_s"], h["batch_count"],
             h["batched_hot_read_max_wait_ms"],
             h["batched_business_ops"], h["batched_total_wait_ms"]))
    print("最终状态等价：%s；每批零悬空：%s"
          % (equivalent, result["every_batch_zero_dangling"]))
    print("结果已写入", out_file)


if __name__ == "__main__":
    main()
