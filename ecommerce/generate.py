"""
电商数据生成器（确定性随机，种子固定后可复现）。

数据拓扑：
- 1 个大客户（id=1，is_key_account=True）+ 若干普通客户，客户之间有
  推荐人自引用链；大客户的订单/明细/物流/发票……占大头，模拟“几百万行”。
- 分类树、品牌、商品环、菜单树是共享主数据，不属于任何客户。
- 订单 -> 明细 -> 物流 -> 轨迹、发票、支付、领券、工单/留言、客户问题
  全部挂上外键，覆盖 schema 里的每一条边。

用法：
  python3 -m ecommerce.generate --db data/shop --customers 2000 --scale 1.0
scale 只放大“每个客户的业务单据量”，主数据规模不变。
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import time
from typing import Dict, List

from mini_fkdb import Database, FKDBError
from .schema import build_catalog

CITIES = ["上海", "北京", "深圳", "杭州", "广州", "成都", "武汉", "南京"]
METHODS = ["alipay", "wechat", "card", "cod"]
TICKET_TITLES = ["催发货", "退货咨询", "发票开错", "质量投诉", "改地址"]
ISSUE_SEV = ["low", "normal", "high", "urgent"]


class IDSeq:
    """每个表独立的自增计数器，先分配再写，保证外键 id 已知。"""

    def __init__(self):
        self.cur: Dict[str, int] = {}

    def next(self, table: str) -> int:
        n = self.cur.get(table, 0) + 1
        self.cur[table] = n
        return n


def generate(db_path: str, n_customers: int = 1000, scale: float = 1.0,
             seed: int = 20261128, wipe: bool = True,
             progress_every: int = 20000,
             ka_orders: int = 0) -> dict:
    """
    ka_orders: 大客户(id=1)的固定订单数。0 表示沿用 scale 推导；
    想要“几百万行关联数据”时显式给大数（如 200000），
    每张订单平均展开成 明细/物流/轨迹/发票/支付 等约 6-8 行。
    """
    if wipe and os.path.exists(db_path):
        shutil.rmtree(db_path)
    catalog = build_catalog()
    db = Database.create(db_path, catalog)
    rng = random.Random(seed)
    ids = IDSeq()
    counts: Dict[str, int] = {}
    t0 = time.monotonic()
    total_written = [0]

    def tick(table: str):
        counts[table] = counts.get(table, 0) + 1
        total_written[0] += 1
        if progress_every and total_written[0] % progress_every == 0:
            print("  ... 已写 %d 行，用时 %.1fs"
                  % (total_written[0], time.monotonic() - t0))

    with db.bulk_loader() as bl:
        # ------------------------------------------------ 主数据：仓库
        for wid in range(1, 6):
            row_id = ids.next("warehouses")
            bl.insert("warehouses", row_id, {
                "name": "仓库%d号" % wid,
                "region": CITIES[wid % len(CITIES)]})
            tick("warehouses")

        # ------------------------------------------------ 分类树（3 层）
        cat_id: Dict[int, int] = {}
        root_cats = []
        for i in range(1, 9):
            cid = ids.next("categories")
            bl.insert("categories", cid, {
                "name": "大类%d" % i, "parent_id": None,
                "featured_product_id": None})
            root_cats.append(cid)
            tick("categories")
        leaf_cats = []
        for parent in root_cats:
            for j in range(5):
                cid = ids.next("categories")
                bl.insert("categories", cid, {
                    "name": "子类%d-%d" % (parent, j),
                    "parent_id": parent, "featured_product_id": None})
                leaf_cats.append(cid)
                tick("categories")

        # ------------------------------------------------ 品牌（挂在大类下）
        brand_ids = []
        for i in range(1, 21):
            bid = ids.next("brands")
            bl.insert("brands", bid, {
                "name": "品牌%02d" % i,
                "category_id": root_cats[i % len(root_cats)]})
            brand_ids.append(bid)
            tick("brands")

        # ------------------------------------------------ 商品（环 A 闭合用）
        n_products = max(200, int(300 * scale))
        product_ids = []
        # 先占位行（featured_product_id 需要商品 id，先建商品再回填分类）
        for i in range(n_products):
            pid = ids.next("products")
            product_ids.append(pid)
            cat = leaf_cats[rng.randrange(len(leaf_cats))]
            brand = brand_ids[rng.randrange(len(brand_ids))]
            bl.insert("products", pid, {
                "sku": "SKU%06d" % pid,
                "name": "商品%04d" % pid,
                "category_id": cat, "brand_id": brand,
                "price": round(rng.uniform(9.9, 999.0), 2),
                "stock": rng.randint(0, 5000)})
            tick("products")
        # 回填分类的 featured_product_id —— 环 A：
        # categories.featured_product_id -> products -> categories
        for rc in root_cats:
            bl.insert("categories", rc, {
                "name": "大类%d" % rc, "parent_id": None,
                "featured_product_id": rng.choice(product_ids)})
            # bulk_load 是 append 新版本，旧版本仍在文件里但索引指向最新
            tick("categories")

        # ------------------------------------------------ 优惠券主数据
        coupon_ids = []
        for i in range(1, 31):
            cid = ids.next("coupons")
            bl.insert("coupons", cid, {
                "code": "CP%04d" % cid,
                "discount": round(rng.uniform(5, 200), 2)})
            coupon_ids.append(cid)
            tick("coupons")

        # ------------------------------------------------ 菜单树
        menu_root = ids.next("menus")
        bl.insert("menus", menu_root, {"label": "后台根菜单",
                                       "parent_id": None, "url": "/"})
        tick("menus")
        menu_parents = [menu_root]
        for i in range(8):
            mid = ids.next("menus")
            bl.insert("menus", mid, {
                "label": "菜单%d" % i, "parent_id": menu_root,
                "url": "/m/%d" % mid})
            menu_parents.append(mid)
            tick("menus")
        for parent in menu_parents[1:]:
            for j in range(4):
                mid = ids.next("menus")
                bl.insert("menus", mid, {
                    "label": "菜单%02d-%d" % (parent, j),
                    "parent_id": parent, "url": "/m/%d/%d" % (parent, mid)})
                tick("menus")

        # ------------------------------------------------ 客户
        customer_ids = []
        for i in range(1, n_customers + 1):
            cid = ids.next("customers")
            # 前 60% 的客户有推荐人，形成自引用链/树
            referrer = customer_ids[rng.randrange(len(customer_ids))] \
                if customer_ids and i > 1 and rng.random() < 0.6 else None
            bl.insert("customers", cid, {
                "name": ("大客户KA" if cid == 1 else "客户%05d" % cid),
                "tier": "KA" if cid == 1 else rng.choice(
                    ["standard", "silver", "gold"]),
                "referrer_id": referrer,
                "is_key_account": cid == 1,
                "created_at": "2026-01-%02dT08:00:00"
                              % (1 + (cid % 28))})
            customer_ids.append(cid)
            tick("customers")

        # 每个客户 1-3 个地址（大客户固定 4 个）
        addr_ids_by_customer: Dict[int, List[int]] = {}
        for cid in customer_ids:
            n_addr = 4 if cid == 1 else rng.randint(1, 3)
            own = []
            for _ in range(n_addr):
                aid = ids.next("addresses")
                bl.insert("addresses", aid, {
                    "customer_id": cid,
                    "kind": rng.choice(["shipping", "billing", "warehouse"]),
                    "city": rng.choice(CITIES),
                    "detail": "街道%d号%d室" % (rng.randint(1, 999),
                                               rng.randint(1, 20))})
                own.append(aid)
                tick("addresses")
            addr_ids_by_customer[cid] = own

        # ------------------------------------------------ 业务单据
        def orders_for(cid: int) -> int:
            if cid == 1:
                return ka_orders if ka_orders > 0 else \
                    max(20, int(120 * scale))      # 大客户主力
            return rng.choices([0, 1, 2, 3, 5],
                               weights=[25, 40, 20, 10, 5])[0]

        for cid in customer_ids:
            n_orders = orders_for(cid)
            for _ in range(n_orders):
                addrs = addr_ids_by_customer[cid]
                oid = ids.next("orders")
                approver = rng.choice(customer_ids) if rng.random() < 0.4 \
                    else None
                bl.insert("orders", oid, {
                    "customer_id": cid,
                    "approver_id": approver,
                    "address_id": rng.choice(addrs),
                    "status": rng.choice(["pending", "paid", "shipped",
                                          "done", "refund"]),
                    "total": 0.0,
                    "created_at": "2026-%02d-%02dT%02d:00:00"
                                  % (rng.randint(1, 11), rng.randint(1, 28),
                                     rng.randint(0, 23))})
                tick("orders")

                total = 0.0
                # 订单明细 1-6 行
                for _ in range(rng.randint(1, 6)):
                    pid = rng.choice(product_ids)
                    prod = db.storage.pk["products"][pid]
                    qty = rng.randint(1, 5)
                    itid = ids.next("order_items")
                    bl.insert("order_items", itid, {
                        "order_id": oid, "product_id": pid, "qty": qty,
                        "unit_price": prod["price"]})
                    total += qty * prod["price"]
                    tick("order_items")

                # 回填订单总额（新版本行）
                cur_order = dict(db.storage.pk["orders"][oid])
                cur_order["total"] = round(total, 2)
                bl.insert("orders", oid, cur_order)

                # 物流：两条地址外键 + 仓库默认值
                sid = ids.next("shipments")
                bl.insert("shipments", sid, {
                    "order_id": oid,
                    "from_address_id": rng.choice(addrs),
                    "to_address_id": rng.choice(addrs),
                    "warehouse_id": rng.randint(1, 5),
                    "tracking_no": "SF%010d" % sid,
                    "shipped_at": "2026-10-01T10:00:00"})
                tick("shipments")
                for ev in range(rng.randint(1, 4)):
                    eid = ids.next("shipment_events")
                    bl.insert("shipment_events", eid, {
                        "shipment_id": sid,
                        "event": ["揽收", "转运", "派送", "签收"][ev % 4],
                        "at": "2026-10-0%dT1%d:00:00" % (ev + 1, ev)})
                    tick("shipment_events")

                # 发票（大客户 80% 有发票；customer_id 是 SET NULL 边）
                if rng.random() < (0.8 if cid == 1 else 0.4):
                    invid = ids.next("invoices")
                    bl.insert("invoices", invid, {
                        "order_id": oid, "customer_id": cid,
                        "amount": round(total, 2),
                        "status": rng.choice(["issued", "paid", "void"])})
                    tick("invoices")

                # 支付
                payid = ids.next("payments")
                bl.insert("payments", payid, {
                    "order_id": oid, "method": rng.choice(METHODS),
                    "amount": round(total, 2),
                    "paid_at": "2026-10-01T09:00:00"})
                tick("payments")

                # 领券记录（coupon RESTRICT 边；order 边可空）
                if rng.random() < 0.5:
                    rid = ids.next("coupon_redemptions")
                    bl.insert("coupon_redemptions", rid, {
                        "coupon_id": rng.choice(coupon_ids),
                        "customer_id": cid, "order_id": oid,
                        "redeemed_at": "2026-09-01T09:00:00"})
                    tick("coupon_redemptions")

                # 工单（环 B：NO ACTION 边），约 1/3 订单有工单
                if rng.random() < 0.33:
                    tid = ids.next("support_tickets")
                    bl.insert("support_tickets", tid, {
                        "order_id": oid, "customer_id": cid,
                        "title": rng.choice(TICKET_TITLES),
                        "state": rng.choice(["open", "pending", "closed"])})
                    tick("support_tickets")
                    # 订单回填主工单，闭合环 B（orders 没有 ticket 列，
                    # 环通过 ticket -> order 与级联删除路径闭合，见下方说明）
                    for _ in range(rng.randint(1, 3)):
                        nid = ids.next("ticket_notes")
                        bl.insert("ticket_notes", nid, {
                            "ticket_id": tid,
                            "author_id": rng.choice(customer_ids),
                            "body": "沟通记录%d" % nid})
                        tick("ticket_notes")

            # 购物车 + 明细（即使没下单也可能有车）
            if rng.random() < 0.7:
                cart_id = ids.next("carts")
                bl.insert("carts", cart_id, {
                    "customer_id": cid,
                    "status": rng.choice(["open", "abandoned"])})
                tick("carts")
                for _ in range(rng.randint(1, 4)):
                    ciid = ids.next("cart_items")
                    bl.insert("cart_items", ciid, {
                        "cart_id": cart_id,
                        "product_id": rng.choice(product_ids),
                        "qty": rng.randint(1, 3)})
                    tick("cart_items")

            # 客户问题：两条客户外键（报单人 CASCADE / 处理人 SET NULL）
            if rng.random() < 0.35:
                issue_id = ids.next("customer_issues")
                bl.insert("customer_issues", issue_id, {
                    "reporter_id": cid,
                    "handler_id": rng.choice(customer_ids),
                    "product_id": rng.choice(product_ids),
                    "severity": rng.choice(ISSUE_SEV),
                    "detail": "问题描述%d" % issue_id})
                tick("customer_issues")

    db.checkpoint()
    # 完整性自检
    dangling = db.dangling_refs()
    if dangling:
        raise FKDBError("生成数据存在悬空外键: %s" % dangling[:3])
    db.close()

    meta = {"customers": n_customers, "scale": scale, "seed": seed,
            "counts": counts, "rows": sum(counts.values()),
            "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "key_account_id": 1}
    with open(os.path.join(db_path, "gen_meta.json"), "w",
              encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print("生成完成：%d 行，%d 张表，用时 %.1fs，库目录 %s"
          % (meta["rows"], len(counts), time.monotonic() - t0, db_path))
    return meta


def main():
    ap = argparse.ArgumentParser(description="生成电商测试库")
    ap.add_argument("--db", default="data/shop")
    ap.add_argument("--customers", type=int, default=1000)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=20261128)
    ap.add_argument("--ka-orders", type=int, default=0,
                    help="大客户固定订单数（百万行场景用，如 200000）")
    args = ap.parse_args()
    generate(args.db, args.customers, args.scale, args.seed,
             ka_orders=args.ka_orders)


if __name__ == "__main__":
    main()
