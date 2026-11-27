"""
电商库表结构（16 张表）。

特意构造的外键形态：
- 自引用树：categories.parent_id、customers.referrer_id、menus.parent_id
- 同表多条外键指向同一张表：
    orders          -> customers（下单人 + 审批人两条）
    customer_issues -> customers（报单人 + 处理客服两条）
    shipments       -> addresses（发货仓地址 + 收货地址两条）
- 表间环：
    A. products -> brands -> categories -> products
       （品牌挂在分类下，分类的 featured_product 又指回商品）
    B. support_tickets -> orders -> ... -> support_tickets
       （订单挂主工单，工单又引用订单）
- 四种动作齐全：
    CASCADE     orders/order_items/... 客户注销主链路
    SET NULL    invoices.customer_id（复现“无主发票”事故，这里显式可见）
    SET DEFAULT shipments.warehouse_id（默认仓 1 不能被删，另有 RESTRICT 保护）
    RESTRICT   warehouses / brands 等基础数据被引用时禁止删
    NO ACTION  support_tickets.order_id（延迟检查，环内可级联自消解）
"""
from mini_fkdb import (Catalog, Table, Column, ForeignKey,
                       CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION,
                       INTEGER, BIGINT, TEXT, REAL, BOOLEAN)


def build_catalog() -> Catalog:
    cat = Catalog()

    def col(name, ctype, nullable=True, default=None, has_default=False,
            pk=False):
        return Column(name, ctype, nullable=nullable, default=default,
                      has_default=has_default, pk=pk)

    def fk(name, table_col, ref_table, on_delete=RESTRICT,
           on_update=CASCADE):
        col_name = table_col
        return ForeignKey(name=name, columns=[col_name],
                          ref_table=ref_table, ref_columns=["id"],
                          on_delete=on_delete, on_update=on_update)

    # 1. 客户（自引用推荐人树；注销走 CASCADE 传播）
    cat.create_table("customers", [
        col("id", BIGINT, nullable=False, pk=True),
        col("name", TEXT, nullable=False),
        col("tier", TEXT, default="standard", has_default=True),
        col("referrer_id", BIGINT),                # 自引用
        col("is_key_account", BOOLEAN, default=False, has_default=True),
        col("created_at", TEXT),
    ], fks=[
        fk("fk_customer_referrer", "referrer_id", "customers",
           on_delete=SET_NULL, on_update=CASCADE),
    ])

    # 2. 客户地址
    cat.create_table("addresses", [
        col("id", BIGINT, nullable=False, pk=True),
        col("customer_id", BIGINT, nullable=False),
        col("kind", TEXT, default="shipping", has_default=True),
        col("city", TEXT),
        col("detail", TEXT),
    ], fks=[
        fk("fk_address_customer", "customer_id", "customers",
           on_delete=CASCADE),
    ])

    # 3. 分类树（自引用）
    cat.create_table("categories", [
        col("id", BIGINT, nullable=False, pk=True),
        col("name", TEXT, nullable=False),
        col("parent_id", BIGINT),
        col("featured_product_id", BIGINT),         # 环 A 的回边，稍后补
    ], fks=[
        fk("fk_category_parent", "parent_id", "categories",
           on_delete=CASCADE),
    ])

    # 4. 品牌（被分类管辖；被商品引用，禁止直接删）
    cat.create_table("brands", [
        col("id", BIGINT, nullable=False, pk=True),
        col("name", TEXT, nullable=False),
        col("category_id", BIGINT),
    ], fks=[
        fk("fk_brand_category", "category_id", "categories",
           on_delete=CASCADE),
    ])

    # 5. 仓库（基础数据，RESTRICT 保护；shipments 置默认值指向 1 号仓）
    cat.create_table("warehouses", [
        col("id", BIGINT, nullable=False, pk=True),
        col("name", TEXT, nullable=False),
        col("region", TEXT),
    ])

    # 6. 商品（环 A：brand -> category -> product）
    cat.create_table("products", [
        col("id", BIGINT, nullable=False, pk=True),
        col("sku", TEXT, nullable=False),
        col("name", TEXT, nullable=False),
        col("category_id", BIGINT),
        col("brand_id", BIGINT),
        col("price", REAL, default=0.0, has_default=True),
        col("stock", INTEGER, default=0, has_default=True),
    ], fks=[
        fk("fk_product_category", "category_id", "categories",
           on_delete=CASCADE),
        fk("fk_product_brand", "brand_id", "brands", on_delete=RESTRICT),
    ])

    # 分类的回边（环 A 闭合）。SET NULL：招牌商品没了，分类还在。
    cat.table("categories").fks.append(ForeignKey(
        name="fk_category_featured",
        columns=["featured_product_id"], ref_table="products",
        ref_columns=["id"], on_delete=SET_NULL, on_update=CASCADE))

    # 7. 购物车（客户级联删）
    cat.create_table("carts", [
        col("id", BIGINT, nullable=False, pk=True),
        col("customer_id", BIGINT, nullable=False),
        col("status", TEXT, default="open", has_default=True),
    ], fks=[
        fk("fk_cart_customer", "customer_id", "customers",
           on_delete=CASCADE),
    ])

    # 8. 购物车明细
    cat.create_table("cart_items", [
        col("id", BIGINT, nullable=False, pk=True),
        col("cart_id", BIGINT, nullable=False),
        col("product_id", BIGINT, nullable=False),
        col("qty", INTEGER, default=1, has_default=True),
    ], fks=[
        fk("fk_cartitem_cart", "cart_id", "carts", on_delete=CASCADE),
        fk("fk_cartitem_product", "product_id", "products",
           on_delete=RESTRICT),
    ])

    # 9. 订单（同表两条外键指向 customers）
    cat.create_table("orders", [
        col("id", BIGINT, nullable=False, pk=True),
        col("customer_id", BIGINT, nullable=False),
        col("approver_id", BIGINT),                  # 审批客服，第二条客户外键
        col("address_id", BIGINT),
        col("status", TEXT, default="pending", has_default=True),
        col("total", REAL, default=0.0, has_default=True),
        col("created_at", TEXT),
    ], fks=[
        fk("fk_order_customer", "customer_id", "customers",
           on_delete=CASCADE),
        ForeignKey(name="fk_order_approver", columns=["approver_id"],
                   ref_table="customers", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
        fk("fk_order_address", "address_id", "addresses",
           on_delete=SET_NULL),
    ])

    # 10. 订单明细
    cat.create_table("order_items", [
        col("id", BIGINT, nullable=False, pk=True),
        col("order_id", BIGINT, nullable=False),
        col("product_id", BIGINT, nullable=False),
        col("qty", INTEGER, nullable=False),
        col("unit_price", REAL, nullable=False),
    ], fks=[
        fk("fk_item_order", "order_id", "orders", on_delete=CASCADE),
        fk("fk_item_product", "product_id", "products",
           on_delete=RESTRICT),
    ])

    # 11. 物流（同表两条外键指向 addresses；仓库 SET DEFAULT）
    cat.create_table("shipments", [
        col("id", BIGINT, nullable=False, pk=True),
        col("order_id", BIGINT, nullable=False),
        col("from_address_id", BIGINT),
        col("to_address_id", BIGINT),
        col("warehouse_id", BIGINT, default=1, has_default=True),
        col("tracking_no", TEXT),
        col("shipped_at", TEXT),
    ], fks=[
        fk("fk_ship_order", "order_id", "orders", on_delete=CASCADE),
        ForeignKey(name="fk_ship_from_addr", columns=["from_address_id"],
                   ref_table="addresses", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
        ForeignKey(name="fk_ship_to_addr", columns=["to_address_id"],
                   ref_table="addresses", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
        ForeignKey(name="fk_ship_warehouse", columns=["warehouse_id"],
                   ref_table="warehouses", ref_columns=["id"],
                   on_delete=SET_DEFAULT, on_update=CASCADE),
    ])

    # 12. 物流轨迹
    cat.create_table("shipment_events", [
        col("id", BIGINT, nullable=False, pk=True),
        col("shipment_id", BIGINT, nullable=False),
        col("event", TEXT),
        col("at", TEXT),
    ], fks=[
        fk("fk_event_shipment", "shipment_id", "shipments",
           on_delete=CASCADE),
    ])

    # 13. 发票（事故复现点：客户删了发票置空，留下无主发票）
    cat.create_table("invoices", [
        col("id", BIGINT, nullable=False, pk=True),
        col("order_id", BIGINT, nullable=False),
        col("customer_id", BIGINT),
        col("amount", REAL),
        col("status", TEXT, default="issued", has_default=True),
    ], fks=[
        fk("fk_invoice_order", "order_id", "orders", on_delete=CASCADE),
        ForeignKey(name="fk_invoice_customer", columns=["customer_id"],
                   ref_table="customers", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
    ])

    # 14. 支付记录
    cat.create_table("payments", [
        col("id", BIGINT, nullable=False, pk=True),
        col("order_id", BIGINT, nullable=False),
        col("method", TEXT),
        col("amount", REAL),
        col("paid_at", TEXT),
    ], fks=[
        fk("fk_payment_order", "order_id", "orders", on_delete=CASCADE),
    ])

    # 15. 优惠券（主数据，禁止删被领取过的券）
    cat.create_table("coupons", [
        col("id", BIGINT, nullable=False, pk=True),
        col("code", TEXT, nullable=False),
        col("discount", REAL),
    ])

    # 16. 领券记录
    cat.create_table("coupon_redemptions", [
        col("id", BIGINT, nullable=False, pk=True),
        col("coupon_id", BIGINT, nullable=False),
        col("customer_id", BIGINT, nullable=False),
        col("order_id", BIGINT),
        col("redeemed_at", TEXT),
    ], fks=[
        fk("fk_redeem_coupon", "coupon_id", "coupons",
           on_delete=RESTRICT),
        fk("fk_redeem_customer", "customer_id", "customers",
           on_delete=CASCADE),
        fk("fk_redeem_order", "order_id", "orders", on_delete=SET_NULL),
    ])

    # 17. 客服工单（环 B：订单有主工单；工单引用订单，NO ACTION 延迟检查）
    cat.create_table("support_tickets", [
        col("id", BIGINT, nullable=False, pk=True),
        col("order_id", BIGINT),
        col("customer_id", BIGINT, nullable=False),
        col("title", TEXT),
        col("state", TEXT, default="open", has_default=True),
    ], fks=[
        ForeignKey(name="fk_ticket_order", columns=["order_id"],
                   ref_table="orders", ref_columns=["id"],
                   on_delete=NO_ACTION, on_update=CASCADE),
        fk("fk_ticket_customer", "customer_id", "customers",
           on_delete=CASCADE),
    ])

    # 18. 工单留言
    cat.create_table("ticket_notes", [
        col("id", BIGINT, nullable=False, pk=True),
        col("ticket_id", BIGINT, nullable=False),
        col("author_id", BIGINT),
        col("body", TEXT),
    ], fks=[
        fk("fk_note_ticket", "ticket_id", "support_tickets",
           on_delete=CASCADE),
        ForeignKey(name="fk_note_author", columns=["author_id"],
                   ref_table="customers", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
    ])

    # 19. 客户问题（同表两条外键指向 customers：报单人 + 处理客服）
    cat.create_table("customer_issues", [
        col("id", BIGINT, nullable=False, pk=True),
        col("reporter_id", BIGINT, nullable=False),
        col("handler_id", BIGINT),
        col("product_id", BIGINT),
        col("severity", TEXT, default="normal", has_default=True),
        col("detail", TEXT),
    ], fks=[
        ForeignKey(name="fk_issue_reporter", columns=["reporter_id"],
                   ref_table="customers", ref_columns=["id"],
                   on_delete=CASCADE, on_update=CASCADE),
        ForeignKey(name="fk_issue_handler", columns=["handler_id"],
                   ref_table="customers", ref_columns=["id"],
                   on_delete=SET_NULL, on_update=CASCADE),
        fk("fk_issue_product", "product_id", "products",
           on_delete=SET_NULL),
    ])

    # 20. 后台导航菜单（第三棵自引用树，和业务数据隔离，用来验证多棵树）
    cat.create_table("menus", [
        col("id", BIGINT, nullable=False, pk=True),
        col("label", TEXT, nullable=False),
        col("parent_id", BIGINT),
        col("url", TEXT),
    ], fks=[
        fk("fk_menu_parent", "parent_id", "menus", on_delete=CASCADE),
    ])

    cat.validate()
    return cat
