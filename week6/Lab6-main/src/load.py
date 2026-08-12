import logging
import sqlite3

from .config import WAREHOUSE_DB

# ---------------------------------------------------------------------
# DDL ของ Warehouse
#
# หัวใจของข้อนี้คือ "รันซ้ำแล้วข้อมูลต้องไม่บาน"
# จึงใช้ CREATE TABLE IF NOT EXISTS (ไม่ DROP ทิ้งทุกรอบ)
# ร่วมกับ PRIMARY KEY / UNIQUE บน business key ของแต่ละตาราง
# ---------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id TEXT PRIMARY KEY,
    name        TEXT,
    province    TEXT,
    email       TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    price        REAL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL REFERENCES dim_customer(customer_id),
    product_id   TEXT NOT NULL REFERENCES dim_product(product_id),
    order_date   TEXT NOT NULL,
    qty          INTEGER NOT NULL CHECK(qty > 0),
    unit_price   REAL    NOT NULL CHECK(unit_price > 0),
    discount_pct REAL    NOT NULL CHECK(discount_pct BETWEEN 0 AND 100),
    sales_amount REAL    NOT NULL CHECK(sales_amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(order_date);
"""

# dimension: ใช้ UPSERT -- ถ้ามี key อยู่แล้วให้อัปเดตค่าคุณลักษณะให้เป็นข้อมูลล่าสุด
UPSERT_CUSTOMER = """
INSERT INTO dim_customer (customer_id, name, province, email)
VALUES (?, ?, ?, ?)
ON CONFLICT(customer_id) DO UPDATE SET
    name     = excluded.name,
    province = excluded.province,
    email    = excluded.email;
"""

UPSERT_PRODUCT = """
INSERT INTO dim_product (product_id, product_name, category, price)
VALUES (?, ?, ?, ?)
ON CONFLICT(product_id) DO UPDATE SET
    product_name = excluded.product_name,
    category     = excluded.category,
    price        = excluded.price;
"""

# fact: ใช้ INSERT OR IGNORE -- order_id ที่โหลดไปแล้วจะถูกข้าม ไม่เกิดแถวซ้ำ
# นี่คือกลไกที่ทำให้ pipeline เป็น idempotent ตาม requirement ของโจทย์
INSERT_FACT = """
INSERT OR IGNORE INTO fact_sales
    (order_id, customer_id, product_id, order_date, qty, unit_price, discount_pct, sales_amount)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""


def load_data(customers, products, sales):
    """
    TODO 3 -- Load

    โหลด 3 ตารางเข้า SQLite warehouse:
        dim_customer / dim_product / fact_sales

    Requirement:
      - customer_id, product_id, order_id ต้อง UNIQUE
      - รัน pipeline สองครั้ง จำนวนแถวใน fact_sales ต้องเท่าเดิม
    """
    # config.py ไม่ได้สร้างโฟลเดอร์ data/warehouse ไว้ ต้องสร้างเองก่อนเปิด connection
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(WAREHOUSE_DB) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(SCHEMA_SQL)

        before = conn.execute("SELECT COUNT(*) FROM fact_sales;").fetchone()[0]

        # ---- dimension ก่อน เพราะ fact มี FK ชี้มาหา ----------------------
        conn.executemany(
            UPSERT_CUSTOMER,
            customers[["customer_id", "name", "province", "email"]].itertuples(index=False, name=None),
        )
        conn.executemany(
            UPSERT_PRODUCT,
            products[["product_id", "product_name", "category", "price"]].itertuples(index=False, name=None),
        )

        # ---- fact ---------------------------------------------------------
        cur = conn.executemany(
            INSERT_FACT,
            sales[[
                "order_id", "customer_id", "product_id", "order_date",
                "qty", "unit_price", "discount_pct", "sales_amount",
            ]].itertuples(index=False, name=None),
        )
        inserted = cur.rowcount

        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM fact_sales;").fetchone()[0]

    logging.info(
        "Load: dim_customer=%d, dim_product=%d | fact_sales %d -> %d (new=%d, skipped as duplicate=%d)",
        len(customers), len(products), before, after, after - before, len(sales) - (after - before),
    )
    logging.info("Load: warehouse = %s", WAREHOUSE_DB)
    return {"fact_rows_before": before, "fact_rows_after": after, "rows_attempted": inserted}
