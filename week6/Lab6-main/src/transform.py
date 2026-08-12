import logging
import re

import pandas as pd

from .config import PROVINCE_MAP

# สถานะที่ถือว่าเป็น "ยอดขายจริง" ตามโจทย์
VALID_STATUS = {"paid", "completed"}

# รูปแบบวันที่ที่พบปนกันใน orders.csv
#   2026-08-01  |  2026/08/02  |  01/08/2026  |  03-Aug-2026
# ลำดับสำคัญ: ต้องลอง %Y ก่อน %d เพื่อไม่ให้ '2026/08/02' ถูกอ่านผิดเป็นวันที่ 2026
DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%b-%Y"]

# คอลัมน์ที่จะเก็บไว้ใน rejects.csv (ค่าดิบก่อนแปลง + เหตุผล)
REJECT_COLUMNS = [
    "order_id", "customer_id", "product_id", "order_date",
    "qty", "unit_price", "discount_pct", "status",
    "reject_stage", "reject_reason",
]


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def to_number(value):
    """แปลงข้อความเป็นตัวเลข รองรับกรณีมี comma คั่นหลักพัน เช่น '1,299.00' -> 1299.0

    ถ้าแปลงไม่ได้จะคืน None เพื่อให้ pd.to_numeric มองเป็น NaN
    และถูกจับเป็น record เสียในขั้นตอนตรวจกฎ
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text)
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_mixed_date(value):
    """แปลงวันที่ที่มีหลายรูปแบบปนกันในคอลัมน์เดียว

    ลองทีละ format ตามลำดับใน DATE_FORMATS ถ้าไม่ตรงสักอันคืน NaT
    ไม่ใช้ pd.to_datetime(errors='coerce') แบบเดาอัตโนมัติ
    เพราะจะทำให้ '01/08/2026' กับ '2026/08/02' ถูกตีความสลับเดือน/วันกันได้
    """
    text = "" if value is None else str(value).strip()
    if text == "":
        return pd.NaT
    for fmt in DATE_FORMATS:
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    return pd.NaT


def standardize_province(value):
    """แปลงชื่อจังหวัดที่เขียนหลากหลายแบบให้เหลือรูปเดียว

    ตัวอย่างที่พบในไฟล์:
        'BKK' / 'bangkok' / 'กรุงเทพฯ'      -> Bangkok
        'chon buri' / 'CHONBURI' / 'ชลบุรี'  -> Chonburi
        'chantaburi' (สะกดตกตัว h) -> Chanthaburi
    ใช้ PROVINCE_MAP ที่อาจารย์เตรียมไว้ใน config.py เป็นตารางเทียบ
    ค่าที่ไม่รู้จักจะคง Title Case ไว้ ค่าว่างจะเป็น 'Unknown'
    """
    text = "" if value is None else str(value).strip()
    if text == "":
        return "Unknown"
    return PROVINCE_MAP.get(text.lower(), text.title())


# ---------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------

def clean_customers(df):
    """ทำความสะอาดตาราง customers"""
    df = df.copy()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    # customer_id ซ้ำ (C004, C009 ปรากฏ 2 ครั้ง) -> เก็บแถวแรก
    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    logging.info("Transform customers: drop duplicate customer_id = %d rows", before - len(df))

    df["name"] = df["name"].astype(str).str.strip().replace("", "Unknown")
    df["province"] = df["province"].apply(standardize_province)

    # email ที่หายไป (C006) เติมเป็น 'Unknown' แทนการทิ้งแถว
    # เพราะลูกค้ายังมีตัวตนจริง แค่ข้อมูลติดต่อไม่ครบ
    df["email"] = df["email"].astype(str).str.strip().str.lower().replace("", "Unknown")

    return df[["customer_id", "name", "province", "email"]].reset_index(drop=True)


# ---------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------

def clean_products(df):
    """ทำความสะอาดตาราง products ที่ flatten มาจาก nested JSON"""
    df = df.copy()

    # เปลี่ยนชื่อคอลัมน์จาก json_normalize ให้สั้นและใช้งานง่าย
    df = df.rename(columns={"category.name": "category", "pricing.price": "price"})

    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip()

    # price ของ P005 เก็บมาเป็นข้อความ "1,299.00" ต้องตัด comma ก่อนแปลงเป็นตัวเลข
    df["price"] = df["price"].apply(to_number)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # category ของ P009 เป็น null ตามโจทย์ให้เติมเป็น "Unknown"
    df["category"] = (df["category"].fillna("").astype(str).str.strip()
                      .replace({"": "Unknown", "None": "Unknown", "nan": "Unknown"}))

    df = df.drop_duplicates(subset=["product_id"], keep="first")
    return df[["product_id", "product_name", "category", "price"]].reset_index(drop=True)


# ---------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------

def clean_orders(df):
    """ทำความสะอาด orders และแยก record ที่ผิดกฎออกมาเป็น rejects

    คืนค่า (orders ที่ผ่านกฎ, rejects ระดับ record)
    """
    df = df.copy()
    for col in ["order_id", "customer_id", "product_id", "status"]:
        df[col] = df[col].astype(str).str.strip()

    # status ตัวพิมพ์ไม่สม่ำเสมอ (PAID / paid) -> lowercase ทั้งหมด
    df["status"] = df["status"].str.lower()

    # order_id ซ้ำ (O0011, O0041, O0101) -> เก็บแถวแรก
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    logging.info("Transform orders: drop duplicate order_id = %d rows", before - len(df))

    # เก็บค่าดิบไว้ก่อน เพื่อให้ rejects.csv แสดงค่าที่ผิดจริง ๆ ไม่ใช่ค่าที่แปลงแล้ว
    df["order_date_parsed"] = df["order_date"].apply(parse_mixed_date)
    df["qty_num"] = pd.to_numeric(df["qty"].apply(to_number), errors="coerce")
    df["price_num"] = pd.to_numeric(df["unit_price"].apply(to_number), errors="coerce")
    df["disc_num"] = pd.to_numeric(df["discount_pct"].apply(to_number), errors="coerce")

    # ---- กฎการ reject ตามโจทย์ ----------------------------------------
    # เรียงตามลำดับความสำคัญ แถวหนึ่งอาจผิดหลายกฎ จะรายงานกฎแรกที่เจอ
    rules = [
        (df["order_date_parsed"].isna(), "invalid order_date"),
        (df["qty_num"].isna() | (df["qty_num"] <= 0), "qty <= 0"),
        (df["price_num"].isna() | (df["price_num"] <= 0), "unit_price <= 0"),
        (df["disc_num"].isna() | (df["disc_num"] < 0) | (df["disc_num"] > 100),
         "discount_pct out of range 0-100"),
    ]

    reason = pd.Series("", index=df.index)
    for mask, text in rules:
        reason = reason.mask((reason == "") & mask, text)

    bad = reason != ""
    rejects = df.loc[bad].copy()
    rejects["reject_stage"] = "orders_rule"
    rejects["reject_reason"] = reason[bad]

    clean = df.loc[~bad].copy()
    logging.info("Transform orders: rejected by rule = %d rows", int(bad.sum()))

    return clean, rejects[REJECT_COLUMNS]


# ---------------------------------------------------------------------
# Transform หลัก
# ---------------------------------------------------------------------

def transform_data(raw):
    """
    TODO 2 -- Transform

    ลำดับการทำงาน:
      1) ทำความสะอาด customers / products
      2) ทำความสะอาด orders + แยก record ที่ผิดกฎ
      3) กรองเฉพาะ status paid / completed
      4) join กับ customers และ products (ถ้าไม่เจอใน master -> reject)
      5) คำนวณ gross / discount / sales_amount

    คืนค่า: (clean_customers, clean_products, sales, rejects)
    """
    customers = clean_customers(raw["customers"])
    products = clean_products(raw["products"])
    orders, rejects_rule = clean_orders(raw["orders"])

    # ---- 3) กรองเฉพาะออร์เดอร์ที่เป็นยอดขายจริง ------------------------
    # หมายเหตุ: pending / cancelled ไม่ใช่ "ข้อมูลผิด" จึงไม่นับเป็น reject
    # แค่ไม่เข้าเงื่อนไขที่จะกลายเป็นยอดขาย เลยกรองทิ้งเฉย ๆ
    is_sale = orders["status"].isin(VALID_STATUS)
    logging.info(
        "Transform orders: filtered out status not in %s = %d rows",
        sorted(VALID_STATUS), int((~is_sale).sum())
    )
    sales = orders.loc[is_sale].copy()

    # ---- 4) join กับ master data --------------------------------------
    # join แบบเต็มตามโจทย์: orders + customers + products
    # ดึงคุณลักษณะของลูกค้า (name, province, email) และสินค้า (product_name,
    # category, price) เข้ามาด้วย แม้ fact_sales จะเก็บแค่ key
    # เพราะทำให้ตรวจสอบผลลัพธ์ระหว่างทางได้ และรองรับการต่อยอดในอนาคต
    #
    # ใช้ left join + indicator เพื่อให้แถวที่หา master ไม่เจอยังอยู่ครบ
    # แล้วค่อยคัดออกไปเป็น reject (ถ้าใช้ inner join แถวพวกนี้จะหายเงียบ)
    sales = sales.merge(
        customers.rename(columns={"name": "customer_name"}),
        on="customer_id", how="left", validate="many_to_one", indicator="_customer_merge",
    )
    sales = sales.merge(
        products.rename(columns={"price": "product_price"}),
        on="product_id", how="left", validate="many_to_one", indicator="_product_merge",
    )

    missing_customer = sales["_customer_merge"].eq("left_only")
    missing_product = sales["_product_merge"].eq("left_only")
    orphan = missing_customer | missing_product

    rejects_master = sales.loc[orphan].copy()
    rejects_master["reject_stage"] = "merge_master"
    rejects_master["reject_reason"] = [
        "customer_id not in master" if c else "product_id not in master"
        for c in missing_customer[orphan]
    ]
    logging.info("Transform merge: rejected unknown customer/product = %d rows", int(orphan.sum()))

    sales = sales.loc[~orphan].copy()

    # ---- 5) คำนวณยอดเงิน ----------------------------------------------
    sales["gross_amount"] = (sales["qty_num"] * sales["price_num"]).round(2)
    sales["discount_amount"] = (sales["gross_amount"] * sales["disc_num"] / 100).round(2)
    sales["sales_amount"] = (sales["gross_amount"] - sales["discount_amount"]).round(2)

    # จัดรูปคอลัมน์ให้ตรงกับ fact_sales ที่โจทย์กำหนด
    sales = sales.rename(columns={
        "order_date_parsed": "order_date_dt",
        "qty_num": "qty_clean",
        "price_num": "unit_price_clean",
        "disc_num": "discount_pct_clean",
    })
    sales["order_date"] = sales["order_date_dt"].dt.strftime("%Y-%m-%d")
    sales["qty"] = sales["qty_clean"].astype(int)
    sales["unit_price"] = sales["unit_price_clean"].round(2)
    sales["discount_pct"] = sales["discount_pct_clean"].round(2)

    sales = sales[[
        "order_id", "customer_id", "product_id", "order_date",
        "qty", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "sales_amount",
    ]].reset_index(drop=True)

    # ---- รวม rejects ทั้งสองระดับไว้ในไฟล์เดียว -------------------------
    rejects = pd.concat(
        [rejects_rule, rejects_master.reindex(columns=REJECT_COLUMNS)],
        ignore_index=True,
    )
    if rejects.empty:
        rejects = pd.DataFrame(columns=REJECT_COLUMNS)

    logging.info(
        "Transform done: sales=%d rows, total=%.2f, rejects=%d rows",
        len(sales), sales["sales_amount"].sum(), len(rejects)
    )

    return customers, products, sales, rejects
