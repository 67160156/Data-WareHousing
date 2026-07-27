# -*- coding: utf-8 -*-

R"""Week 04 Workshop - ETL Pipeline สำหรับ E-commerce Data Warehouse
================================================================
Dataset : raw_ecommerce_data.csv
Output  : warehouse.db (SQLite)
Schema  : Star Schema -> 1 Fact (fact_sales) + 3 Dimensions
                         (dim_customer, dim_product, dim_date)

ลำดับการทำงานตามที่เรียนในคาบ:
    Phase 1  EXTRACT   อ่าน flat file + สำรวจความยุ่งเหยิงของข้อมูล
    Phase 2A TRANSFORM สร้าง Dimension Tables + Surrogate Keys
    Phase 2B TRANSFORM สร้าง Fact Table (map Foreign Keys)
    Phase 3A LOAD      สร้าง connection + schema (PRIMARY KEY)
    Phase 3B LOAD      บังคับใช้ความสัมพันธ์ (FOREIGN KEY)
    Phase 3C LOAD      ผลักข้อมูลลง warehouse (idempotent)
    Verify             ทดสอบคิวรีจาก warehouse
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path.cwd()
CSV_PATH = BASE_DIR / "raw_ecommerce_data.csv"
DB_PATH = BASE_DIR / "warehouse.db"

# วันที่ในไฟล์ดิบมีปนกัน 3 รูปแบบ
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y"]

# คำย่อที่ต้องคงตัวพิมพ์ใหญ่ไว้ (str.title() ธรรมดาจะทำ 'USB-C Hub' เพี้ยนเป็น 'Usb-C Hub')
ACRONYMS = {"USB", "HD", "SSD", "TB", "GB", "PC", "TV", "LED", "HDMI"}



# Helper functions สำหรับการทำความสะอาดข้อมูล

def clean_text(value: object, default: str = "Unknown") -> str:
    """ตัดช่องว่างหัว-ท้าย ยุบช่องว่างซ้ำ และปรับตัวพิมพ์ให้เป็นมาตรฐานเดียวกัน

    แก้ปัญหา ' Emma Brown ', 'PETER KIM', 'headphone stand' ให้กลายเป็นค่าเดียวกัน
    ถ้าไม่ทำขั้นตอนนี้ก่อน drop_duplicates() จะได้ dimension ที่ซ้ำกันเต็มไปหมด

    ใช้ title case แบบรู้จักคำย่อ เพื่อไม่ให้ 'Portable SSD 1TB' เพี้ยนเป็น 'Portable Ssd 1Tb'
    """
    if pd.isna(value) or str(value).strip() == "":
        return default

    words = []
    for word in str(value).split():
        parts = []
        for part in word.split("-"):
            if part.upper() in ACRONYMS or any(ch.isdigit() for ch in part):
                parts.append(part.upper())
            else:
                parts.append(part.capitalize())
        words.append("-".join(parts))
    return " ".join(words)


def clean_email(value: object) -> str:
    """อีเมลใช้ตัวพิมพ์เล็กเสมอ ('JOHN@EMAIL.COM' กับ 'john@email.com' คือคนเดียวกัน)"""
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def clean_number(value: object) -> float | None:
    """ถอดสัญลักษณ์สกุลเงินและ comma ออก: '฿25,160.00' -> 25160.0"""
    if pd.isna(value) or str(value).strip() == "":
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    return float(cleaned) if cleaned not in ("", "-", ".") else None


def parse_mixed_date(value: object) -> pd.Timestamp:
    """ลอง parse ทีละรูปแบบจนกว่าจะสำเร็จ

    ใช้ format ที่ระบุชัดเจนแทนการเดาอัตโนมัติ เพื่อไม่ให้ 04/01/2026
    ถูกตีความสลับเป็นเดือนเมษายน (ไฟล์นี้เป็น dd/mm/yyyy)
    """
    text = "" if pd.isna(value) else str(value).strip()
    for fmt in DATE_FORMATS:
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    return pd.NaT



# Phase 1: EXTRACT

def extract() -> pd.DataFrame:
    # อ่านทุกคอลัมน์เป็น string ก่อน เพื่อไม่ให้ pandas เดา dtype ผิดตั้งแต่ต้นทาง
    # encoding='utf-8-sig' เพื่อตัด BOM ที่ติดมากับคอลัมน์แรก
    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]

    print("=" * 70)
    print("PHASE 1: EXTRACT")
    print("=" * 70)
    print(f"  จำนวนแถวดิบ            : {len(df):,}")
    print(f"  Order_ID ซ้ำ            : {df.duplicated('order_id').sum():,}")
    print(f"  Amount ที่ขาดหาย (NaN) : {df['amount'].isna().sum():,}")
    print(f"  Customer_Name ที่ว่าง  : {df['customer_name'].isna().sum():,}")
    print(f"  Email ที่ว่าง          : {df['email'].isna().sum():,}")
    print(f"  Category ที่ว่าง       : {df['category'].isna().sum():,}")
    return df



# Phase 2: TRANSFORM

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """ทำความสะอาดข้อมูลดิบก่อนแตกออกเป็น dimension / fact"""
    # 2.0 ลบธุรกรรมที่ซ้ำ (Order_ID เดียวกันถูกบันทึกซ้ำ) เก็บแถวแรกไว้
    df = df.drop_duplicates(subset=["order_id"], keep="first").copy()

    df["customer_name"] = df["customer_name"].apply(clean_text)
    df["email"] = df["email"].apply(clean_email)
    df["product"] = df["product"].apply(clean_text)
    df["category"] = df["category"].apply(clean_text)

    # เติมข้อมูลลูกค้าที่ขาดหายแบบไขว้กัน แทนที่จะโยนทิ้งหรือสร้างลูกค้าปลอม
    #   - แถวที่มีชื่อแต่ไม่มีอีเมล  -> ยืมอีเมลจากแถวอื่นที่ชื่อเดียวกัน
    #   - แถวที่มีอีเมลแต่ไม่มีชื่อ -> ยืมชื่อจากแถวอื่นที่อีเมลเดียวกัน
    known = df[df["email"].ne("") & df["customer_name"].ne("Unknown")]
    name_to_email = known.drop_duplicates("customer_name").set_index("customer_name")["email"]
    email_to_name = known.drop_duplicates("email").set_index("email")["customer_name"]

    missing_email = df["email"].eq("")
    df.loc[missing_email, "email"] = df.loc[missing_email, "customer_name"].map(name_to_email).fillna("")

    missing_name = df["customer_name"].eq("Unknown")
    df.loc[missing_name, "customer_name"] = (
        df.loc[missing_name, "email"].map(email_to_name).fillna("Unknown")
    )

    # ถ้ายังไม่มีอีเมลจริง ๆ ค่อยสร้าง placeholder เพื่อไม่ให้ FK พัง
    still_missing = df["email"].eq("")
    df.loc[still_missing, "email"] = (
        df.loc[still_missing, "customer_name"]
        .str.lower()
        .str.replace(r"[^a-z0-9]+", ".", regex=True)
        .str.strip(".")
        + "@unknown.local"
    )

    # ตัวเลขและวันที่
    df["order_date"] = df["order_date"].apply(parse_mixed_date)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = df["unit_price"].apply(clean_number)
    df["amount"] = df["amount"].apply(clean_number)

    # Amount ที่หายไป คำนวณกลับจาก quantity * unit_price ได้ ไม่ต้องทิ้งแถว
    df["amount"] = df["amount"].fillna(df["quantity"] * df["unit_price"])

    before = len(df)
    df = df.dropna(subset=["order_date", "unit_price", "amount"])
    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)].copy()

    print()
    print("=" * 70)
    print("PHASE 2: TRANSFORM")
    print("=" * 70)
    print(f"  แถวหลังทำความสะอาด     : {len(df):,} (ตัดทิ้ง {before - len(df):,} แถวที่กู้ไม่ได้)")
    return df


def build_dimensions(df: pd.DataFrame):
    """Phase 2A - แยกคอลัมน์ที่เป็นบริบท (Context) ออกมาเป็น Dimension + Surrogate Key"""

    # --- dim_customer : ใช้ email เป็นตัวระบุตัวตนที่แท้จริง ---
    dim_customer = (
        df[["customer_name", "email"]]
        .drop_duplicates("email")
        .sort_values(["customer_name", "email"])
        .reset_index(drop=True)
    )
    dim_customer.insert(0, "customer_id", range(1, len(dim_customer) + 1))

    # --- dim_product : สินค้า 1 ชื่อ = 1 แถว (เลือก category ที่ไม่ใช่ Unknown ก่อน) ---
    products = df[["product", "category"]].copy()
    products["_is_unknown"] = products["category"].eq("Unknown")
    dim_product = (
        products.sort_values(["product", "_is_unknown"])
        .drop_duplicates("product")
        .drop(columns="_is_unknown")
        .rename(columns={"product": "product_name"})
        .sort_values(["category", "product_name"])
        .reset_index(drop=True)
    )
    dim_product.insert(0, "product_id", range(1, len(dim_product) + 1))

    # --- dim_date : ใช้ surrogate key แบบ YYYYMMDD ซึ่งอ่านออกและเรียงลำดับได้ ---
    dim_date = (
        df[["order_date"]]
        .drop_duplicates()
        .sort_values("order_date")
        .reset_index(drop=True)
        .rename(columns={"order_date": "full_date"})
    )
    dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["quarter"] = "Q" + dim_date["full_date"].dt.quarter.astype(str)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["full_date"] = dim_date["full_date"].dt.strftime("%Y-%m-%d")
    dim_date = dim_date[["date_id", "full_date", "day", "month", "month_name", "quarter", "year"]]

    print(f"  dim_customer           : {len(dim_customer):,} แถว")
    print(f"  dim_product            : {len(dim_product):,} แถว")
    print(f"  dim_date               : {len(dim_date):,} แถว")
    return dim_customer, dim_product, dim_date


def build_fact(df, dim_customer, dim_product, dim_date) -> pd.DataFrame:
    """Phase 2B - นำ Surrogate Key กลับไป map เพื่อแทนที่ข้อความด้วย Foreign Key"""
    mapped = (
        df.merge(dim_customer[["customer_id", "email"]], on="email", how="left", validate="many_to_one")
        .merge(
            dim_product[["product_id", "product_name"]],
            left_on="product",
            right_on="product_name",
            how="left",
            validate="many_to_one",
        )
        .assign(date_key=lambda x: x["order_date"].dt.strftime("%Y%m%d").astype(int))
    )

    fact_sales = mapped[
        ["order_id", "customer_id", "product_id", "date_key", "quantity", "unit_price", "amount"]
    ].rename(columns={"order_id": "transaction_id", "date_key": "date_id", "amount": "total_amount"})

    # ลบคอลัมน์ข้อความทิ้ง เหลือไว้แค่ FK + ตัวเลข = หัวใจของ Fact Table
    fact_sales[["customer_id", "product_id", "date_id"]] = fact_sales[
        ["customer_id", "product_id", "date_id"]
    ].astype(int)
    fact_sales["total_amount"] = fact_sales["total_amount"].round(2)

    assert fact_sales.notna().all().all(), "พบ Foreign Key ที่ map ไม่ได้"
    print(f"  fact_sales             : {len(fact_sales):,} แถว")
    return fact_sales



# Phase 3: LOAD

SCHEMA_SQL = """
-- Phase 3B: บังคับใช้ความสัมพันธ์แบบ Star Schema
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    customer_name TEXT    NOT NULL,
    email         TEXT    NOT NULL UNIQUE
);

CREATE TABLE dim_product (
    product_id   INTEGER PRIMARY KEY,
    product_name TEXT    NOT NULL UNIQUE,
    category     TEXT    NOT NULL
);

CREATE TABLE dim_date (
    date_id    INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date  TEXT    NOT NULL UNIQUE,
    day        INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    month_name TEXT    NOT NULL,
    quarter    TEXT    NOT NULL,
    year       INTEGER NOT NULL
);

CREATE TABLE fact_sales (
    transaction_id TEXT    PRIMARY KEY,
    customer_id    INTEGER NOT NULL,
    product_id     INTEGER NOT NULL,
    date_id        INTEGER NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     REAL    NOT NULL CHECK (unit_price >= 0),
    total_amount   REAL    NOT NULL CHECK (total_amount >= 0),
    FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
    FOREIGN KEY (product_id)  REFERENCES dim_product  (product_id),
    FOREIGN KEY (date_id)     REFERENCES dim_date     (date_id)
);
"""


def load(dim_customer, dim_product, dim_date, fact_sales) -> None:
    print()
    print("=" * 70)
    print("PHASE 3: LOAD")
    print("=" * 70)

    with sqlite3.connect(DB_PATH) as conn:
        # Phase 3A + 3B: สร้าง schema ใหม่ทุกครั้ง -> รัน pipeline ซ้ำได้ผลเหมือนเดิม (Idempotency)
        conn.executescript(SCHEMA_SQL)

        # Phase 3C: ผลักข้อมูลลงตาราง (โหลด dimension ก่อนเสมอ ไม่งั้น FK จะพัง)
        dim_customer.to_sql("dim_customer", conn, if_exists="append", index=False)
        dim_product.to_sql("dim_product", conn, if_exists="append", index=False)
        dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
        fact_sales.to_sql("fact_sales", conn, if_exists="append", index=False)
        conn.commit()

        violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
        print(f"  เขียนลงไฟล์            : {DB_PATH.name}")
        print(f"  ตรวจสอบ Foreign Key    : {'ผ่าน (ไม่พบข้อผิดพลาด)' if not violations else violations}")



# Verification

QUERIES = {
    "Top 5 ลูกค้าที่ใช้จ่ายสูงสุด": """
        SELECT c.customer_name,
               ROUND(SUM(f.total_amount), 2) AS total_spend
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY total_spend DESC
        LIMIT 5;
    """,
    "ยอดขายรายหมวดสินค้า": """
        SELECT p.category,
               SUM(f.quantity)                 AS units_sold,
               ROUND(SUM(f.total_amount), 2)   AS revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.category
        ORDER BY revenue DESC;
    """,
    "ยอดขายรายเดือน": """
        SELECT d.year, d.month, d.month_name,
               ROUND(SUM(f.total_amount), 2) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month;
    """,
}


def verify() -> None:
    print()
    print("=" * 70)
    print("VERIFICATION: ทดสอบคิวรีจาก Warehouse")
    print("=" * 70)
    with sqlite3.connect(DB_PATH) as conn:
        for title, sql in QUERIES.items():
            print(f"\n>> {title}")
            print(pd.read_sql_query(sql, conn).to_string(index=False))


if __name__ == "__main__":
    raw = extract()
    cleaned = clean(raw)
    dims = build_dimensions(cleaned)
    fact = build_fact(cleaned, *dims)
    load(*dims, fact)
    verify()
    print("\nETL Pipeline ran successfully!")