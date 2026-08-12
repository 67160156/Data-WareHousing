import json
import logging
import sqlite3

import pandas as pd

from .config import RAW_DIR, SOURCE_DB


def extract_data():
    """
    TODO 1 -- Extract

    ดึงข้อมูลดิบจาก 4 แหล่งที่ต่างรูปแบบกัน แล้วคืนเป็น dict ของ DataFrame

      customers.csv  -> CSV ธรรมดา
      orders.csv     -> CSV ธรรมดา
      products.json  -> JSON ที่มี object ซ้อนชั้น (category.name, pricing.price)
      store.db       -> ตาราง stores ใน SQLite

    หลักการสำคัญของขั้น Extract:
    "อ่านมาให้ครบก่อน อย่าเพิ่งทำความสะอาด"
    จึงอ่านทุกคอลัมน์เป็น str (dtype=str) เพื่อไม่ให้ pandas เดาชนิดข้อมูลผิด
    เช่น เดา qty ที่มีค่าติดลบเป็น int แล้วทำให้ตรวจจับ record เสียยากขึ้น
    การแปลงชนิดข้อมูลทั้งหมดจะไปทำในขั้น Transform
    """
    # ---- customers.csv ------------------------------------------------
    customers = pd.read_csv(
        RAW_DIR / "customers.csv",
        dtype=str,
        encoding="utf-8-sig",  # ตัด BOM ที่อาจติดมากับไฟล์ที่ export จาก Excel
        keep_default_na=False,  # ให้ค่าว่างเป็น "" แทน NaN จะได้จัดการเองในขั้น Transform
    )

    # ---- orders.csv ---------------------------------------------------
    orders = pd.read_csv(
        RAW_DIR / "orders.csv",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )

    # ---- products.json (nested) ---------------------------------------
    # โครงสร้างไฟล์:
    #   {"product_id": ..., "category": {"name": ...}, "pricing": {"price": ...}}
    # json_normalize จะแบน object ซ้อนชั้นให้เป็นคอลัมน์ "category.name", "pricing.price"
    with open(RAW_DIR / "products.json", encoding="utf-8") as f:
        products_raw = json.load(f)
    products = pd.json_normalize(products_raw)

    # ---- stores จาก SQLite --------------------------------------------
    # ใช้ context manager เพื่อให้ปิด connection ให้แน่นอนแม้เกิด error
    with sqlite3.connect(SOURCE_DB) as conn:
        stores = pd.read_sql_query("SELECT * FROM stores;", conn)

    raw = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "stores": stores,
    }

    # ---- Checkpoint หลัง Extract ---------------------------------------
    # log shape ของทุก DataFrame ไว้เป็นหลักฐานว่าอ่านข้อมูลมาครบจริง
    for name, df in raw.items():
        logging.info(
            "Extract %-10s rows=%-5d cols=%-3d | %s",
            name, df.shape[0], df.shape[1], list(df.columns)
        )

    return raw
