"""

ETL Pipeline : Retail Store Logs -> Star Schema Data Warehouse (SQLite)

Assignment : Data Warehousing - Week 04
Source     : retail_logs.csv (ข้อมูลการขายสินค้าตามสาขา)
Output     : retail_warehouse.db (SQLite)

Star Schema ที่ออกแบบ:
    Fact_Sales            (Fact Table)
    ├── Dim_Location       (สาขา / จังหวัด / ภูมิภาค)
    ├── Dim_Product        (สินค้า / หมวดหมู่)
    └── Dim_Date            (วันเดือนปีของการขาย)

ขั้นตอนหลัก 3 ขั้น: Extract -> Transform -> Load

"""

import sqlite3
import pandas as pd
import numpy as np

SOURCE_CSV = "retail_logs.csv"
DB_FILE = "retail_warehouse.db"



# 1) EXTRACT

def extract(path: str) -> pd.DataFrame:
    """อ่านไฟล์ CSV ต้นทางเข้ามาเป็น DataFrame"""
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"[EXTRACT] อ่านข้อมูลทั้งหมด {len(df)} แถว, {len(df.columns)} คอลัมน์")
    return df



# 2) TRANSFORM

def clean_text(series: pd.Series) -> pd.Series:
    """ตัดช่องว่างหน้า-หลัง และทำให้เป็น Title Case ให้เขียนเหมือนกันทุกแถว"""
    return series.astype(str).str.strip().str.title()


def parse_mixed_dates(series: pd.Series) -> pd.Series:
    """
    วันที่ในไฟล์ต้นทางมี 3 รูปแบบปนกัน:
        2026-03-15   (YYYY-MM-DD)
        14-May-2026  (DD-Mon-YYYY)
        17/05/2026   (DD/MM/YYYY)
    จึงต้องลองแปลงทีละรูปแบบ แล้วรวมผลลัพธ์เข้าด้วยกัน
    """
    formats = ["%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"]
    result = pd.Series(pd.NaT, index=series.index)
    remaining = series.copy()

    for fmt in formats:
        parsed = pd.to_datetime(remaining, format=fmt, errors="coerce")
        result = result.fillna(parsed)

    return result


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- 2.1 ลบแถวข้อมูลซ้ำซ้อน (duplicate Sale_ID ที่ข้อมูลเหมือนกันทุก field) ---
    before = len(df)
    df = df.drop_duplicates(subset=["Sale_ID"], keep="first")
    print(f"[TRANSFORM] ลบข้อมูลซ้ำ {before - len(df)} แถว")

    # --- 2.2 ทำความสะอาดข้อความ (Trim + Title Case) ให้เขียนเป็นมาตรฐานเดียวกัน ---
    for col in ["Branch", "Province", "Region", "Product_Name", "Category"]:
        df[col] = clean_text(df[col])

    # Region บางแถวเป็นค่าว่าง (NaN) -> เติมโดย map จาก Province ที่มีอยู่แล้ว
    province_to_region = (
        df.dropna(subset=["Region"])
        .drop_duplicates(subset=["Province"])
        .set_index("Province")["Region"]
        .to_dict()
    )
    df["Region"] = df["Region"].fillna(df["Province"].map(province_to_region))

    # --- 2.3 เติมค่า Discount ที่ขาดหายด้วย 0 (สมมติว่าไม่มีส่วนลด) ---
    df["Discount_Percent"] = df["Discount_Percent"].fillna(0)

    # --- 2.4 แปลงวันที่ให้เป็นรูปแบบเดียวกัน ---
    df["Sale_Date"] = parse_mixed_dates(df["Sale_Date"])

    # --- 2.5 คำนวณยอดขายสุทธิต่อรายการ (Measure หลักของ Fact table) ---
    gross = df["Quantity"] * df["Unit_Price"]
    df["Total_Amount"] = (gross * (1 - df["Discount_Percent"] / 100)).round(2)

    print(f"[TRANSFORM] ข้อมูลหลังทำความสะอาด: {len(df)} แถว")
    return df



# 3) BUILD STAR SCHEMA (Dimension + Fact tables)

def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["Store_Code", "Branch", "Province", "Region"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.insert(0, "location_key", dim.index + 1)
    return dim


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["Product_Name", "Category"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.insert(0, "product_key", dim.index + 1)
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = df["Sale_Date"].drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame({"full_date": dates})
    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.strftime("%B")
    dim["day"] = dim["full_date"].dt.day
    dim["day_name"] = dim["full_date"].dt.strftime("%A")
    dim["is_weekend"] = dim["full_date"].dt.dayofweek >= 5
    return dim[
        ["date_key", "full_date", "year", "quarter", "month",
         "month_name", "day", "day_name", "is_weekend"]
    ]


def build_fact_sales(
    df: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    fact = df.merge(
        dim_location, on=["Store_Code", "Branch", "Province", "Region"], how="left"
    )
    fact = fact.merge(
        dim_product, on=["Product_Name", "Category"], how="left"
    )
    fact["date_key"] = fact["Sale_Date"].dt.strftime("%Y%m%d").astype(int)

    fact = fact[
        [
            "Sale_ID", "date_key", "location_key", "product_key",
            "Quantity", "Unit_Price", "Discount_Percent", "Total_Amount",
        ]
    ].rename(
        columns={
            "Sale_ID": "sale_id",
            "Quantity": "quantity",
            "Unit_Price": "unit_price",
            "Discount_Percent": "discount_percent",
            "Total_Amount": "total_amount",
        }
    )
    fact.insert(0, "sale_key", range(1, len(fact) + 1))
    return fact



# 4) LOAD

def load(
    db_path: str,
    dim_location: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_sales: pd.DataFrame,
):
    conn = sqlite3.connect(db_path)
    try:
        dim_location.to_sql("Dim_Location", conn, if_exists="replace", index=False)
        dim_product.to_sql("Dim_Product", conn, if_exists="replace", index=False)
        dim_date.to_sql("Dim_Date", conn, if_exists="replace", index=False)
        fact_sales.to_sql("Fact_Sales", conn, if_exists="replace", index=False)

        # primary key ให้แต่ละตาราง (สร้างเพิ่มหลังโหลดข้อมูล เพื่อความง่ายของ to_sql)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS pk_location ON Dim_Location(location_key)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS pk_product ON Dim_Product(product_key)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS pk_date ON Dim_Date(date_key)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS pk_sale ON Fact_Sales(sale_key)")
        conn.commit()
        print(f"[LOAD] บันทึกลงฐานข้อมูล {db_path} เรียบร้อย")
        print(
            f"        Dim_Location={len(dim_location)}, "
            f"Dim_Product={len(dim_product)}, "
            f"Dim_Date={len(dim_date)}, "
            f"Fact_Sales={len(fact_sales)} แถว"
        )
    finally:
        conn.close()



# MAIN

def main():
    raw_df = extract(SOURCE_CSV)
    clean_df = transform(raw_df)

    dim_location = build_dim_location(clean_df)
    dim_product = build_dim_product(clean_df)
    dim_date = build_dim_date(clean_df)
    fact_sales = build_fact_sales(clean_df, dim_location, dim_product, dim_date)

    load(DB_FILE, dim_location, dim_product, dim_date, fact_sales)

    # ตรวจสอบผลลัพธ์เบื้องต้น
    print("\n--- ตัวอย่าง Dim_Location ---")
    print(dim_location.head())
    print("\n--- ตัวอย่าง Fact_Sales ---")
    print(fact_sales.head())


if __name__ == "__main__":
    main()
