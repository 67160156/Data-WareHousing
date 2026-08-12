import logging
import sqlite3

from .config import WAREHOUSE_DB

# ยอมให้ยอดเงินต่างกันได้ไม่เกิน 1 สตางค์ (ผลจากการปัดทศนิยม)
TOLERANCE = 0.01


def validate_data(source_sales):
    """
    TODO 4 -- Validate

    เทียบตัวเลขระหว่าง "ข้อมูลที่ผ่าน Transform" กับ "ข้อมูลที่อยู่ใน Warehouse จริง"
    ถ้าสองฝั่งไม่ตรงกัน แปลว่ามีข้อมูลตกหล่นหรือซ้ำระหว่างขั้น Load

    เกณฑ์ PASS ต้องผ่านครบ 3 ข้อ:
      1) จำนวนแถวเท่ากัน
      2) ไม่มี order_id ซ้ำใน fact_sales
      3) ยอดขายรวมสองฝั่งตรงกัน (ต่างได้ไม่เกิน 0.01)
    """
    source_valid_rows = int(len(source_sales))
    source_total_sales = round(float(source_sales["sales_amount"].sum()), 2)

    with sqlite3.connect(WAREHOUSE_DB) as conn:
        warehouse_rows = int(conn.execute("SELECT COUNT(*) FROM fact_sales;").fetchone()[0])

        # นับ order_id ที่ปรากฏมากกว่า 1 ครั้ง -- ถ้า PRIMARY KEY ทำงานถูกต้องต้องได้ 0
        duplicate_order_ids = int(conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT order_id FROM fact_sales
                GROUP BY order_id HAVING COUNT(*) > 1
            );
        """).fetchone()[0])

        warehouse_total_sales = round(
            float(conn.execute(
                "SELECT COALESCE(SUM(sales_amount), 0) FROM fact_sales;"
            ).fetchone()[0]), 2
        )

    checks = {
        "row_count_match": source_valid_rows == warehouse_rows,
        "no_duplicate_order_id": duplicate_order_ids == 0,
        "total_sales_match": abs(source_total_sales - warehouse_total_sales) <= TOLERANCE,
    }
    status = "PASS" if all(checks.values()) else "FAIL"

    result = {
        "source_valid_rows": source_valid_rows,
        "warehouse_rows": warehouse_rows,
        "duplicate_order_ids": duplicate_order_ids,
        "source_total_sales": source_total_sales,
        "warehouse_total_sales": warehouse_total_sales,
        "status": status,
    }

    logging.info("Validate: %s | checks=%s", status, checks)
    if status == "FAIL":
        failed = [name for name, ok in checks.items() if not ok]
        logging.error("Validation FAILED on: %s", ", ".join(failed))

    return result
