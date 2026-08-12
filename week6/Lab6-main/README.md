# Lab 6 — Mini ETL Pipeline with Python

Week 06 · Data Warehousing Concepts and Design
67160156 — ธมนวรรณ ศรีสวัสดิ์

CampusMart — รวมข้อมูลจาก 4 แหล่งเข้าสู่ SQLite Data Warehouse
`Extract → Transform → Load → Validate`

## วิธีรัน

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.main
```

## โครงสร้าง

```
src/
  config.py      ค่าคงที่ + PROVINCE_MAP
  extract.py     TODO 1 — อ่าน CSV / nested JSON / SQLite
  transform.py   TODO 2 — clean, reject, merge, คำนวณยอด
  load.py        TODO 3 — โหลดเข้า warehouse แบบ idempotent
  validate.py    TODO 4 — เทียบ source vs warehouse
  main.py        orchestrator
data/
  raw/           customers.csv, orders.csv, products.json
  source_db/     store.db
  warehouse/     warehouse.db  (สร้างจากการรัน)
output/
  rejects.csv       record ที่ไม่ผ่านกฎ
  validation.json   ผลตรวจสอบ PASS/FAIL
logs/etl.log
REPORT.md
```

## Warehouse Schema

```
dim_customer          dim_product              fact_sales
  customer_id (PK)      product_id (PK)          order_id (PK)
  name                  product_name             customer_id (FK)
  province              category                 product_id (FK)
  email                 price                    order_date
                                                 qty
                                                 unit_price
                                                 discount_pct
                                                 sales_amount
```

## ผลลัพธ์

| | |
|---|---|
| แถวดิบ | 183 |
| order_id ซ้ำ | 3 |
| reject (ผิดกฎ) | 4 |
| กรองออก (ไม่ใช่ paid/completed) | 76 |
| **fact_sales** | **100** |
| ยอดขายรวม | 192,074.63 |
| Validation | **PASS** |
| รันซ้ำครั้งที่ 2 | fact_sales = 100 (ไม่เพิ่ม) |

รายละเอียดปัญหาคุณภาพข้อมูลและวิธีแก้ทั้งหมดอยู่ใน [REPORT.md](REPORT.md)
