# Python Data Pipeline Engineering — Lab Assignment

ETL pipeline for the **Omnichannel Retail Data Warehouse** lab. Loads
`customers`, `products`, and three order batches into a SQLite Star Schema,
with data-quality validation, quarantine, and idempotent / incremental
loading.

## 1. วิธีติดตั้ง (Setup)

```bash
pip install pandas
```

Only the Python standard library (`sqlite3`, `dataclasses`, `logging`) and
`pandas` are required.

## 2. วิธีรัน (How to run)

```
pipeline_lab/
├── data/                     # source CSVs (extracted from the lab dataset PDF)
│   ├── customers.csv
│   ├── products.csv
│   ├── orders_batch_1.csv
│   ├── orders_batch_2.csv
│   ├── orders_batch_3.csv
│   └── data_dictionary.csv
├── pipeline.py                # the pipeline itself
├── output/
│   ├── retail_dw.db           # SQLite Star Schema after all 3 batches
│   ├── quarantine.csv         # rejected rows with reason_code
│   └── pipeline_run_log.csv   # per-run KPIs / watermark log
└── README.md
```

Run the built-in demo (runs `batch_1`, re-runs `batch_1` to prove
idempotency, then `batch_2`, then `batch_3`):

```bash
cd pipeline_lab
python pipeline.py
```

Or drive it programmatically for any subset/order of batches:

```python
from pathlib import Path
from pipeline import PipelineConfig, run_pipeline

config = PipelineConfig(
    input_path=Path("data"),
    output_db=Path("output/retail_dw.db"),
    batches=["1", "2", "3"],
    error_mode="continue",   # or "strict" to raise on the first bad batch
)
result = run_pipeline(config)
print(result["kpi"])
```

## 3. โครงสร้าง Star Schema

Grain of `fact_sales`: **one validated order-product transaction line per
`order_id`.**

| Table | Key columns | Notes |
|---|---|---|
| `dim_customer` | `customer_key` (PK, autoincrement), `customer_id` (UNIQUE) | customer_name, province, segment |
| `dim_product` | `product_key` (PK, autoincrement), `product_id` (UNIQUE) | product_name, category |
| `dim_date` | `date_key` (PK, `YYYYMMDD` int), `full_date` (UNIQUE) | day, month, quarter, year |
| `fact_sales` | `order_id` (PK) | FKs to the three dimensions above; quantity, unit_price, discount_pct, gross_amount, net_amount, payment_method, sales_channel, updated_at, source_batch |
| `quarantine` | `id` (PK, autoincrement) | rejected order rows + `reason_code` + `source_batch` + `run_id` |
| `pipeline_run_log` | `run_id` (PK, autoincrement) | one row per batch run: rows_read / rows_valid / rows_rejected / rows_duplicated / rows_loaded / status |

`order_id` as the `fact_sales` primary key, combined with `INSERT ... ON
CONFLICT(order_id) DO UPDATE`, is what makes loads both **idempotent**
(re-running the same batch changes nothing) and **incremental** (a row with
a newer `updated_at` than what's stored replaces it; a row that is not
newer is skipped).

### Data-quality rules enforced (see `data/data_dictionary.csv`)

- `order_id`, `order_datetime` required and parseable.
- `customer_id` / `product_id` required **and** must exist in the
  dimension tables (referential integrity).
- `quantity` must be a valid integer from 1–20 (text like `"three"` and
  out-of-range/negative values are rejected).
- `unit_price` must be numeric and > 0 (values like `"THB 649.9"` are
  coerced, not silently kept).
- `discount_pct` must be numeric, 0–100.
- `payment_method` / `sales_channel` are normalized to a fixed label set
  (case-insensitive matching; `E-Commerce` → `Online` per the data
  dictionary's explicit mapping).
- Rows failing any rule go to `quarantine` with a specific `reason_code`
  (e.g. `MISSING_CUSTOMER_ID`, `CUSTOMER_NOT_FOUND`, `INVALID_QUANTITY_TYPE`,
  `INVALID_ORDER_DATETIME`, …) instead of stopping the batch.
- Within a batch, duplicate `order_id`s are resolved by keeping the row
  with the latest `updated_at` (Task 2 requirement) **before** load.

### Run-log formula

For every batch run: `rows_read = rows_valid (pre-dedup) + rows_rejected`.
Deduplication and "already up to date" skips happen *after* this count and
are reported separately (`rows_duplicated`, and `rows_loaded` vs. clean
row count), so the identity above always holds exactly — verified in
`pipeline_run_log.csv`.

## 4. Reflection — เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline

ใน Production Pipeline ข้อมูลต้นทางแทบไม่มีทางสมบูรณ์แบบเสมอไป — จะมีค่าว่าง
ประเภทข้อมูลผิด หรือ Referential Integrity ที่ขาดหายอยู่เสมอ หาก Pipeline
ถูกออกแบบให้ "Strict" คือหยุดทำงานทั้งระบบทันทีที่พบแถวใดแถวหนึ่งผิดปกติ
ผลกระทบจะไม่ได้จำกัดอยู่แค่แถวนั้น แต่จะทำให้ข้อมูลที่ถูกต้องอีกหลายพันแถวไม่ถูกโหลด
เข้าคลังข้อมูลไปด้วย ทำให้ Dashboard หรือรายงานที่ฝ่ายธุรกิจใช้ตัดสินใจกลายเป็น
ข้อมูลที่ขาดหายหรือล้าสมัย ซึ่งสร้างความเสียหายในวงกว้างกว่าการมีข้อมูลบางแถวที่รอ
การแก้ไข

แนวทางที่ Pipeline นี้ใช้คือแยกข้อมูลที่ผ่านการตรวจสอบ (clean) ออกจากข้อมูลที่มี
ปัญหา (quarantine) พร้อมบันทึก reason_code ที่ชัดเจน ทำให้ทีมข้อมูลสามารถไล่แก้ไข
ต้นตอของปัญหาได้ภายหลัง โดยที่ระบบส่วนใหญ่ยังทำงานต่อได้ตามปกติ (Availability)
เช่นเดียวกับระดับ batch — หาก batch หนึ่งอ่านไฟล์ไม่สำเร็จ ระบบจะบันทึกสถานะ
"failed" ไว้ใน pipeline_run_log แต่จะไม่ rollback ข้อมูลจาก batch ก่อนหน้าที่โหลด
สำเร็จไปแล้ว

Strictness ยังคงสำคัญ แต่ควรถูกใช้ ณ จุดที่ผลกระทบของความผิดพลาดรุนแรงจริง ๆ
เช่น Primary Key ซ้ำใน fact_sales หรือ Foreign Key ที่ไม่สามารถ resolve ได้ ซึ่ง
Pipeline นี้ยังคงบังคับด้วย constraint และ validation ที่เข้มงวด — แต่ใช้ในระดับ
"แถว" ไม่ใช่ระดับ "ทั้งระบบ" เพื่อให้ทั้งคุณภาพข้อมูลและความพร้อมใช้งานของระบบ
อยู่ร่วมกันได้
