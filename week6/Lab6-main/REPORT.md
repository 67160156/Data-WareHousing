# ETL Lab Report

Student ID: 67160156
Name: ธมนวรรณ ศรีสวัสดิ์

---

## 1. Data Quality Problems Found

**customers.csv** (62 แถว)

| ปัญหา | ตัวอย่าง | จำนวน |
|---|---|---|
| `customer_id` ซ้ำ | C004, C009 ปรากฏ 2 ครั้ง | 2 แถว |
| จังหวัดเขียนหลายแบบ | `BKK` / `bangkok` / `กรุงเทพฯ` → จังหวัดเดียวกัน | เกือบทุกแถว |
| จังหวัดสะกดตกตัวอักษร | `chantaburi` (ขาด h) vs `Chanthaburi` | 3 แถว |
| จังหวัดว่าง | C013 | 1 แถว |
| email ว่าง | C006 | 1 แถว |

**products.json** (15 แถว)

| ปัญหา | ตัวอย่าง |
|---|---|
| JSON ซ้อนชั้น | `category.name`, `pricing.price` ไม่ใช่คอลัมน์แบน |
| ราคาเป็นข้อความมี comma | P005 = `"1,299.00"` |
| category เป็น null | P009 |

**orders.csv** (183 แถว)

| ปัญหา | ตัวอย่าง | จำนวน |
|---|---|---|
| `order_id` ซ้ำ | O0011, O0041, O0101 | 3 แถว |
| วันที่ 4 รูปแบบปนกัน | `2026-08-01`, `2026/08/02`, `01/08/2026`, `03-Aug-2026` | ทั้งไฟล์ |
| วันที่ใช้ไม่ได้ | O0034 = `not-a-date` | 1 แถว |
| status ตัวพิมพ์ไม่สม่ำเสมอ | `PAID` vs `paid` | หลายแถว |
| qty ติดลบ | O0007 = -2 | 1 แถว |
| unit_price ติดลบ | O0091 = -100.0 | 1 แถว |
| discount_pct เกินช่วง | O0021 = 150 | 1 แถว |
| อ้าง master ที่ไม่มีอยู่ | O0049 → C999, O0076 → P999 | 2 แถว |

---

## 2. Cleaning / Transformation Rules

**Customers**

- `drop_duplicates(subset=["customer_id"], keep="first")`
- จังหวัด: `str.lower()` แล้วเทียบกับ `PROVINCE_MAP` ใน `config.py` — จับได้ทั้งอังกฤษ ไทย และตัวย่อ ค่าที่ไม่รู้จักคง Title Case ไว้
- จังหวัด/email ที่ว่าง → `"Unknown"` (ไม่ทิ้งแถว เพราะลูกค้ายังมีตัวตนจริง แค่ข้อมูลติดต่อไม่ครบ)

**Products**

- `pd.json_normalize()` แบน nested JSON → `category.name`, `pricing.price`
- rename → `category`, `price`
- ราคา: ตัดทุกอักขระที่ไม่ใช่ตัวเลข/จุด/ลบ ด้วย regex ก่อนแปลงเป็น float (`"1,299.00"` → `1299.0`)
- `category` ที่เป็น null → `"Unknown"`

**Orders**

- `drop_duplicates(subset=["order_id"], keep="first")`
- `status.str.lower()`
- วันที่: ลองทีละ format ตามลำดับ `%Y-%m-%d` → `%Y/%m/%d` → `%d/%m/%Y` → `%d-%b-%Y`
  ไม่ใช้ `pd.to_datetime` แบบเดาเอง เพราะ `01/08/2026` (1 ส.ค.) กับ `2026/08/02` (2 ส.ค.) จะถูกตีความสลับวัน/เดือนกันได้
- reject เมื่อ `qty <= 0` / `unit_price <= 0` / `discount_pct` นอกช่วง 0–100 / วันที่ parse ไม่ได้

**Merge**

- กรองเหลือเฉพาะ `status ∈ {paid, completed}`
- `left join` กับ customers และ products แล้วคัดแถวที่หา master ไม่เจอออกไปเป็น reject
- `gross_amount = qty × unit_price`
- `discount_amount = gross_amount × discount_pct / 100`
- `sales_amount = gross_amount − discount_amount`

**เส้นทางของข้อมูล**

```
183 แถวดิบ
 − 3   order_id ซ้ำ                → 180
 − 4   ผิดกฎ (ไปอยู่ rejects.csv)  → 176
 − 76  status ไม่ใช่ paid/completed → 100
 − 0   หา master ไม่เจอ            → 100  ← fact_sales
```

---

## 3. Rejected Records

จำนวน: **4 รายการ** (ดู `output/rejects.csv`)

| order_id | เหตุผล | ค่าที่ผิด |
|---|---|---|
| O0007 | `qty <= 0` | qty = -2 |
| O0021 | `discount_pct out of range 0-100` | discount_pct = 150 |
| O0034 | `invalid order_date` | order_date = `not-a-date` |
| O0091 | `unit_price <= 0` | unit_price = -100.0 |

เหตุผลหลัก: ค่าตัวเลขที่เป็นไปไม่ได้ทางธุรกิจ (จำนวน/ราคาติดลบ, ส่วนลดเกิน 100%) และวันที่ที่แปลงไม่ได้

**หมายเหตุสำคัญ** — O0049 (`C999`) และ O0076 (`P999`) อ้างถึง master ที่ไม่มีอยู่จริง
แต่ทั้งสองรายการมี `status = cancelled` จึงถูกกรองออกตั้งแต่ขั้น "เก็บเฉพาะ paid/completed"
ก่อนจะถึงขั้น join กับ master ผลลัพธ์ของ `merge_master` จึงเป็น 0
โค้ดยังคงเขียนกฎนี้ไว้ครบ (`_customer_found` / `_product_found`) เพื่อรองรับข้อมูลชุดอื่น

อนึ่ง แถวที่ status เป็น `pending` / `cancelled` (76 แถว) **ไม่นับเป็น reject**
เพราะไม่ใช่ข้อมูลผิด เพียงแต่ยังไม่ใช่ยอดขาย จึงถูกกรองออกเฉย ๆ

---

## 4. ETL Validation

| รายการ | ค่า |
|---|---|
| Valid transformed rows | 100 |
| Warehouse rows | 100 |
| Duplicate order_id (ใน warehouse) | 0 |
| Source total sales | 192,074.63 |
| Warehouse total sales | 192,074.63 |
| Validation status | **PASS** |

```json
{
  "source_valid_rows": 100,
  "warehouse_rows": 100,
  "duplicate_order_ids": 0,
  "source_total_sales": 192074.63,
  "warehouse_total_sales": 192074.63,
  "status": "PASS"
}
```

เกณฑ์ PASS ต้องผ่านครบ 3 ข้อ: จำนวนแถวเท่ากัน, ไม่มี `order_id` ซ้ำ,
และยอดขายรวมสองฝั่งต่างกันไม่เกิน 0.01 (เผื่อความคลาดเคลื่อนจากการปัดทศนิยม)

---

## 5. Idempotency Test

จำนวน fact_sales หลัง run ครั้งที่ 1: **100**

จำนวน fact_sales หลัง run ครั้งที่ 2: **100**

อธิบายผล:

จำนวนไม่เพิ่ม เพราะออกแบบขั้น Load ไว้ 2 ชั้น

1. **`order_id` เป็น PRIMARY KEY** ของ `fact_sales` — SQLite บังคับไม่ให้มีค่าซ้ำในระดับฐานข้อมูล
2. **`INSERT OR IGNORE`** — เมื่อเจอ `order_id` ที่มีอยู่แล้ว SQLite จะข้ามแถวนั้นไปเงียบ ๆ
   แทนที่จะโยน `IntegrityError` ทำให้รันซ้ำได้โดยไม่พัง และไม่เกิดข้อมูลซ้ำ

ส่วนตาราง dimension ใช้ `INSERT ... ON CONFLICT DO UPDATE` (upsert) แทน
เพราะถ้าลูกค้าย้ายจังหวัดหรือสินค้าเปลี่ยนราคา เราต้องการให้ warehouse อัปเดตตาม
ไม่ใช่ข้ามทิ้งไป — แต่จำนวนแถวก็ยังคงเท่าเดิม

จุดสำคัญคือ **ไม่ใช้ `DROP TABLE` ทุกรอบ** (ใช้ `CREATE TABLE IF NOT EXISTS`)
เพราะการล้างตารางทิ้งแล้วโหลดใหม่จะทำให้ "จำนวนเท่าเดิม" ด้วยเหตุผลที่ผิด
และใช้กับ warehouse จริงที่มีข้อมูลสะสมหลายรอบไม่ได้

หลักฐานใน `logs/etl.log`

```
run 1: fact_sales 0   -> 100 (new=100, skipped as duplicate=0)
run 2: fact_sales 100 -> 100 (new=0,   skipped as duplicate=100)
```
