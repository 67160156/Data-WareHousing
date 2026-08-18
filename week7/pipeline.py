"""
Python Data Pipeline Engineering - Lab Assignment
ETL pipeline for an Omnichannel Retail Data Warehouse.

Builds an incremental, idempotent pipeline that:
  1. Extracts customers / products / orders_batch_N from CSV source files.
  2. Transforms and validates the order rows (type coercion, normalization,
     referential-integrity checks, business-rule checks, dedup).
  3. Loads a Star Schema (dim_customer, dim_product, dim_date, fact_sales)
     into a SQLite database, using upserts so re-running a batch never
     duplicates or corrupts data.
  4. Writes quarantine records (rows that fail validation) with a
     reason_code, and a pipeline_run_log with per-run KPIs.

Run directly for a demo:  python pipeline.py
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------- #
# Task 1 - Pipeline Configuration
# --------------------------------------------------------------------------- #
@dataclass
class PipelineConfig:
    """Configuration for a pipeline run."""
    input_path: Path                       # directory holding the source CSVs
    output_db: Path                        # path to the SQLite database file
    batches: list[str] = field(default_factory=lambda: ["1", "2", "3"])
    error_mode: Literal["continue", "strict"] = "continue"
    quarantine_path: Path = Path("output/quarantine.csv")
    run_log_path: Path = Path("output/pipeline_run_log.csv")

    def __post_init__(self):
        self.input_path = Path(self.input_path)
        self.output_db = Path(self.output_db)
        self.quarantine_path = Path(self.quarantine_path)
        self.run_log_path = Path(self.run_log_path)


# --------------------------------------------------------------------------- #
# Reference / validation domains
# --------------------------------------------------------------------------- #
APPROVED_PROVINCES = {
    "Bangkok", "Chonburi", "Rayong", "Chanthaburi",
    "Chachoengsao", "Samut Prakan",
}

PAYMENT_METHOD_MAP = {
    "cash": "Cash",
    "credit card": "Credit Card",
    "bank transfer": "Bank Transfer",
    "promptpay": "PromptPay",
}

SALES_CHANNEL_MAP = {
    "store": "Store",
    "online": "Online",
    "marketplace": "Marketplace",
    "e-commerce": "Online",   # explicit mapping required by data dictionary
}


# --------------------------------------------------------------------------- #
# Task 1 - Extract
# --------------------------------------------------------------------------- #
def extract_csv(path: Path, label: str) -> pd.DataFrame:
    """Read a single source CSV with try/except + start/end/row-count logging.
    Never mutates the source file."""
    start = time.time()
    log.info("EXTRACT start | source=%s", label)
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as exc:
        log.error("EXTRACT failed | source=%s | error=%s", label, exc)
        raise
    elapsed = time.time() - start
    log.info(
        "EXTRACT done  | source=%s | rows=%d | elapsed=%.3fs",
        label, len(df), elapsed,
    )
    return df


def extract_dimensions(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    customers = extract_csv(config.input_path / "customers.csv", "customers")
    products = extract_csv(config.input_path / "products.csv", "products")
    return customers, products


def extract_orders_batch(config: PipelineConfig, batch: str) -> pd.DataFrame:
    path = config.input_path / f"orders_batch_{batch}.csv"
    return extract_csv(path, f"orders_batch_{batch}")


# --------------------------------------------------------------------------- #
# Task 2 - Transform & Data Quality
# --------------------------------------------------------------------------- #
def _clean_numeric(series: pd.Series) -> pd.Series:
    """Strip currency prefixes/whitespace and coerce to numeric (NaN on failure)."""
    cleaned = (
        series.astype(str)
        .str.replace(r"[A-Za-z฿]", "", regex=True)
        .str.strip()
        .replace("", pd.NA)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _clean_datetime(series: pd.Series) -> pd.Series:
    """Parse mixed date formats; unparseable / impossible dates (e.g. 31/02)
    become NaT, which is caught later as INVALID_DATE."""
    return pd.to_datetime(series, errors="coerce")


def transform_orders(
    orders: pd.DataFrame,
    dim_customer_ids: set[str],
    dim_product_ids: set[str],
    source_batch_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Validate + clean one batch's orders. Returns:
      clean_df       - rows that passed all checks, deduplicated by order_id
                        (keeping the row with the latest updated_at)
      quarantine_df  - rows that failed validation, each with a reason_code
      stats          - dict of intermediate counts, used for the KPI / run log
    """
    df = orders.copy()
    rows_read = len(df)

    # ---- type coercion (safe / "errors=coerce" style) ----
    df["quantity_num"] = _clean_numeric(df["quantity"])
    df["unit_price_num"] = _clean_numeric(df["unit_price"])
    df["discount_pct_num"] = _clean_numeric(df["discount_pct"])
    df["order_datetime_parsed"] = _clean_datetime(df["order_datetime"])
    df["updated_at_parsed"] = _clean_datetime(df["updated_at"])

    # ---- normalize categoricals (documented mapping) ----
    df["payment_method_norm"] = (
        df["payment_method"].astype(str).str.strip().str.lower().map(PAYMENT_METHOD_MAP)
    )
    df["sales_channel_norm"] = (
        df["sales_channel"].astype(str).str.strip().str.lower().map(SALES_CHANNEL_MAP)
    )

    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["order_id"] = df["order_id"].astype(str).str.strip()

    # ---- row-level validation -> collect first-failing reason_code per row ----
    def reason_code(row) -> str | None:
        if not row["order_id"]:
            return "MISSING_ORDER_ID"
        if pd.isna(row["order_datetime_parsed"]):
            return "INVALID_ORDER_DATETIME"
        if not row["customer_id"] or row["customer_id"] in ("", "nan"):
            return "MISSING_CUSTOMER_ID"
        if row["customer_id"] not in dim_customer_ids:
            return "CUSTOMER_NOT_FOUND"
        if not row["product_id"] or row["product_id"] in ("", "nan"):
            return "MISSING_PRODUCT_ID"
        if row["product_id"] not in dim_product_ids:
            return "PRODUCT_NOT_FOUND"
        if pd.isna(row["quantity_num"]):
            return "INVALID_QUANTITY_TYPE"
        if not (1 <= row["quantity_num"] <= 20):
            return "QUANTITY_OUT_OF_RANGE"
        if pd.isna(row["unit_price_num"]) or row["unit_price_num"] <= 0:
            return "INVALID_UNIT_PRICE"
        if pd.isna(row["discount_pct_num"]) or not (0 <= row["discount_pct_num"] <= 100):
            return "INVALID_DISCOUNT_PCT"
        if row["payment_method_norm"] is None or (
            isinstance(row["payment_method_norm"], float) and pd.isna(row["payment_method_norm"])
        ):
            return "INVALID_PAYMENT_METHOD"
        if row["sales_channel_norm"] is None or (
            isinstance(row["sales_channel_norm"], float) and pd.isna(row["sales_channel_norm"])
        ):
            return "INVALID_SALES_CHANNEL"
        if pd.isna(row["updated_at_parsed"]):
            return "INVALID_UPDATED_AT"
        return None

    df["reason_code"] = df.apply(reason_code, axis=1)

    invalid_mask = df["reason_code"].notna()
    valid_before_dedup = df[~invalid_mask].copy()
    rejected = df[invalid_mask].copy()
    rejected["source_batch"] = source_batch_label

    rows_valid_before_dedup = len(valid_before_dedup)
    rows_rejected = len(rejected)
    assert rows_valid_before_dedup + rows_rejected == rows_read, (
        "read must equal valid_before_dedup + rejected"
    )

    # ---- Task 2: dedupe by order_id, keep latest updated_at ----
    valid_before_dedup = valid_before_dedup.sort_values("updated_at_parsed")
    deduped = valid_before_dedup.drop_duplicates(subset="order_id", keep="last").copy()
    duplicates_removed = rows_valid_before_dedup - len(deduped)

    # ---- derived amounts ----
    deduped["gross_amount"] = (deduped["quantity_num"] * deduped["unit_price_num"]).round(2)
    deduped["net_amount"] = (
        deduped["gross_amount"] * (1 - deduped["discount_pct_num"] / 100)
    ).round(2)

    clean_df = pd.DataFrame({
        "order_id": deduped["order_id"],
        "order_datetime": deduped["order_datetime_parsed"],
        "customer_id": deduped["customer_id"],
        "product_id": deduped["product_id"],
        "quantity": deduped["quantity_num"].astype(int),
        "unit_price": deduped["unit_price_num"],
        "discount_pct": deduped["discount_pct_num"],
        "payment_method": deduped["payment_method_norm"],
        "sales_channel": deduped["sales_channel_norm"],
        "gross_amount": deduped["gross_amount"],
        "net_amount": deduped["net_amount"],
        "updated_at": deduped["updated_at_parsed"],
        "source_batch": source_batch_label,
    })

    quarantine_df = pd.DataFrame({
        "order_id": rejected["order_id"],
        "order_datetime": rejected["order_datetime"],
        "customer_id": rejected["customer_id"],
        "product_id": rejected["product_id"],
        "quantity": rejected["quantity"],
        "unit_price": rejected["unit_price"],
        "discount_pct": rejected["discount_pct"],
        "payment_method": rejected["payment_method"],
        "sales_channel": rejected["sales_channel"],
        "updated_at": rejected["updated_at"],
        "reason_code": rejected["reason_code"],
        "source_batch": rejected["source_batch"],
    })

    stats = {
        "rows_read": rows_read,
        "rows_valid_before_dedup": rows_valid_before_dedup,
        "rows_rejected": rows_rejected,
        "rows_duplicated": duplicates_removed,
        "rows_clean": len(clean_df),
    }
    log.info(
        "TRANSFORM done | batch=%s | read=%d valid=%d rejected=%d duplicates=%d clean=%d",
        source_batch_label, rows_read, rows_valid_before_dedup, rows_rejected,
        duplicates_removed, len(clean_df),
    )
    return clean_df, quarantine_df, stats


def transform_dimensions(
    customers: pd.DataFrame, products: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Light validation/cleanup of the dimension sources (used as reference
    data for FK checks and dim table loads)."""
    c = customers.copy()
    c["customer_id"] = c["customer_id"].astype(str).str.strip()
    c["province"] = c["province"].astype(str).str.strip()
    c = c[c["customer_id"].astype(bool)]
    c = c.drop_duplicates(subset="customer_id", keep="last")

    p = products.copy()
    p["product_id"] = p["product_id"].astype(str).str.strip()
    p = p[p["product_id"].astype(bool)]
    p = p.drop_duplicates(subset="product_id", keep="last")
    return c, p


# --------------------------------------------------------------------------- #
# Task 3 - Star Schema DDL
# --------------------------------------------------------------------------- #
DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id  TEXT UNIQUE NOT NULL,
    customer_name TEXT,
    province     TEXT,
    segment      TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id   TEXT UNIQUE NOT NULL,
    product_name TEXT,
    category     TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key   INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date  TEXT UNIQUE NOT NULL,
    day        INTEGER,
    month      INTEGER,
    quarter    INTEGER,
    year       INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id       TEXT PRIMARY KEY,     -- grain: one validated order-product line per order_id
    date_key       INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key   INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key    INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity       INTEGER NOT NULL,
    unit_price     REAL NOT NULL,
    discount_pct   REAL NOT NULL,
    gross_amount   REAL NOT NULL,
    net_amount     REAL NOT NULL,
    payment_method TEXT,
    sales_channel  TEXT,
    updated_at     TEXT NOT NULL,
    source_batch   TEXT
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    order_datetime TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount_pct TEXT,
    payment_method TEXT,
    sales_channel TEXT,
    updated_at TEXT,
    reason_code TEXT,
    source_batch TEXT,
    run_id INTEGER
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch TEXT,
    started_at TEXT,
    ended_at TEXT,
    rows_read INTEGER,
    rows_valid INTEGER,
    rows_rejected INTEGER,
    rows_duplicated INTEGER,
    rows_loaded INTEGER,
    status TEXT,
    error_message TEXT
);
"""


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# --------------------------------------------------------------------------- #
# Task 3 - Load
# --------------------------------------------------------------------------- #
def load_dim_customer(conn: sqlite3.Connection, customers: pd.DataFrame) -> None:
    rows = list(
        customers[["customer_id", "customer_name", "province", "segment"]].itertuples(
            index=False, name=None
        )
    )
    conn.executemany(
        """
        INSERT INTO dim_customer (customer_id, customer_name, province, segment)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(customer_id) DO UPDATE SET
            customer_name = excluded.customer_name,
            province = excluded.province,
            segment = excluded.segment
        """,
        rows,
    )


def load_dim_product(conn: sqlite3.Connection, products: pd.DataFrame) -> None:
    rows = list(
        products[["product_id", "product_name", "category"]].itertuples(index=False, name=None)
    )
    conn.executemany(
        """
        INSERT INTO dim_product (product_id, product_name, category)
        VALUES (?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            product_name = excluded.product_name,
            category = excluded.category
        """,
        rows,
    )


def load_dim_date(conn: sqlite3.Connection, dates: pd.Series) -> None:
    unique_dates = pd.to_datetime(dates.dropna().unique())
    rows = []
    for d in unique_dates:
        date_key = int(d.strftime("%Y%m%d"))
        quarter = (d.month - 1) // 3 + 1
        rows.append((date_key, d.strftime("%Y-%m-%d"), d.day, d.month, quarter, d.year))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def _existing_fact_updated_at(conn: sqlite3.Connection) -> dict[str, str]:
    cur = conn.execute("SELECT order_id, updated_at FROM fact_sales")
    return {r[0]: r[1] for r in cur.fetchall()}


def load_fact_sales(conn: sqlite3.Connection, clean_df: pd.DataFrame) -> tuple[int, int]:
    """
    Upsert clean rows into fact_sales. A row is only written if it is new
    or its updated_at is strictly newer than what's already stored — this
    is what makes the load both idempotent (re-running a batch changes
    nothing) and incremental (only new/changed data is applied).
    Returns (rows_loaded, rows_skipped_as_stale).
    """
    existing = _existing_fact_updated_at(conn)

    cust_map = dict(conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
    prod_map = dict(conn.execute("SELECT product_id, product_key FROM dim_product").fetchall())

    to_load = []
    skipped_stale = 0
    for row in clean_df.itertuples(index=False):
        updated_at_str = row.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        prev = existing.get(row.order_id)
        if prev is not None and prev >= updated_at_str:
            skipped_stale += 1
            continue
        date_key = int(row.order_datetime.strftime("%Y%m%d"))
        to_load.append((
            row.order_id,
            date_key,
            cust_map[row.customer_id],
            prod_map[row.product_id],
            int(row.quantity),
            float(row.unit_price),
            float(row.discount_pct),
            float(row.gross_amount),
            float(row.net_amount),
            row.payment_method,
            row.sales_channel,
            updated_at_str,
            row.source_batch,
        ))

    conn.executemany(
        """
        INSERT INTO fact_sales (
            order_id, date_key, customer_key, product_key, quantity, unit_price,
            discount_pct, gross_amount, net_amount, payment_method, sales_channel,
            updated_at, source_batch
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            date_key = excluded.date_key,
            customer_key = excluded.customer_key,
            product_key = excluded.product_key,
            quantity = excluded.quantity,
            unit_price = excluded.unit_price,
            discount_pct = excluded.discount_pct,
            gross_amount = excluded.gross_amount,
            net_amount = excluded.net_amount,
            payment_method = excluded.payment_method,
            sales_channel = excluded.sales_channel,
            updated_at = excluded.updated_at,
            source_batch = excluded.source_batch
        """,
        to_load,
    )
    return len(to_load), skipped_stale


def load_quarantine(conn: sqlite3.Connection, quarantine_df: pd.DataFrame, run_id: int) -> None:
    if quarantine_df.empty:
        return
    df = quarantine_df.copy()
    df["run_id"] = run_id
    cols = [
        "order_id", "order_datetime", "customer_id", "product_id", "quantity",
        "unit_price", "discount_pct", "payment_method", "sales_channel",
        "updated_at", "reason_code", "source_batch", "run_id",
    ]
    conn.executemany(
        f"INSERT INTO quarantine ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
        list(df[cols].itertuples(index=False, name=None)),
    )


def log_run(conn: sqlite3.Connection, batch: str, started_at: str, ended_at: str,
            rows_read: int, rows_valid: int, rows_rejected: int, rows_duplicated: int,
            rows_loaded: int, status: str, error_message: str | None) -> int:
    cur = conn.execute(
        """
        INSERT INTO pipeline_run_log (
            batch, started_at, ended_at, rows_read, rows_valid, rows_rejected,
            rows_duplicated, rows_loaded, status, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (batch, started_at, ended_at, rows_read, rows_valid, rows_rejected,
         rows_duplicated, rows_loaded, status, error_message),
    )
    return cur.lastrowid


# --------------------------------------------------------------------------- #
# Task 5 - Orchestration
# --------------------------------------------------------------------------- #
def run_batch(conn: sqlite3.Connection, config: PipelineConfig, batch: str) -> dict:
    """Extract -> transform -> validate -> load a single batch, inside a
    transaction. Any unexpected failure (bad file, etc.) is caught, logged
    as a 'failed' run, and does NOT roll back data from earlier batches."""
    started_at = datetime.now().isoformat(timespec="seconds")
    try:
        orders = extract_orders_batch(config, batch)
        cust_ids = set(
            r[0] for r in conn.execute("SELECT customer_id FROM dim_customer").fetchall()
        )
        prod_ids = set(
            r[0] for r in conn.execute("SELECT product_id FROM dim_product").fetchall()
        )
        clean_df, quarantine_df, stats = transform_orders(orders, cust_ids, prod_ids, batch)

        conn.execute("BEGIN")
        load_dim_date(conn, clean_df["order_datetime"])
        rows_loaded, skipped_stale = load_fact_sales(conn, clean_df)
        run_id = log_run(
            conn, batch, started_at, datetime.now().isoformat(timespec="seconds"),
            stats["rows_read"], stats["rows_valid_before_dedup"], stats["rows_rejected"],
            stats["rows_duplicated"], rows_loaded, "success", None,
        )
        load_quarantine(conn, quarantine_df, run_id)
        conn.commit()

        log.info(
            "LOAD done     | batch=%s | loaded=%d skipped_stale(already up to date)=%d",
            batch, rows_loaded, skipped_stale,
        )
        result = dict(stats)
        result.update(rows_loaded=rows_loaded, skipped_stale=skipped_stale, status="success")
        return result

    except Exception as exc:
        conn.rollback()
        log.error("BATCH FAILED  | batch=%s | error=%s", batch, exc)
        log_run(
            conn, batch, started_at, datetime.now().isoformat(timespec="seconds"),
            0, 0, 0, 0, 0, "failed", str(exc),
        )
        conn.commit()
        if config.error_mode == "strict":
            raise
        return {"status": "failed", "error": str(exc)}


def run_pipeline(config: PipelineConfig) -> dict:
    """Top-level orchestrator: run every batch in config.batches in order."""
    config.output_db.parent.mkdir(parents=True, exist_ok=True)
    config.quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(config.output_db)
    conn.execute("PRAGMA foreign_keys = ON")
    create_schema(conn)

    customers_raw, products_raw = extract_dimensions(config)
    customers, products = transform_dimensions(customers_raw, products_raw)
    conn.execute("BEGIN")
    load_dim_customer(conn, customers)
    load_dim_product(conn, products)
    conn.commit()

    results = {}
    for batch in config.batches:
        results[batch] = run_batch(conn, config, batch)

    _export_quarantine_csv(conn, config.quarantine_path)
    _export_run_log_csv(conn, config.run_log_path)

    kpi = summarize_kpi(conn)
    conn.close()
    return {"batches": results, "kpi": kpi}


def _export_quarantine_csv(conn: sqlite3.Connection, path: Path) -> None:
    df = pd.read_sql_query("SELECT * FROM quarantine", conn)
    df.to_csv(path, index=False)


def _export_run_log_csv(conn: sqlite3.Connection, path: Path) -> None:
    df = pd.read_sql_query("SELECT * FROM pipeline_run_log", conn)
    df.to_csv(path, index=False)


def summarize_kpi(conn: sqlite3.Connection) -> dict:
    rows_read = conn.execute("SELECT COALESCE(SUM(rows_read),0) FROM pipeline_run_log").fetchone()[0]
    rows_valid = conn.execute("SELECT COALESCE(SUM(rows_valid),0) FROM pipeline_run_log").fetchone()[0]
    rows_rejected = conn.execute("SELECT COALESCE(SUM(rows_rejected),0) FROM pipeline_run_log").fetchone()[0]
    rows_duplicated = conn.execute("SELECT COALESCE(SUM(rows_duplicated),0) FROM pipeline_run_log").fetchone()[0]
    fact_rows = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    net_sales = conn.execute("SELECT COALESCE(SUM(net_amount),0) FROM fact_sales").fetchone()[0]
    return {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rows_duplicated": rows_duplicated,
        "fact_rows_current": fact_rows,
        "total_net_sales": round(net_sales, 2),
    }


# --------------------------------------------------------------------------- #
# Demo entry point: batch_1, batch_1 (repeat), batch_2, batch_3
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    cfg = PipelineConfig(
        input_path=Path("data"),
        output_db=Path("output/retail_dw.db"),
        batches=["1", "2", "3"],
        error_mode="continue",
        quarantine_path=Path("output/quarantine.csv"),
        run_log_path=Path("output/pipeline_run_log.csv"),
    )

    if cfg.output_db.exists():
        cfg.output_db.unlink()

    conn = sqlite3.connect(cfg.output_db)
    conn.execute("PRAGMA foreign_keys = ON")
    create_schema(conn)

    customers_raw, products_raw = extract_dimensions(cfg)
    customers, products = transform_dimensions(customers_raw, products_raw)
    conn.execute("BEGIN")
    load_dim_customer(conn, customers)
    load_dim_product(conn, products)
    conn.commit()

    print("\n=== RUN 1: batch_1 ===")
    r1 = run_batch(conn, cfg, "1")
    fact_count_1 = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    print("fact_sales row count after run 1:", fact_count_1)

    print("\n=== RUN 2: batch_1 REPEATED (idempotency check) ===")
    r2 = run_batch(conn, cfg, "1")
    fact_count_2 = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    print("fact_sales row count after repeat run:", fact_count_2)
    assert fact_count_1 == fact_count_2, "Idempotency check FAILED: fact row count changed on repeat run"
    print("Idempotency check PASSED: repeat run did not add rows.")

    print("\n=== RUN 3: batch_2 ===")
    r3 = run_batch(conn, cfg, "2")
    print("fact_sales row count after batch_2:", conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0])

    print("\n=== RUN 4: batch_3 ===")
    r4 = run_batch(conn, cfg, "3")
    print("fact_sales row count after batch_3:", conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0])

    _export_quarantine_csv(conn, cfg.quarantine_path)
    _export_run_log_csv(conn, cfg.run_log_path)

    kpi = summarize_kpi(conn)
    print("\n=== KPI SUMMARY ===")
    for k, v in kpi.items():
        print(f"{k}: {v}")

    conn.close()
    print("\nDone. Outputs written to ./output/")
