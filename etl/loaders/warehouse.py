"""
etl/loaders/warehouse.py
-------------------------
All WRITE operations into the data warehouse dimension and fact tables.
Each loader:
  1. Resolves natural (GUID) keys → surrogate (INT) keys via dim lookups
  2. Deduplicates against already-loaded rows
  3. Bulk-inserts in configurable batch sizes
"""
import pandas as pd
from loguru import logger
from sqlalchemy import text
from db.connection import get_engine
from etl.extractors.source import extract_dim_keys
from config.settings import ETLConfig
from utils.helpers import log_load_result, chunk_dataframe

UNKNOWN_GUID = "00000000-0000-0000-0000-000000000000"


# ── Key resolution helpers ─────────────────────────────────────────────────────

def _build_key_map(dim_table: str, oid_col: str, key_col: str) -> dict:
    """Return {oid_str: surrogate_int} mapping for a dimension table."""
    df = extract_dim_keys(dim_table, oid_col, key_col)
    return dict(zip(df[oid_col].astype(str), df[key_col]))


def _resolve_keys(df: pd.DataFrame, mappings: list[dict]) -> pd.DataFrame:
    """
    Resolve multiple OID columns to surrogate key columns in one pass.

    mappings: list of dicts, each with:
      - oid_col:    source column name holding the GUID
      - key_col:    target column name for the surrogate key
      - dim_table:  warehouse dimension table name
      - dim_oid:    OID column name in the dimension table
      - dim_key:    key column name in the dimension table
      - unknown_key: fallback surrogate key when GUID is NULL or not found
    """
    result = df.copy()
    for m in mappings:
        key_map = _build_key_map(m["dim_table"], m["dim_oid"], m["dim_key"])
        unknown_key = m.get("unknown_key", -1)
        result[m["key_col"]] = (
            result[m["oid_col"]]
            .astype(str)
            .map(key_map)
            .fillna(unknown_key)
            .astype(int)
        )
    return result


def _get_unknown_keys() -> dict:
    """
    Fetch the actual surrogate key values for UNKNOWN sentinel rows.
    These are created by Script 01 with Code='UNKNOWN'.
    Returns dict of {KeyColumn: actual_int_value}
    """
    engine = get_engine()
    keys = {}
    lookups = [
        ("DimBranch",        "BranchKey",        "Code = 'UNKNOWN'"),
        ("DimStudent",       "StudentKey",        "StudentOid = '00000000-0000-0000-0000-000000000000'"),
        ("DimService",       "ServiceKey",        "Code = 'UNKNOWN'"),
        ("DimSchoolYear",    "SchoolYearKey",     "Code = 'UNKNOWN'"),
        ("DimZone",          "ZoneKey",           "Code = 'UNKNOWN'"),
        ("DimShuttleType",   "ShuttleTypeKey",    "Code = 'UNKNOWN'"),
        ("DimDate",          "DateKey",           "DateKey = -1"),
    ]
    with engine.connect() as conn:
        for table, key_col, where in lookups:
            try:
                row = conn.execute(
                    text(f"SELECT TOP 1 [{key_col}] FROM dbo.[{table}] WHERE {where}")
                ).fetchone()
                keys[key_col] = int(row[0]) if row else -1
            except Exception:
                keys[key_col] = -1
    return keys
    """Return set of SourceOid strings already in the fact table (for incremental load)."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT SourceOid FROM dbo.[{fact_table}]"))
        return {str(r[0]) for r in rows}


def _bulk_insert(df: pd.DataFrame, table: str) -> int:
    """
    Insert DataFrame rows into a warehouse table in batches.
    Uses method=None (single-row INSERT) to avoid SQL Server's 2100 parameter limit.
    Returns number of rows inserted.
    """
    if df.empty:
        return 0
    engine = get_engine()
    total = 0
    n_cols = len(df.columns)
    # SQL Server limit: 2100 params per statement; with multi method each row = n_cols params
    # Use row-by-row (method=None) which pandas sends as individual INSERTs — safe for any size
    safe_chunk = max(1, min(ETLConfig.BATCH_SIZE, 2000 // max(n_cols, 1)))
    for chunk in chunk_dataframe(df, safe_chunk):
        chunk.to_sql(
            name=table,
            con=engine,
            schema="dbo",
            if_exists="append",
            index=False,
            method=None,   # row-by-row INSERT — avoids 2100 param limit
        )
        total += len(chunk)
    return total


# ── Fact loaders ──────────────────────────────────────────────────────────────

def load_fact_student_service_revenue(tall_df: pd.DataFrame) -> None:
    """
    Load transformed (unpivoted) service revenue rows into FactStudentServiceRevenue.

    Expects columns from transformer:
      SourceOid, StudentOid, ServiceOid, BranchOid, SchoolYearOid,
      MonthNumber, Amount
    """
    if tall_df.empty:
        logger.warning("load_fact_student_service_revenue: nothing to load")
        return

    logger.info("Loading FactStudentServiceRevenue...")

    # ── Step 0: fetch actual sentinel key values from warehouse ────────────
    unk = _get_unknown_keys()

    # ── Step 1: filter already-loaded rows ─────────────────────────────────
    # FactStudentServiceRevenue has no single SourceOid (one source row →
    # many fact rows), so we deduplicate on (SourceOid, MonthNumber)
    engine = get_engine()
    existing = pd.read_sql(
        "SELECT SourceOid, MonthNumber FROM dbo.FactStudentServiceRevenue",
        engine
    )
    existing["_key"] = existing["SourceOid"].astype(str) + "_" + existing["MonthNumber"].astype(str)
    existing_keys = set(existing["_key"])

    tall_df["_key"] = tall_df["SourceOid"].astype(str) + "_" + tall_df["MonthNumber"].astype(str)
    new_rows = tall_df[~tall_df["_key"].isin(existing_keys)].drop(columns=["_key"])
    skipped = len(tall_df) - len(new_rows)

    if new_rows.empty:
        log_load_result("FactStudentServiceRevenue", 0, skipped)
        return

    # ── Step 2: resolve surrogate keys ─────────────────────────────────────
    new_rows = _resolve_keys(new_rows, [
        {"oid_col": "StudentOid",    "key_col": "StudentKey",
         "dim_table": "DimStudent",    "dim_oid": "StudentOid",    "dim_key": "StudentKey",    "unknown_key": unk["StudentKey"]},
        {"oid_col": "ServiceOid",    "key_col": "ServiceKey",
         "dim_table": "DimService",    "dim_oid": "ServiceOid",    "dim_key": "ServiceKey",    "unknown_key": unk["ServiceKey"]},
        {"oid_col": "BranchOid",     "key_col": "BranchKey",
         "dim_table": "DimBranch",     "dim_oid": "BranchOid",     "dim_key": "BranchKey",     "unknown_key": unk["BranchKey"]},
        {"oid_col": "SchoolYearOid", "key_col": "SchoolYearKey",
         "dim_table": "DimSchoolYear", "dim_oid": "SchoolYearOid", "dim_key": "SchoolYearKey", "unknown_key": unk["SchoolYearKey"]},
    ])

    # ── Step 3: rename Amount → MonthlyAmount to match warehouse schema ────
    if "Amount" in new_rows.columns:
        new_rows = new_rows.rename(columns={"Amount": "MonthlyAmount"})

    # ── Step 4: fill sentinel keys for columns not in source ───────────────
    new_rows["DateKey"]        = unk["DateKey"]
    new_rows["ZoneKey"]        = unk["ZoneKey"]
    new_rows["ShuttleTypeKey"] = unk["ShuttleTypeKey"]

    # ── Step 5: select only warehouse columns ──────────────────────────────
    fact_cols = [
        "StudentKey", "ServiceKey", "BranchKey", "SchoolYearKey",
        "DateKey", "ZoneKey", "ShuttleTypeKey",
        "SourceOid", "MonthNumber", "MonthlyAmount"
    ]
    new_rows = new_rows[[c for c in fact_cols if c in new_rows.columns]]

    # ── Step 4: bulk insert ─────────────────────────────────────────────────
    inserted = _bulk_insert(new_rows, "FactStudentServiceRevenue")
    log_load_result("FactStudentServiceRevenue", inserted, skipped)


def load_fact_student_service_tranche(raw_df: pd.DataFrame) -> None:
    """
    Load StudentServiceReportTranche into FactStudentServiceTranche.

    Source is a wide/pivoted table: one row per student-service subscription,
    with many tranche amount columns (TR1, TR2, Tranche 1, Tranche 1(22-23)…).
    Warehouse has UNIQUE constraint on SourceOid → one row per source record.

    Strategy: sum all non-null tranche amount columns into a single Amount value.
    """
    if raw_df.empty:
        logger.warning("load_fact_student_service_tranche: nothing to load")
        return

    logger.info("Loading FactStudentServiceTranche...")

    # ── Step 0: fetch actual sentinel key values from warehouse ────────────
    unk = _get_unknown_keys()

    # ── Step 1: rename Oid → SourceOid ─────────────────────────────────────
    df = raw_df.rename(columns={"Oid": "SourceOid"})

    # ── Step 2: deduplicate against warehouse ──────────────────────────────
    engine = get_engine()
    existing = pd.read_sql("SELECT SourceOid FROM dbo.FactStudentServiceTranche", engine)
    existing_set = set(existing["SourceOid"].astype(str))
    new_rows = df[~df["SourceOid"].astype(str).isin(existing_set)].copy()
    skipped = len(df) - len(new_rows)

    if new_rows.empty:
        log_load_result("FactStudentServiceTranche", 0, skipped)
        return

    # ── Step 3: resolve surrogate keys ─────────────────────────────────────
    new_rows = _resolve_keys(new_rows, [
        {"oid_col": "Student",           "key_col": "StudentKey",
         "dim_table": "DimStudent",       "dim_oid": "StudentOid",    "dim_key": "StudentKey",    "unknown_key": unk["StudentKey"]},
        {"oid_col": "SchoolService",      "key_col": "ServiceKey",
         "dim_table": "DimService",       "dim_oid": "ServiceOid",    "dim_key": "ServiceKey",    "unknown_key": unk["ServiceKey"]},
        {"oid_col": "Branch",             "key_col": "BranchKey",
         "dim_table": "DimBranch",        "dim_oid": "BranchOid",     "dim_key": "BranchKey",     "unknown_key": unk["BranchKey"]},
        {"oid_col": "CurrentSchoolYear",  "key_col": "SchoolYearKey",
         "dim_table": "DimSchoolYear",    "dim_oid": "SchoolYearOid", "dim_key": "SchoolYearKey", "unknown_key": unk["SchoolYearKey"]},
    ])

    # ── Step 4: sum all tranche amount columns → Amount ────────────────────
    # All columns except known non-amount cols are candidates
    non_amount_cols = {
        "oid", "branch", "createdby", "currentschoolyear", "schoolservice",
        "zone", "shuttletype", "serviceinvoicelineoption", "student", "order",
        "subscriptionpayed", "optimisticlockfield", "gcrecord",
        "sourceoid", "studentkey", "servicekey", "branchkey", "schoolyearkey"
    }
    # Tranche amount cols = numeric cols whose lowercase name is not in exclusion set
    #   and don't end with _solde (those are balances/remainders, not amounts paid)
    amount_cols = [
        c for c in new_rows.columns
        if c.lower() not in non_amount_cols
        and not c.lower().endswith("_solde")
        and c not in ("SourceOid",)
        and pd.api.types.is_numeric_dtype(new_rows[c])
    ]
    logger.info(f"  Tranche amount columns detected ({len(amount_cols)}): {amount_cols}")

    if amount_cols:
        new_rows["Amount"] = new_rows[amount_cols].apply(
            pd.to_numeric, errors="coerce"
        ).sum(axis=1, skipna=True)
    else:
        new_rows["Amount"] = 0.0

    # ── Step 5: map Order → SortOrder ──────────────────────────────────────
    if "Order" in new_rows.columns:
        new_rows = new_rows.rename(columns={"Order": "SortOrder"})
    else:
        new_rows["SortOrder"] = 0

    # ── Step 6: select warehouse columns ───────────────────────────────────
    fact_cols = ["StudentKey", "ServiceKey", "BranchKey", "SchoolYearKey",
                 "SourceOid", "SortOrder", "Amount"]
    new_rows = new_rows[[c for c in fact_cols if c in new_rows.columns]]

    # ── Step 7: bulk insert ────────────────────────────────────────────────
    inserted = _bulk_insert(new_rows, "FactStudentServiceTranche")
    log_load_result("FactStudentServiceTranche", inserted, skipped)