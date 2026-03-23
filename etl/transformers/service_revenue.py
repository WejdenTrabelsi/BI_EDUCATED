"""
etl/transformers/service_revenue.py
-------------------------------------
Transforms StudentServiceReport (wide/pivoted) into a tall fact table
ready for FactStudentServiceRevenue.

Source shape (wide):
  Oid | Student | SchoolService | Branch | CurrentSchoolYear | 1 | 2 | ... | 12

Target shape (tall):
  StudentKey | ServiceKey | BranchKey | SchoolYearKey | MonthNumber | Amount
"""
import pandas as pd
from loguru import logger
from utils.helpers import safe_cast_decimal

# Month columns are named 1..12 (integers stored as column names)
MONTH_COLUMNS = [str(i) for i in range(1, 13)]


def detect_month_columns(df: pd.DataFrame) -> list[str]:
    """
    Detect which month columns actually exist in the source DataFrame.
    Source may have only a subset of 1-12.
    """
    found = [col for col in MONTH_COLUMNS if col in df.columns]
    logger.debug(f"Month columns found: {found}")
    return found


def unpivot_service_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Melt the wide monthly revenue DataFrame into one row per student/service/month.

    Steps:
      1. Detect which month columns exist
      2. melt() wide → tall
      3. Drop rows where Amount is NULL (student had no charge that month)
      4. Cast Amount to float
      5. Rename columns to match warehouse target

    Returns a clean DataFrame with columns:
      SourceOid, StudentOid, ServiceOid, BranchOid, SchoolYearOid,
      MonthNumber, Amount
    """
    if df.empty:
        logger.warning("service_revenue: source DataFrame is empty, nothing to transform")
        return pd.DataFrame()

    month_cols = detect_month_columns(df)
    if not month_cols:
        logger.error("service_revenue: no month columns (1-12) found in source!")
        return pd.DataFrame()

    id_cols = ["Oid", "Student", "SchoolService", "Branch", "CurrentSchoolYear"]
    # Keep only columns we need (some source tables have many extra cols)
    existing_id_cols = [c for c in id_cols if c in df.columns]

    logger.info(f"Unpivoting {len(df)} rows × {len(month_cols)} month columns "
                f"→ up to {len(df) * len(month_cols)} fact rows")

    tall = df[existing_id_cols + month_cols].melt(
        id_vars=existing_id_cols,
        value_vars=month_cols,
        var_name="MonthNumber",
        value_name="Amount",
    )

    # Drop rows with no amount (student not enrolled that month)
    before = len(tall)
    tall = tall.dropna(subset=["Amount"])
    dropped = before - len(tall)
    logger.debug(f"  Dropped {dropped} NULL-amount rows")

    # Clean types
    tall["Amount"]      = safe_cast_decimal(tall["Amount"])
    tall["MonthNumber"] = pd.to_numeric(tall["MonthNumber"], errors="coerce").astype("Int64")

    # Drop rows that became NaN after cast (bad data in source)
    tall = tall.dropna(subset=["Amount", "MonthNumber"])

    # Standardise column names to match what the loader expects
    tall = tall.rename(columns={
        "Oid":               "SourceOid",
        "Student":           "StudentOid",
        "SchoolService":     "ServiceOid",
        "Branch":            "BranchOid",
        "CurrentSchoolYear": "SchoolYearOid",
    })

    logger.info(f"  service_revenue transform complete: {len(tall)} rows")
    return tall
