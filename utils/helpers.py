"""
utils/helpers.py
----------------
Reusable utility functions shared across ETL, dashboards, and reports.
"""
import pandas as pd
from loguru import logger


def safe_cast_decimal(series: pd.Series, precision: int = 2) -> pd.Series:
    """
    Convert a pandas Series to float, coercing errors to NaN.
    Replaces comma-decimal notation (French locale) with dot.
    """
    return (
        series
        .astype(str)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .round(precision)
    )


def log_load_result(table: str, inserted: int, skipped: int = 0) -> None:
    """Standard log line after each ETL load step."""
    logger.info(f"  → {table}: {inserted} inserted, {skipped} skipped")


def chunk_dataframe(df: pd.DataFrame, size: int) -> list[pd.DataFrame]:
    """Split a DataFrame into chunks of `size` rows for batch inserts."""
    return [df.iloc[i:i + size] for i in range(0, len(df), size)]
