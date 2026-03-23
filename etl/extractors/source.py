"""
etl/extractors/source.py
------------------------
All READ operations from the source OLTP database.
Each function returns a raw pandas DataFrame.
No transformation logic here — just extraction.
"""
import pandas as pd
from sqlalchemy import text
from loguru import logger
from db.connection import get_engine


def extract_table(table_name: str, where: str = "1=1") -> pd.DataFrame:
    """
    Generic extractor — read any source table into a DataFrame.

    Args:
        table_name: source table name (e.g. 'StudentServiceReport')
        where:      optional WHERE clause (default: all rows)

    Returns:
        DataFrame with all columns from the source table.
    """
    query = f"SELECT * FROM dbo.[{table_name}] WHERE {where}"
    engine = get_engine()
    logger.debug(f"Extracting {table_name}...")
    df = pd.read_sql(query, engine)
    logger.debug(f"  {table_name}: {len(df)} rows extracted")
    return df


def extract_student_service_report() -> pd.DataFrame:
    """
    Extract StudentServiceReport — the pivoted monthly revenue table.
    Returns raw DataFrame with month columns as-is (unpivot happens in transformer).
    Filters out soft-deleted rows (GCRecord IS NULL).
    """
    query = """
        SELECT *
        FROM dbo.StudentServiceReport
        WHERE GCRecord IS NULL
    """
    engine = get_engine()
    logger.info("Extracting StudentServiceReport (pivoted monthly revenue)...")
    df = pd.read_sql(query, engine)
    logger.info(f"  StudentServiceReport: {len(df)} rows")
    return df


def extract_student_service_report_tranche() -> pd.DataFrame:
    """
    Extract StudentServiceReportTranche — one row per student-service subscription.
    Tranche amount columns are SQL Server 'money' type, which pyodbc reads as
    Decimal objects. We cast them all to float64 here so pandas treats them as numeric.
    """
    query = "SELECT * FROM dbo.StudentServiceReportTranche"
    engine = get_engine()
    logger.info("Extracting StudentServiceReportTranche (invoice tranche lines)...")
    df = pd.read_sql(query, engine)

    # pyodbc returns SQL Server money columns as Python Decimal objects,
    # which pandas stores as dtype=object — cast all to float64
    money_cols = [
        c for c in df.columns
        if c not in ('Oid','Branch','CreatedBy','CurrentSchoolYear','SchoolService',
                     'Zone','ShuttleType','ServiceInvoiceLineOption','Student',
                     'Order','SubscriptionPayed','OptimisticLockField','GCRecord')
    ]
    for col in money_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    logger.info(f"  StudentServiceReportTranche: {len(df)} rows, "
                f"{len(money_cols)} amount columns cast to float")
    return df


def extract_dim_keys(dim_table: str, oid_col: str, key_col: str) -> pd.DataFrame:
    """
    Extract Oid → SurrogateKey mapping from a dimension table.
    Used by loaders to resolve natural keys to surrogate keys.

    Returns DataFrame with two columns: [oid_col, key_col]
    """
    query = f"SELECT [{oid_col}], [{key_col}] FROM dbo.[{dim_table}]"
    engine = get_engine()
    df = pd.read_sql(query, engine)
    return df


def extract_date_keys() -> pd.DataFrame:
    """
    Extract DimDate lookup: FullDate → DateKey (YYYYMMDD integer).
    Used to resolve datetime values from source to DimDate surrogate keys.
    """
    query = "SELECT DateKey, FullDate FROM dbo.DimDate"
    engine = get_engine()
    return pd.read_sql(query, engine, parse_dates=["FullDate"])