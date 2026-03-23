"""
etl/transformers/service_tranche.py
-------------------------------------
Transforms StudentServiceReportTranche (wide/pivoted) into a tall fact table
ready for FactStudentServiceTranche.

The tranche columns are messy — different school years used different names:
  TR1, TR2, Tranche 1, Tranche 1(22-23), Tranche_1_24_25, ...

Strategy:
  1. Auto-detect all tranche columns by pattern matching
  2. Normalise them to a canonical SliceNumber (1, 2, 3, ...)
  3. melt() wide → tall
  4. Drop NULLs and cast types
"""
import re
import pandas as pd
from loguru import logger
from utils.helpers import safe_cast_decimal


# Patterns that identify a tranche/slice column (case-insensitive)
TRANCHE_PATTERNS = [
    r"^TR\d+$",                    # TR1, TR2
    r"^Tranche[\s_]?\d+",          # Tranche 1, Tranche_1, Tranche 1(22-23)
    r"^Tranche_\d+_\d{2}_\d{2}$",  # Tranche_1_24_25
]


def detect_tranche_columns(df: pd.DataFrame) -> dict[str, int]:
    """
    Scan DataFrame columns and return a mapping of
      original_column_name → normalised_slice_number

    E.g. {"TR1": 1, "Tranche 1": 1, "Tranche 1(22-23)": 1, "TR2": 2}

    When multiple columns map to the same slice number, we keep all of them
    as separate rows (they represent different school year billing — that's fine).
    """
    mapping: dict[str, int] = {}
    for col in df.columns:
        for pattern in TRANCHE_PATTERNS:
            if re.match(pattern, col, re.IGNORECASE):
                # Extract the digit from the column name
                digits = re.findall(r"\d+", col)
                if digits:
                    mapping[col] = int(digits[0])
                break
    logger.debug(f"Tranche columns detected: {mapping}")
    return mapping


def unpivot_service_tranche(df: pd.DataFrame) -> pd.DataFrame:
    """
    Melt the wide tranche DataFrame into one row per student/service/tranche.

    Returns a clean DataFrame with columns:
      SourceOid, StudentOid, ServiceOid, BranchOid, SchoolYearOid,
      ZoneOid, ShuttleTypeOid, SliceNumber, Amount
    """
    if df.empty:
        logger.warning("service_tranche: source DataFrame is empty")
        return pd.DataFrame()

    tranche_map = detect_tranche_columns(df)
    if not tranche_map:
        logger.error("service_tranche: no tranche columns detected in source!")
        return pd.DataFrame()

    tranche_cols = list(tranche_map.keys())

    id_cols = [
        "Oid", "Student", "SchoolService", "Branch",
        "CurrentSchoolYear", "Zone", "ShuttleType"
    ]
    existing_id_cols = [c for c in id_cols if c in df.columns]

    logger.info(f"Unpivoting {len(df)} rows × {len(tranche_cols)} tranche columns "
                f"→ up to {len(df) * len(tranche_cols)} fact rows")

    tall = df[existing_id_cols + tranche_cols].melt(
        id_vars=existing_id_cols,
        value_vars=tranche_cols,
        var_name="TrancheName",
        value_name="Amount",
    )

    # Map original tranche column name → normalised slice number
    tall["SliceNumber"] = tall["TrancheName"].map(tranche_map).astype("Int64")

    # Drop NULLs
    before = len(tall)
    tall = tall.dropna(subset=["Amount"])
    logger.debug(f"  Dropped {before - len(tall)} NULL-amount rows")

    # Clean types
    tall["Amount"] = safe_cast_decimal(tall["Amount"])
    tall = tall.dropna(subset=["Amount"])

    # Drop the raw TrancheName column — SliceNumber is canonical
    tall = tall.drop(columns=["TrancheName"])

    # Standardise column names
    tall = tall.rename(columns={
        "Oid":               "SourceOid",
        "Student":           "StudentOid",
        "SchoolService":     "ServiceOid",
        "Branch":            "BranchOid",
        "CurrentSchoolYear": "SchoolYearOid",
        "Zone":              "ZoneOid",
        "ShuttleType":       "ShuttleTypeOid",
    })

    logger.info(f"  service_tranche transform complete: {len(tall)} rows")
    return tall
