"""
tests/etl/test_transformers.py
-------------------------------
Unit tests for ETL transformers — no DB connection needed.
"""
import pandas as pd
import pytest
from etl.transformers.service_revenue import unpivot_service_revenue
from etl.transformers.service_tranche  import unpivot_service_tranche, detect_tranche_columns


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def revenue_wide():
    """Minimal StudentServiceReport-like wide DataFrame."""
    return pd.DataFrame([
        {"Oid": "AAA", "Student": "S1", "SchoolService": "SVC1",
         "Branch": "B1", "CurrentSchoolYear": "Y1",
         "1": "100,00", "2": "200,00", "3": None},
        {"Oid": "BBB", "Student": "S2", "SchoolService": "SVC1",
         "Branch": "B1", "CurrentSchoolYear": "Y1",
         "1": "150,00", "2": None,     "3": "300,00"},
    ])


@pytest.fixture
def tranche_wide():
    """Minimal StudentServiceReportTranche-like wide DataFrame."""
    return pd.DataFrame([
        {"Oid": "CCC", "Student": "S1", "SchoolService": "SVC2",
         "Branch": "B1", "CurrentSchoolYear": "Y1",
         "Zone": "Z1", "ShuttleType": "ST1",
         "TR1": "500,00", "TR2": "600,00", "Tranche 1(22-23)": "700,00"},
    ])


# ── Revenue transformer tests ─────────────────────────────────────────────────

def test_revenue_unpivot_row_count(revenue_wide):
    result = unpivot_service_revenue(revenue_wide)
    # Row 1: months 1,2 have values (3 is NULL → dropped) = 2 rows
    # Row 2: months 1,3 have values (2 is NULL → dropped) = 2 rows
    assert len(result) == 4


def test_revenue_columns_present(revenue_wide):
    result = unpivot_service_revenue(revenue_wide)
    expected = {"SourceOid", "StudentOid", "ServiceOid", "BranchOid",
                "SchoolYearOid", "MonthNumber", "Amount"}
    assert expected.issubset(set(result.columns))


def test_revenue_comma_decimal_handled(revenue_wide):
    result = unpivot_service_revenue(revenue_wide)
    assert result["Amount"].dtype == float
    assert 100.0 in result["Amount"].values


def test_revenue_empty_input():
    result = unpivot_service_revenue(pd.DataFrame())
    assert result.empty


# ── Tranche transformer tests ─────────────────────────────────────────────────

def test_tranche_column_detection(tranche_wide):
    mapping = detect_tranche_columns(tranche_wide)
    assert "TR1" in mapping
    assert mapping["TR1"] == 1
    assert "TR2" in mapping
    assert mapping["TR2"] == 2
    assert "Tranche 1(22-23)" in mapping
    assert mapping["Tranche 1(22-23)"] == 1


def test_tranche_unpivot_row_count(tranche_wide):
    result = unpivot_service_tranche(tranche_wide)
    # 3 tranche cols, all have values = 3 rows
    assert len(result) == 3


def test_tranche_slice_number_is_int(tranche_wide):
    result = unpivot_service_tranche(tranche_wide)
    assert pd.api.types.is_integer_dtype(result["SliceNumber"])


def test_tranche_empty_input():
    result = unpivot_service_tranche(pd.DataFrame())
    assert result.empty
