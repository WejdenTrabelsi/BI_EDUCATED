"""
etl/pipeline.py
---------------
Orchestrator for the full Python ETL pipeline.
Runs the two pivoted fact tables that couldn't be loaded via SQL:
  - FactStudentServiceRevenue  (unpivot months 1-12)
  - FactStudentServiceTranche  (unpivot TR1/TR2/Tranche cols)

Usage:
    python -m etl.pipeline                  # run full pipeline
    python -m etl.pipeline --table revenue  # run only revenue
    python -m etl.pipeline --table tranche  # run only tranche
"""
import sys
import time
import argparse
from loguru import logger
from utils.logger import setup_logger
from db.connection import test_connection
from etl.extractors.source import (
    extract_student_service_report,
    extract_student_service_report_tranche,
)
from etl.transformers.service_revenue import unpivot_service_revenue
from etl.transformers.service_tranche  import unpivot_service_tranche
from etl.loaders.warehouse import (
    load_fact_student_service_revenue,
    load_fact_student_service_tranche,
)


def run_revenue_pipeline() -> None:
    """Extract → Transform → Load for FactStudentServiceRevenue."""
    logger.info("━━━ Pipeline: FactStudentServiceRevenue ━━━")
    t0 = time.perf_counter()
    raw  = extract_student_service_report()
    tall = unpivot_service_revenue(raw)
    load_fact_student_service_revenue(tall)
    logger.info(f"  Completed in {time.perf_counter() - t0:.1f}s")


def run_tranche_pipeline() -> None:
    """Extract → Load for FactStudentServiceTranche (no transform needed — source is already one row per line)."""
    logger.info("━━━ Pipeline: FactStudentServiceTranche ━━━")
    t0 = time.perf_counter()
    raw = extract_student_service_report_tranche()
    load_fact_student_service_tranche(raw)   # direct load, no unpivot
    logger.info(f"  Completed in {time.perf_counter() - t0:.1f}s")


def run_all() -> None:
    """Run the complete Python ETL pipeline."""
    setup_logger(log_file="logs/etl.log")
    logger.info("══════════════════════════════════════════")
    logger.info("  Education BI — Python ETL Pipeline")
    logger.info("══════════════════════════════════════════")

    # Fail fast if DB is unreachable
    if not test_connection():
        logger.error("Cannot connect to database. Aborting.")
        sys.exit(1)

    total_start = time.perf_counter()
    run_revenue_pipeline()
    run_tranche_pipeline()

    logger.info("══════════════════════════════════════════")
    logger.success(f"  ETL complete in {time.perf_counter() - total_start:.1f}s")
    logger.info("══════════════════════════════════════════")


def main() -> None:
    parser = argparse.ArgumentParser(description="Education BI ETL Pipeline")
    parser.add_argument(
        "--table",
        choices=["revenue", "tranche", "all"],
        default="all",
        help="Which pipeline to run (default: all)"
    )
    args = parser.parse_args()

    setup_logger(log_file="logs/etl.log")

    if not test_connection():
        logger.error("Cannot connect to database. Aborting.")
        sys.exit(1)

    if args.table == "revenue":
        run_revenue_pipeline()
    elif args.table == "tranche":
        run_tranche_pipeline()
    else:
        run_all()


if __name__ == "__main__":
    main()