"""
utils/logger.py
---------------
Configures loguru for the entire project.
Import setup_logger() once at the entry point (pipeline.py / main.py).
All other modules just do: from loguru import logger
"""
import sys
from pathlib import Path
from loguru import logger
from config.settings import ETLConfig


def setup_logger(log_file: str | None = None) -> None:
    """
    Configure loguru sinks:
      - Console: colored, human-readable
      - File:    JSON-structured, rotating (optional)
    """
    logger.remove()  # remove default handler

    # Console sink
    logger.add(
        sys.stdout,
        level=ETLConfig.LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File sink (optional — pass a path to activate)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_path,
            level="DEBUG",
            format="{time} | {level} | {name}:{function}:{line} | {message}",
            rotation="10 MB",
            retention="30 days",
            compression="zip",
        )
        logger.info(f"File logging → {log_path}")
