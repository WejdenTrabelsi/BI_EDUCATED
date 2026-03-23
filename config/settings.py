"""
config/settings.py
------------------
Central configuration — reads from .env file.
All other modules import from here. Never hardcode credentials anywhere else.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class DBConfig:
    """SQL Server connection parameters."""
    SERVER:   str = os.getenv("DB_SERVER", "localhost")
    PORT:     str = os.getenv("DB_PORT", "1433")
    NAME:     str = os.getenv("DB_NAME", "educated-demo-db")
    USER:     str = os.getenv("DB_USER", "")
    PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DRIVER:   str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    @classmethod
    def connection_string(cls) -> str:
        """pyodbc-style connection string."""
        base = (
            f"DRIVER={{{cls.DRIVER}}};"
            f"SERVER={cls.SERVER};"
            f"DATABASE={cls.NAME};"
            f"TrustServerCertificate=yes;"
        )
        if cls.USER and cls.PASSWORD:
            return base + f"UID={cls.USER};PWD={cls.PASSWORD};"
        else:
            return base + "Trusted_Connection=yes;"

    @classmethod
    def sqlalchemy_url(cls) -> str:
        """SQLAlchemy connection URL (used for pandas read_sql)."""
        import urllib.parse
        params = urllib.parse.quote_plus(cls.connection_string())
        return f"mssql+pyodbc:///?odbc_connect={params}"


class ETLConfig:
    """ETL runtime settings."""
    BATCH_SIZE: int = int(os.getenv("ETL_BATCH_SIZE", "500"))
    LOG_LEVEL:  str = os.getenv("ETL_LOG_LEVEL", "INFO")


class AppConfig:
    ENV: str = os.getenv("APP_ENV", "development")
    IS_DEV: bool = ENV == "development"