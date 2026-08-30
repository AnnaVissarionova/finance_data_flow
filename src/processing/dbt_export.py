import logging

import duckdb
import pandas as pd

from src.config import settings
from src.storage.minio import upload_parquet


logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

DBT_DATABASE = "/opt/airflow/dbt/finance.duckdb"

SILVER_MODEL = "silver_finance"
GOLD_MODEL = "gold_market_data"

SILVER_OBJECT_KEY = (
    "daily_returns/data.parquet"
)

GOLD_OBJECT_KEY = (
    "market_data/data.parquet"
)


# ============================================================
# DuckDB
# ============================================================


def get_connection():
    """
    Create a connection to the dbt DuckDB database.
    """

    return duckdb.connect(
        database=DBT_DATABASE,
        read_only=True,
    )


def load_dbt_model(
    model_name: str,
) -> pd.DataFrame:
    """
    Load a dbt model from DuckDB into pandas.
    """

    logger.info(
        "Reading dbt model: %s",
        model_name,
    )

    connection = get_connection()

    try:
        dataframe = connection.execute(
            f'SELECT * FROM "{model_name}"'
        ).df()

    finally:
        connection.close()

    logger.info(
        "Loaded %d rows from %s",
        len(dataframe),
        model_name,
    )

    return dataframe


# ============================================================
# Validation
# ============================================================


def validate_dataframe(
    dataframe: pd.DataFrame,
    model_name: str,
) -> None:
    """
    Basic validation before writing data to MinIO.
    """

    if dataframe.empty:
        raise ValueError(
            f"dbt model '{model_name}' is empty"
        )

    if dataframe.columns.duplicated().any():
        raise ValueError(
            f"dbt model '{model_name}' contains "
            "duplicate columns"
        )

    logger.info(
        "Validated dbt model '%s': %d rows, %d columns",
        model_name,
        len(dataframe),
        len(dataframe.columns),
    )


# ============================================================
# Silver
# ============================================================


def export_silver_to_minio() -> None:
    """
    Export dbt silver_finance table
    from DuckDB to MinIO Silver bucket.
    """

    logger.info(
        "=============================================="
    )

    logger.info(
        "Starting dbt Silver export"
    )

    dataframe = load_dbt_model(
        SILVER_MODEL,
    )

    validate_dataframe(
        dataframe=dataframe,
        model_name=SILVER_MODEL,
    )

    logger.info(
        "Writing Silver dataset to MinIO: %s",
        SILVER_OBJECT_KEY,
    )

    upload_parquet(
        dataframe=dataframe,
        object_key=SILVER_OBJECT_KEY,
        bucket=settings.minio_silver_bucket,
    )

    logger.info(
        "Silver export completed: %d rows",
        len(dataframe),
    )

    logger.info(
        "=============================================="
    )


# ============================================================
# Gold
# ============================================================


def export_gold_to_minio() -> None:
    """
    Export dbt gold_market_data table
    from DuckDB to MinIO Gold bucket.
    """

    logger.info(
        "=============================================="
    )

    logger.info(
        "Starting dbt Gold export"
    )

    dataframe = load_dbt_model(
        GOLD_MODEL,
    )

    validate_dataframe(
        dataframe=dataframe,
        model_name=GOLD_MODEL,
    )

    logger.info(
        "Writing Gold dataset to MinIO: %s",
        GOLD_OBJECT_KEY,
    )

    upload_parquet(
        dataframe=dataframe,
        object_key=GOLD_OBJECT_KEY,
        bucket=settings.minio_gold_bucket,
    )

    logger.info(
        "Gold export completed: %d rows",
        len(dataframe),
    )

    logger.info(
        "=============================================="
    )
