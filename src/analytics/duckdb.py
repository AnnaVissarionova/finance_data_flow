import logging

import duckdb
import pandas as pd

from src.storage.minio import download_parquet


logger = logging.getLogger(__name__)


GOLD_OBJECT_KEY = (
    "gold/exchange_rates/"
    "weekly_performance.parquet"
)


def create_connection() -> duckdb.DuckDBPyConnection:
    """Create DuckDB in-memory connection."""

    logger.info(
        "Creating DuckDB connection"
    )

    return duckdb.connect(
        database=":memory:"
    )


def load_gold() -> pd.DataFrame:
    """Load Gold dataset from MinIO."""

    logger.info(
        "Loading Gold dataset from MinIO"
    )

    df = download_parquet(
        GOLD_OBJECT_KEY
    )

    logger.info(
        "Loaded %d Gold records",
        len(df),
    )

    return df


def register_gold(
    connection: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
) -> None:
    """Register Gold DataFrame as DuckDB table."""

    connection.register(
        "weekly_performance",
        dataframe,
    )

    logger.info(
        "Registered weekly_performance table"
    )


def run_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
) -> pd.DataFrame:
    """Execute SQL query."""

    logger.info(
        "Executing SQL query"
    )

    return connection.execute(
        query
    ).df()


def get_weekly_performance() -> pd.DataFrame:
    """Return all weekly performance records."""

    connection = create_connection()

    try:
        dataframe = load_gold()

        register_gold(
            connection,
            dataframe,
        )

        query = """
            SELECT
                week_start,
                base,
                quote,
                start_rate,
                end_rate,
                min_rate,
                max_rate,
                avg_rate,
                weekly_return_pct
            FROM weekly_performance
            ORDER BY
                week_start DESC,
                weekly_return_pct DESC
        """

        return run_query(
            connection,
            query,
        )

    finally:
        connection.close()

def create_gold_connection() -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection with Gold
    dataset registered.
    """

    connection = create_connection()

    dataframe = load_gold()

    register_gold(
        connection,
        dataframe,
    )

    return connection



