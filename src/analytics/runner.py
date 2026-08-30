import logging

from src.analytics.duckdb import create_gold_connection
from src.analytics.queries import (
    get_latest_performance,
    get_weekly_summary,
)
from src.storage.minio import upload_parquet


logger = logging.getLogger(__name__)


LATEST_PERFORMANCE_KEY = (
    "analytics/latest_performance.parquet"
)

WEEKLY_SUMMARY_KEY = (
    "analytics/weekly_summary.parquet"
)


def run_analytics() -> None:
    """
    Run analytics queries and save results
    to MinIO as Parquet files.
    """

    logger.info(
        "========== ANALYTICS START =========="
    )

    connection = create_gold_connection()

    try:
        latest_performance = (
            get_latest_performance(
                connection
            )
        )

        weekly_summary = (
            get_weekly_summary(
                connection
            )
        )

        logger.info(
            "Latest performance: %d records",
            len(latest_performance),
        )

        logger.info(
            "Weekly summary: %d records",
            len(weekly_summary),
        )

        upload_parquet(
            dataframe=latest_performance,
            object_key=LATEST_PERFORMANCE_KEY,
        )

        upload_parquet(
            dataframe=weekly_summary,
            object_key=WEEKLY_SUMMARY_KEY,
        )

    finally:
        connection.close()

    logger.info(
        "========== ANALYTICS COMPLETED =========="
    )
