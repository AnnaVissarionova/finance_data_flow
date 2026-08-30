import logging

import pandas as pd

from src.config import settings
from src.storage.minio import (
    download_parquet,
    list_objects,
    upload_parquet,
)


logger = logging.getLogger(__name__)


SILVER_PREFIX = (
    "exchange_rates/"
)


GOLD_PREFIX = (
    "exchange_rates/"
)


def get_silver_objects() -> list[str]:
    """Find all Silver Parquet objects."""

    objects = list_objects(
        prefix=SILVER_PREFIX,
        bucket=settings.minio_silver_bucket,
    )

    parquet_objects = [
        object_key
        for object_key in objects
        if object_key.endswith(".parquet")
    ]

    logger.info(
        "Found %d Silver objects",
        len(parquet_objects),
    )

    return parquet_objects


def load_silver_data(
    object_keys: list[str],
) -> pd.DataFrame:
    """Load all Silver partitions."""

    if not object_keys:
        raise ValueError(
            "No Silver objects found"
        )

    dataframes = []

    for object_key in object_keys:

        logger.info(
            "Reading Silver object: %s",
            object_key,
        )

        df = download_parquet(
            object_key=object_key,
            bucket=settings.minio_silver_bucket,
        )

        dataframes.append(df)

    result = pd.concat(
        dataframes,
        ignore_index=True,
    )

    logger.info(
        "Loaded %d Silver records",
        len(result),
    )

    return result


def transform_to_gold(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create weekly exchange-rate performance.
    """

    if df.empty:
        raise ValueError(
            "Silver dataframe is empty"
        )

    df = df.copy()

    # Ensure correct types
    df["date"] = pd.to_datetime(
        df["date"]
    )

    df["rate"] = pd.to_numeric(
        df["rate"]
    )

    # Sort before calculating first/last rate
    df = df.sort_values(
        by=[
            "base",
            "quote",
            "date",
        ]
    )

    # Monday as beginning of week
    df["week_start"] = (
        df["date"]
        - pd.to_timedelta(
            df["date"].dt.weekday,
            unit="D",
        )
    ).dt.normalize()

    # Aggregate weekly metrics
    grouped = (
        df.groupby(
            [
                "week_start",
                "base",
                "quote",
            ]
        )
    )

    gold = grouped["rate"].agg(
        start_rate="first",
        end_rate="last",
        min_rate="min",
        max_rate="max",
        avg_rate="mean",
    ).reset_index()

    # Weekly performance
    gold["weekly_return_pct"] = (
        (
            gold["end_rate"]
            / gold["start_rate"]
        )
        - 1
    ) * 100

    # Round numerical values
    gold["start_rate"] = (
        gold["start_rate"].round(6)
    )

    gold["end_rate"] = (
        gold["end_rate"].round(6)
    )

    gold["min_rate"] = (
        gold["min_rate"].round(6)
    )

    gold["max_rate"] = (
        gold["max_rate"].round(6)
    )

    gold["avg_rate"] = (
        gold["avg_rate"].round(6)
    )

    gold["weekly_return_pct"] = (
        gold["weekly_return_pct"]
        .round(4)
    )

    gold = gold.sort_values(
        by=[
            "week_start",
            "weekly_return_pct",
        ],
        ascending=[
            True,
            False,
        ],
    )

    gold = gold.reset_index(
        drop=True
    )

    logger.info(
        "Created %d Gold records",
        len(gold),
    )

    return gold


def upload_gold(
    dataframe: pd.DataFrame,
) -> None:
    """Upload Gold dataset."""

    object_key = (
        f"{GOLD_PREFIX}"
        "weekly_performance.parquet"
    )

    logger.info(
        "Writing Gold dataset: %s",
        object_key,
    )

    upload_parquet(
        dataframe=dataframe,
        object_key=object_key,
        bucket=settings.minio_gold_bucket,
    )


def process_silver_to_gold() -> None:
    """Run Silver → Gold pipeline."""

    logger.info(
        "Starting Silver → Gold processing"
    )

    silver_objects = (
        get_silver_objects()
    )

    df = load_silver_data(
        silver_objects
    )

    gold = transform_to_gold(
        df
    )

    if gold.empty:
        logger.warning(
            "Gold dataframe is empty"
        )
        return

    upload_gold(
        gold
    )

    logger.info(
        "Silver → Gold processing completed"
    )
