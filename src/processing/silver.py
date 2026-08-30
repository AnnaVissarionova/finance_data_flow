import logging

import pandas as pd

from src.config import settings
from src.storage.minio import (
    download_json,
    list_objects,
    upload_parquet,
)

from src.utils.validators import (
    validate_record,
)


logger = logging.getLogger(__name__)


# ============================================================
# Bronze
# ============================================================

FRANKFURTER_PREFIX = (
    "frankfurter/rates/"
)

YFINANCE_PREFIX = (
    "yfinance/market/"
)


# ============================================================
# Silver
# ============================================================

SILVER_PREFIX = (
    "exchange_rates_and_market/"
)

SILVER_COLUMNS = [
    "date",
    "asset_type",
    "instrument",
    "base",
    "quote",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "rate",
    "return",
    "source",
]


# ============================================================
# Bronze loading
# ============================================================


def get_bronze_objects(
    bucket: str,
    prefix: str,
) -> list[str]:
    """
    Find JSON Bronze objects under a prefix.
    """

    objects = list_objects(
        bucket=bucket,
        prefix=prefix,
    )

    json_objects = [
        object_key
        for object_key in objects
        if object_key.endswith(".json")
    ]

    logger.info(
        "Found %d Bronze objects under '%s'",
        len(json_objects),
        prefix,
    )

    return json_objects


def load_bronze_records(
    bucket: str,
    object_keys: list[str],
) -> list[dict]:
    """
    Download Bronze JSON records.
    """

    records = []

    for object_key in object_keys:

        logger.info(
            "Reading Bronze object: %s",
            object_key,
        )

        try:
            record = download_json(
                bucket=bucket,
                object_key=object_key,
            )

            records.append(record)

        except Exception:
            logger.exception(
                "Failed to read Bronze object: %s",
                object_key,
            )

    logger.info(
        "Loaded %d Bronze records",
        len(records),
    )

    return records


# ============================================================
# Frankfurter
# ============================================================


def transform_frankfurter(
    records: list[dict],
) -> pd.DataFrame:
    """
    Validate and normalize Frankfurter Bronze records.
    """

    if not records:
        return pd.DataFrame()

    valid_records = []

    for record in records:

        try:
            validate_record(
                record,
            )

        except ValueError as exc:

            logger.warning(
                "Invalid Frankfurter record: %s",
                exc,
            )

            continue

        valid_records.append(
            record,
        )

    if not valid_records:
        logger.warning(
            "No valid Frankfurter records found",
        )

        return pd.DataFrame()

    df = pd.DataFrame(
        valid_records,
    )

    required_columns = [
        "date",
        "base",
        "quote",
        "rate",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Frankfurter data is missing columns: "
            f"{missing_columns}"
        )

    df = df[
        required_columns
    ].copy()

    # --------------------------------------------------------
    # Types
    # --------------------------------------------------------

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
    )

    df["rate"] = pd.to_numeric(
        df["rate"],
        errors="coerce",
    )

    df["base"] = (
        df["base"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    df["quote"] = (
        df["quote"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    # --------------------------------------------------------
    # Required values
    # --------------------------------------------------------

    before = len(df)

    df = df.dropna(
        subset=[
            "date",
            "rate",
            "base",
            "quote",
        ],
    )

    logger.info(
        "Frankfurter null cleanup: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Logical validation
    # --------------------------------------------------------

    before = len(df)

    df = df[
        df["rate"] > 0
    ]

    logger.info(
        "Frankfurter positive-rate validation: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Add unified schema
    # --------------------------------------------------------

    df["asset_type"] = "FX"

    df["instrument"] = (
        df["base"]
        + "/"
        + df["quote"]
    )

    df["open"] = pd.NA
    df["high"] = pd.NA
    df["low"] = pd.NA
    df["close"] = pd.NA
    df["adj_close"] = pd.NA
    df["volume"] = pd.NA

    df["return"] = pd.NA

    df["source"] = "frankfurter"

    return df[
        SILVER_COLUMNS
    ]


# ============================================================
# yfinance
# ============================================================


def transform_yfinance(
    records: list[dict],
) -> pd.DataFrame:
    """
    Validate and normalize yfinance Bronze records.
    """

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(
        records,
    )

    required_columns = [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "yfinance data is missing columns: "
            f"{missing_columns}"
        )

    df = df[
        required_columns
    ].copy()

    # --------------------------------------------------------
    # Types
    # --------------------------------------------------------

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
    )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    df["ticker"] = (
        df["ticker"]
        .astype("string")
        .str.strip()
    )

    # --------------------------------------------------------
    # Required values
    # --------------------------------------------------------

    before = len(df)

    df = df.dropna(
        subset=[
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
        ],
    )

    logger.info(
        "yfinance null cleanup: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Positive prices
    # --------------------------------------------------------

    before = len(df)

    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["adj_close"] > 0)
    ]

    logger.info(
        "yfinance positive-price validation: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # OHLC logical validation
    # --------------------------------------------------------

    before = len(df)

    df = df[
        (df["high"] >= df["low"])
        & (df["high"] >= df["open"])
        & (df["high"] >= df["close"])
        & (df["low"] <= df["open"])
        & (df["low"] <= df["close"])
    ]

    logger.info(
        "yfinance OHLC validation: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Volume
    # --------------------------------------------------------

    # Volume may legitimately be zero or NULL.
    # Negative volume is logically invalid.

    before = len(df)

    df = df[
        df["volume"].isna()
        | (df["volume"] >= 0)
    ]

    logger.info(
        "yfinance volume validation: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Add unified schema
    # --------------------------------------------------------

    df["asset_type"] = "EQUITY"

    df["instrument"] = df["ticker"]

    df["base"] = pd.NA
    df["quote"] = pd.NA
    df["rate"] = pd.NA

    df["return"] = pd.NA

    df["source"] = "yfinance"

    return df[
        SILVER_COLUMNS
    ]


# ============================================================
# Returns
# ============================================================


def calculate_returns(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate daily returns.

    FX:
        rate / previous rate - 1

    EQUITY:
        adjusted close / previous adjusted close - 1
    """

    if dataframe.empty:
        return dataframe

    df = dataframe.copy()

    # --------------------------------------------------------
    # FX
    # --------------------------------------------------------

    fx_mask = (
        df["asset_type"] == "FX"
    )

    if fx_mask.any():

        fx = (
            df.loc[fx_mask]
            .sort_values(
                [
                    "instrument",
                    "date",
                ],
            )
            .copy()
        )

        fx["return"] = (
            fx.groupby(
                "instrument",
            )["rate"]
            .pct_change()
        )

        df.loc[
            fx.index,
            "return",
        ] = fx["return"]

    # --------------------------------------------------------
    # Equity
    # --------------------------------------------------------

    equity_mask = (
        df["asset_type"] == "EQUITY"
    )

    if equity_mask.any():

        equity = (
            df.loc[equity_mask]
            .sort_values(
                [
                    "instrument",
                    "date",
                ],
            )
            .copy()
        )

        equity["return"] = (
            equity.groupby(
                "instrument",
            )["adj_close"]
            .pct_change()
        )

        df.loc[
            equity.index,
            "return",
        ] = equity["return"]

    return df


# ============================================================
# Unified transformation
# ============================================================


def transform_to_silver(
    frankfurter_records: list[dict],
    yfinance_records: list[dict],
) -> pd.DataFrame:
    """
    Transform and combine Frankfurter and
    yfinance Bronze data into a unified Silver dataset.
    """

    frankfurter_df = (
        transform_frankfurter(
            frankfurter_records,
        )
    )

    yfinance_df = (
        transform_yfinance(
            yfinance_records,
        )
    )

    dataframes = []

    if not frankfurter_df.empty:
        dataframes.append(
            frankfurter_df,
        )

    if not yfinance_df.empty:
        dataframes.append(
            yfinance_df,
        )

    if not dataframes:
        raise ValueError(
            "No valid Bronze data found",
        )

    df = pd.concat(
        dataframes,
        ignore_index=True,
    )

    # --------------------------------------------------------
    # Normalize date
    # --------------------------------------------------------

    df["date"] = (
        pd.to_datetime(
            df["date"],
        )
        .dt.normalize()
    )

    # --------------------------------------------------------
    # Deduplication
    # --------------------------------------------------------

    before = len(df)

    df = df.drop_duplicates(
        subset=[
            "date",
            "instrument",
            "source",
        ],
        keep="last",
    )

    logger.info(
        "Silver deduplication: "
        "%d -> %d",
        before,
        len(df),
    )

    # --------------------------------------------------------
    # Calculate returns
    # --------------------------------------------------------

    df = calculate_returns(
        df,
    )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    df = df.sort_values(
        by=[
            "date",
            "asset_type",
            "instrument",
        ],
    )

    df = df.reset_index(
        drop=True,
    )

    logger.info(
        "Final Silver records: %d",
        len(df),
    )

    return df[
        SILVER_COLUMNS
    ]


# ============================================================
# Silver storage
# ============================================================


def upload_partitioned_silver(
    dataframe: pd.DataFrame,
) -> None:
    """
    Upload Silver data partitioned by date.
    """

    for partition_date, partition_df in dataframe.groupby(
        dataframe["date"].dt.date
    ):

        year = partition_date.year
        month = partition_date.month
        day = partition_date.day

        object_key = (
            f"{SILVER_PREFIX}"
            f"year={year}/"
            f"month={month:02d}/"
            f"day={day:02d}/"
            "data.parquet"
        )

        logger.info(
            "Writing Silver partition: %s",
            object_key,
        )

        upload_parquet(
            dataframe=partition_df,
            object_key=object_key,
            bucket=settings.minio_silver_bucket,
        )


# ============================================================
# Pipeline
# ============================================================


def process_bronze_objects() -> None:
    """
    Read, validate, clean, normalize and combine
    Frankfurter and yfinance Bronze data.
    """

    logger.info(
        "=============================================="
    )

    logger.info(
        "Starting Bronze -> Silver processing"
    )

    logger.info(
        "=============================================="
    )

    bronze_bucket = (
        settings.minio_bronze_bucket
    )

    # --------------------------------------------------------
    # Frankfurter Bronze
    # --------------------------------------------------------

    frankfurter_objects = (
        get_bronze_objects(
            bucket=bronze_bucket,
            prefix=FRANKFURTER_PREFIX,
        )
    )

    frankfurter_records = (
        load_bronze_records(
            bucket=bronze_bucket,
            object_keys=frankfurter_objects,
        )
    )

    # --------------------------------------------------------
    # yfinance Bronze
    # --------------------------------------------------------

    yfinance_objects = (
        get_bronze_objects(
            bucket=bronze_bucket,
            prefix=YFINANCE_PREFIX,
        )
    )

    yfinance_records = (
        load_bronze_records(
            bucket=bronze_bucket,
            object_keys=yfinance_objects,
        )
    )

    # --------------------------------------------------------
    # Check Bronze data
    # --------------------------------------------------------

    if (
        not frankfurter_records
        and not yfinance_records
    ):
        logger.warning(
            "No Bronze records found",
        )

        return

    logger.info(
        "Frankfurter Bronze records: %d",
        len(frankfurter_records),
    )

    logger.info(
        "yfinance Bronze records: %d",
        len(yfinance_records),
    )

    # --------------------------------------------------------
    # Transform
    # --------------------------------------------------------

    silver = transform_to_silver(
        frankfurter_records=(
            frankfurter_records
        ),
        yfinance_records=(
            yfinance_records
        ),
    )

    if silver.empty:
        logger.warning(
            "Silver dataframe is empty",
        )

        return

    # --------------------------------------------------------
    # Upload
    # --------------------------------------------------------

    upload_partitioned_silver(
        dataframe=silver,
    )

    logger.info(
        "=============================================="
    )

    logger.info(
        "Bronze -> Silver processing completed"
    )

    logger.info(
        "=============================================="
    )
