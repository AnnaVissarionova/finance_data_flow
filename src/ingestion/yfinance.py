import logging
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

from src.config import settings
from src.storage.minio import (
    list_objects,
    upload_json,
)


logger = logging.getLogger(__name__)


START_DATE = "2026-08-24"
END_DATE = "2026-08-30"

BRONZE_PREFIX = (
    "yfinance/"
    "market/"
)


def generate_dates(
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Generate all dates in an inclusive date range.
    """

    start = datetime.strptime(
        start_date,
        "%Y-%m-%d",
    ).date()

    end = datetime.strptime(
        end_date,
        "%Y-%m-%d",
    ).date()

    dates = []

    current = start

    while current <= end:
        dates.append(
            current.isoformat()
        )

        current += timedelta(days=1)

    return dates


def sanitize_ticker(
    ticker: str,
) -> str:
    """
    Convert ticker into a safe object-key component.
    """

    return (
        ticker
        .replace("^", "_")
        .replace("/", "_")
        .replace(" ", "_")
    )


def build_object_key(
    date: str,
    ticker: str,
) -> str:
    """
    Build deterministic Bronze object key.
    """

    year = date[:4]
    month = date[5:7]
    day = date[8:10]

    safe_ticker = sanitize_ticker(
        ticker
    )

    return (
        f"{BRONZE_PREFIX}"
        f"{year}/"
        f"{month}/"
        f"{day}/"
        f"{safe_ticker}.json"
    )


def download_market_data(
    tickers: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Download daily market data for multiple tickers.

    yfinance uses an exclusive end date,
    therefore the caller must provide the day
    after the desired final date.
    """

    logger.info(
        "Downloading yfinance data: "
        "%s -> %s",
        start_date,
        end_date,
    )

    logger.info(
        "Tickers: %s",
        ", ".join(tickers),
    )

    try:
        dataframe = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            actions=False,
            group_by="ticker",
            threads=True,
            multi_level_index=True,
        )

    except Exception:
        logger.exception(
            "yfinance download failed"
        )
        raise

    if dataframe is None or dataframe.empty:
        logger.warning(
            "yfinance returned no data"
        )

        return pd.DataFrame()

    logger.info(
        "Downloaded %d rows",
        len(dataframe),
    )

    return dataframe


def extract_ticker_dataframe(
    dataframe: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """
    Extract one ticker from yfinance MultiIndex DataFrame.
    """

    if not isinstance(
        dataframe.columns,
        pd.MultiIndex,
    ):
        raise ValueError(
            "Expected yfinance MultiIndex columns"
        )

    if ticker not in dataframe.columns.get_level_values(0):
        logger.warning(
            "Ticker %s not found in downloaded data",
            ticker,
        )

        return pd.DataFrame()

    ticker_df = dataframe[ticker].copy()

    ticker_df = ticker_df.reset_index()

    return ticker_df


def normalize_ticker_data(
    dataframe: pd.DataFrame,
    ticker: str,
) -> list[dict]:
    """
    Convert ticker DataFrame into Bronze records.
    """

    if dataframe.empty:
        return []

    required_columns = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns for {ticker}: "
            f"{missing_columns}"
        )

    records = []

    for _, row in dataframe.iterrows():

        if pd.isna(row["Close"]):
            continue

        date = pd.to_datetime(
            row["Date"]
        ).date().isoformat()

        record = {
            "date": date,
            "ticker": ticker,
            "open": (
                None
                if pd.isna(row["Open"])
                else float(row["Open"])
            ),
            "high": (
                None
                if pd.isna(row["High"])
                else float(row["High"])
            ),
            "low": (
                None
                if pd.isna(row["Low"])
                else float(row["Low"])
            ),
            "close": (
                None
                if pd.isna(row["Close"])
                else float(row["Close"])
            ),
            "adj_close": (
                None
                if pd.isna(row["Adj Close"])
                else float(row["Adj Close"])
            ),
            "volume": (
                None
                if pd.isna(row["Volume"])
                else int(row["Volume"])
            ),
            "source": "yfinance",
            "ingestion_timestamp": (
                datetime.now(timezone.utc)
                .isoformat()
            ),
            "ingestion_id": str(
                uuid.uuid4()
            ),
        }

        records.append(record)

    return records


def get_existing_bronze_objects() -> set[str]:
    """
    Load existing yfinance Bronze objects.
    """

    objects = list_objects(
        prefix=BRONZE_PREFIX,
        bucket=settings.minio_bronze_bucket,
    )

    existing_objects = {
        object_key
        for object_key in objects
        if object_key.endswith(".json")
    }

    logger.info(
        "Existing yfinance Bronze objects: %d",
        len(existing_objects),
    )

    return existing_objects


def ingest(
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    tickers: list[str] | None = None,
) -> None:
    """
    Incrementally ingest yfinance market data.

    The end_date is inclusive for this function,
    while yfinance's end parameter is exclusive.
    """

    if tickers is None:
        tickers = settings.yfinance_ticker_list

    logger.info(
        "=============================================="
    )
    logger.info(
        "Starting yfinance incremental ingestion"
    )
    logger.info(
        "Period: %s -> %s",
        start_date,
        end_date,
    )
    logger.info(
        "Tickers: %d",
        len(tickers),
    )
    logger.info(
        "=============================================="
    )

    existing_objects = (
        get_existing_bronze_objects()
    )

    # yfinance end date is exclusive.
    end_exclusive = (
        datetime.strptime(
            end_date,
            "%Y-%m-%d",
        )
        + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    dataframe = download_market_data(
        tickers=tickers,
        start_date=start_date,
        end_date=end_exclusive,
    )

    if dataframe.empty:
        logger.warning(
            "No market data available"
        )
        return

    total_uploaded = 0
    total_skipped = 0

    for ticker in tickers:

        logger.info(
            "Processing ticker: %s",
            ticker,
        )

        ticker_dataframe = (
            extract_ticker_dataframe(
                dataframe=dataframe,
                ticker=ticker,
            )
        )

        records = normalize_ticker_data(
            dataframe=ticker_dataframe,
            ticker=ticker,
        )

        if not records:
            logger.warning(
                "No records for ticker %s",
                ticker,
            )
            continue

        for record in records:

            object_key = build_object_key(
                date=record["date"],
                ticker=record["ticker"],
            )

            if object_key in existing_objects:

                logger.info(
                    "SKIP existing object: %s",
                    object_key,
                )

                total_skipped += 1

                continue

            upload_json(
                data=record,
                object_key=object_key,
                bucket=settings.minio_bronze_bucket,
            )

            existing_objects.add(
                object_key
            )

            total_uploaded += 1

    logger.info(
        "=============================================="
    )
    logger.info(
        "yfinance incremental ingestion completed"
    )
    logger.info(
        "Uploaded: %d",
        total_uploaded,
    )
    logger.info(
        "Skipped: %d",
        total_skipped,
    )
    logger.info(
        "=============================================="
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    ingest()


if __name__ == "__main__":
    main()
