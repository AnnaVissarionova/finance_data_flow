import logging
import uuid
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import settings
from src.storage.minio import (
    list_objects,
    upload_json,
)
from src.utils.validators import (
    validate_currency,
    validate_date_range,
)


logger = logging.getLogger(__name__)


START_DATE = "2026-08-24"
END_DATE = "2026-08-29"

BRONZE_PREFIX = (
    "frankfurter/"
    "rates/"
)


def create_session() -> requests.Session:
    """Create HTTP session with retry configuration."""

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    return session


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


def build_object_key(
    date: str,
    base_currency: str,
    quote_currency: str,
) -> str:
    """
    Build deterministic Bronze object key.
    """

    year = date[:4]
    month = date[5:7]
    day = date[8:10]

    return (
        f"{BRONZE_PREFIX}"
        f"{year}/"
        f"{month}/"
        f"{day}/"
        f"{base_currency}_{quote_currency}.json"
    )


def fetch_exchange_rates(
    session: requests.Session,
    base_currency: str,
    quote_currency: str,
    date: str,
    timeout: int = 30,
) -> list[dict]:
    """
    Fetch exchange rates for a single date.
    """

    params = {
        "from": date,
        "to": date,
        "base": base_currency,
        "quotes": quote_currency,
    }

    logger.info(
        "Requesting %s/%s for %s",
        base_currency,
        quote_currency,
        date,
    )

    try:
        response = session.get(
            f"{settings.frankfurter_api_url}/rates",
            params=params,
            timeout=timeout,
        )

        response.raise_for_status()

    except requests.RequestException as exc:
        logger.error(
            "API request failed for %s/%s on %s: %s",
            base_currency,
            quote_currency,
            date,
            exc,
        )
        raise

    try:
        data = response.json()

    except ValueError as exc:
        raise ValueError(
            "Invalid JSON response"
        ) from exc

    if not isinstance(data, list):
        raise ValueError(
            "Expected API response to be a list"
        )

    return data


def enrich_record(data: dict) -> dict:
    """Add ingestion metadata to a record."""

    return {
        **data,
        "source": "frankfurter",
        "ingestion_timestamp": (
            datetime.now(timezone.utc)
            .isoformat()
        ),
        "ingestion_id": str(uuid.uuid4()),
    }


def get_existing_bronze_objects() -> set[str]:
    """
    Load existing Bronze object keys.

    The result is stored in a set to make
    existence checks O(1).
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
        "Existing Bronze objects: %d",
        len(existing_objects),
    )

    return existing_objects


def ingest_pair(
    session: requests.Session,
    base_currency: str,
    quote_currency: str,
    start_date: str,
    end_date: str,
    existing_objects: set[str],
) -> tuple[int, int]:
    """
    Incrementally ingest one currency pair.

    Returns:
        uploaded_count, skipped_count
    """

    dates = generate_dates(
        start_date,
        end_date,
    )

    uploaded = 0
    skipped = 0

    logger.info(
        "Processing %s/%s for %d dates",
        base_currency,
        quote_currency,
        len(dates),
    )

    for date in dates:

        expected_key = build_object_key(
            date=date,
            base_currency=base_currency,
            quote_currency=quote_currency,
        )

        if expected_key in existing_objects:

            logger.info(
                "SKIP existing Bronze object: %s",
                expected_key,
            )

            skipped += 1
            continue

        logger.info(
            "MISSING Bronze object: %s",
            expected_key,
        )

        records = fetch_exchange_rates(
            session=session,
            base_currency=base_currency,
            quote_currency=quote_currency,
            date=date,
        )

        if not records:
            logger.warning(
                "No data returned for %s/%s on %s",
                base_currency,
                quote_currency,
                date,
            )
            continue

        for record in records:

            enriched_record = enrich_record(
                record
            )

            object_key = build_object_key(
                date=enriched_record["date"],
                base_currency=enriched_record["base"],
                quote_currency=enriched_record["quote"],
            )

            if object_key in existing_objects:

                logger.info(
                    "SKIP object returned by API: %s",
                    object_key,
                )

                skipped += 1
                continue

            upload_json(
                data=enriched_record,
                object_key=object_key,
                bucket=settings.minio_bronze_bucket,
            )

            existing_objects.add(
                object_key
            )

            uploaded += 1

    logger.info(
        "Completed %s/%s: uploaded=%d skipped=%d",
        base_currency,
        quote_currency,
        uploaded,
        skipped,
    )

    return uploaded, skipped


def ingest(
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    pairs: list[tuple[str, str]] | None = None,
) -> None:
    """
    Run incremental ingestion for multiple
    currency pairs.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    if pairs is None:
        pairs = settings.currency_pair_list

    logger.info(
        "=============================================="
    )
    logger.info(
        "Starting incremental ingestion"
    )
    logger.info(
        "Period: %s -> %s",
        start_date,
        end_date,
    )
    logger.info(
        "Currency pairs: %d",
        len(pairs),
    )
    logger.info(
        "=============================================="
    )

    session = create_session()

    existing_objects = (
        get_existing_bronze_objects()
    )

    total_uploaded = 0
    total_skipped = 0

    successful = 0
    failed = 0

    for base_currency, quote_currency in pairs:

        try:
            base_currency = validate_currency(
                base_currency
            )

            quote_currency = validate_currency(
                quote_currency
            )

            uploaded, skipped = ingest_pair(
                session=session,
                base_currency=base_currency,
                quote_currency=quote_currency,
                start_date=start_date,
                end_date=end_date,
                existing_objects=existing_objects,
            )

            total_uploaded += uploaded
            total_skipped += skipped

            successful += 1

        except Exception:
            failed += 1

            logger.exception(
                "Ingestion failed for %s/%s",
                base_currency,
                quote_currency,
            )

    logger.info(
        "=============================================="
    )
    logger.info(
        "Incremental ingestion completed"
    )
    logger.info(
        "Successful pairs: %d",
        successful,
    )
    logger.info(
        "Failed pairs: %d",
        failed,
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
