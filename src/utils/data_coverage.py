import logging
from datetime import datetime, timedelta
from collections import defaultdict

from src.storage.minio import list_objects
from src.config import settings

logger = logging.getLogger(__name__)


def get_data_coverage() -> dict:
    """Analyze which data has been loaded."""
    coverage = defaultdict(lambda: {
        "dates": set(),
        "min_date": None,
        "max_date": None,
        "count": 0,
    })

    bronze_objects = list_objects("bronze/frankfurter/rates/")

    for obj in bronze_objects:
        parts = obj.split("/")
        if len(parts) >= 7:
            try:
                year, month, day = int(parts[3]), int(parts[4]), int(parts[5])
                date_obj = datetime(year, month, day).date()

                filename = parts[-1]
                if filename.endswith(".json"):
                    pair = filename.replace(".json", "")
                    base, quote = pair.split("_")

                    key = f"{base}/{quote}"
                    coverage[key]["dates"].add(date_obj)
                    coverage[key]["count"] += 1

                    if coverage[key]["min_date"] is None or date_obj < coverage[key]["min_date"]:
                        coverage[key]["min_date"] = date_obj

                    if coverage[key]["max_date"] is None or date_obj > coverage[key]["max_date"]:
                        coverage[key]["max_date"] = date_obj

            except (ValueError, IndexError):
                continue

    return dict(coverage)


def print_coverage_report():
    """Print data coverage report."""
    coverage = get_data_coverage()

    print("\n" + "=" * 70)
    print("DATA COVERAGE REPORT")
    print("=" * 70)

    if not coverage:
        print("No data found in Bronze layer")
        return

    total_records = 0
    total_days = 0

    for key, info in sorted(coverage.items()):
        min_date = info["min_date"].strftime("%Y-%m-%d") if info["min_date"] else "None"
        max_date = info["max_date"].strftime("%Y-%m-%d") if info["max_date"] else "None"
        days = (info["max_date"] - info["min_date"]).days + 1 if info["min_date"] else 0

        total_records += info["count"]
        total_days += days

        print(f"\nPair: {key}")
        print(f"  Records: {info['count']}")
        print(f"  Date range: {min_date} -> {max_date}")
        print(f"  Days covered: {days}")

    print("\n" + "-" * 70)
    print(f"Total pairs: {len(coverage)}")
    print(f"Total records: {total_records}")
    print(f"Total days covered: {total_days}")
    print("=" * 70 + "\n")


def get_missing_dates_report(
        start_date: str,
        end_date: str,
) -> dict:
    """Get missing dates for all pairs."""
    coverage = get_data_coverage()

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    all_dates = set()
    current = start
    while current <= end:
        all_dates.add(current)
        current += timedelta(days=1)

    missing_report = {}

    for pair in settings.currency_pair_list:
        key = f"{pair[0]}/{pair[1]}"

        if key in coverage:
            existing = coverage[key]["dates"]
            missing = all_dates - existing
        else:
            missing = all_dates

        missing_report[key] = sorted(missing)

    return missing_report