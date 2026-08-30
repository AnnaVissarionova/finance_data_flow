from datetime import date, datetime


def validate_date_range(
    start_date: str,
    end_date: str,
) -> None:
    """Validate ingestion date range."""

    try:
        start = date.fromisoformat(
            start_date
        )
        end = date.fromisoformat(
            end_date
        )
    except ValueError as exc:
        raise ValueError(
            "Dates must have format YYYY-MM-DD"
        ) from exc

    if start > end:
        raise ValueError(
            "Start date cannot be after end date"
        )

def validate_currency(currency: str) -> str:
    """Validate and normalize currency code."""

    currency = currency.upper()

    if len(currency) != 3:
        raise ValueError(
            f"Invalid currency code: {currency}"
        )

    if not currency.isalpha():
        raise ValueError(
            f"Invalid currency code: {currency}"
        )

    return currency

def validate_record(data: dict) -> None:
    """Validate a Bronze exchange-rate record."""

    required_fields = {
        "date",
        "base",
        "quote",
        "rate",
    }

    missing_fields = (
        required_fields - data.keys()
    )

    if missing_fields:
        raise ValueError(
            f"Missing required fields: "
            f"{missing_fields}"
        )

    if not isinstance(
        data["rate"],
        (int, float),
    ):
        raise ValueError(
            f"Rate must be numeric: {data}"
        )

    if data["rate"] <= 0:
        raise ValueError(
            f"Rate must be greater than zero: "
            f"{data}"
        )

    if not data["base"]:
        raise ValueError(
            "Base currency cannot be empty"
        )

    if not data["quote"]:
        raise ValueError(
            "Quote currency cannot be empty"
        )

    try:
        datetime.strptime(
            data["date"],
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise ValueError(
            f"Invalid date: {data['date']}"
        ) from exc
