import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable "
            f"'{name}' is not set"
        )

    return value


@dataclass(frozen=True)
class Settings:
    frankfurter_api_url: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

    minio_bronze_bucket: str
    minio_silver_bucket: str
    minio_gold_bucket: str
    minio_analytics_bucket: str

    currency_pairs: str
    yfinance_tickers: str

    @property
    def currency_pair_list(
        self,
    ) -> list[tuple[str, str]]:
        pairs = []

        for pair in self.currency_pairs.split(","):
            pair = pair.strip().upper()

            if not pair:
                continue

            try:
                base, quote = pair.split("/")
            except ValueError as exc:
                raise RuntimeError(
                    f"Invalid currency pair: '{pair}'. "
                    f"Expected format: EUR/USD"
                ) from exc

            pairs.append(
                (base, quote)
            )

        if not pairs:
            raise RuntimeError(
                "CURRENCY_PAIRS must contain "
                "at least one currency pair"
            )

        return pairs

    @property
    def yfinance_ticker_list(
        self,
    ) -> list[str]:
        tickers = []

        for ticker in self.yfinance_tickers.split(","):
            ticker = ticker.strip()

            if not ticker:
                continue

            tickers.append(ticker)

        if not tickers:
            raise RuntimeError(
                "YFINANCE_TICKERS must contain "
                "at least one ticker"
            )

        return tickers


settings = Settings(
    frankfurter_api_url=get_required_env(
        "FRANKFURTER_API_URL"
    ),

    minio_endpoint=get_required_env(
        "MINIO_ENDPOINT"
    ),
    minio_access_key=get_required_env(
        "MINIO_ACCESS_KEY"
    ),
    minio_secret_key=get_required_env(
        "MINIO_SECRET_KEY"
    ),

    minio_bronze_bucket=get_required_env(
        "MINIO_BRONZE_BUCKET"
    ),
    minio_silver_bucket=get_required_env(
        "MINIO_SILVER_BUCKET"
    ),
    minio_gold_bucket=get_required_env(
        "MINIO_GOLD_BUCKET"
    ),
    minio_analytics_bucket=get_required_env(
        "MINIO_ANALYTICS_BUCKET"
    ),

    currency_pairs=get_required_env(
        "CURRENCY_PAIRS"
    ),

    yfinance_tickers=get_required_env(
        "YFINANCE_TICKERS"
    ),
)
