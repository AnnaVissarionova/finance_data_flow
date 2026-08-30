import logging
from typing import Optional
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


class PipelineSettings:
    """Управление настройками через Airflow Variables."""

    # Префикс для всех переменных
    PREFIX = "finance_"

    @classmethod
    def get_var(cls, key: str, default: Optional[str] = None) -> str:
        """Получить переменную с префиксом."""
        full_key = f"{cls.PREFIX}{key}"
        try:
            return Variable.get(full_key)
        except AirflowException:
            if default is not None:
                return default
            raise ValueError(f"Airflow Variable '{full_key}' is not set")

    @classmethod
    def get_int(cls, key: str, default: Optional[int] = None) -> int:
        """Получить целочисленную переменную."""
        value = cls.get_var(key, str(default) if default is not None else None)
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Variable '{key}' must be an integer, got: {value}")

    @classmethod
    def get_bool(cls, key: str, default: Optional[bool] = None) -> bool:
        """Получить булеву переменную."""
        value = cls.get_var(key, str(default).lower() if default is not None else None)
        return value.lower() in ("true", "1", "yes", "y", "on")

    @classmethod
    def get_list(cls, key: str, default: Optional[str] = None) -> list[str]:
        """Получить список переменных (разделитель: запятая)."""
        value = cls.get_var(key, default)
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    # Конкретные настройки

    @classmethod
    def get_currency_pairs(cls) -> list[tuple[str, str]]:
        """Получить список валютных пар."""
        pairs_raw = cls.get_var("currency_pairs", "EUR/USD,EUR/GBP,EUR/JPY")
        pairs = []

        for pair in pairs_raw.split(","):
            pair = pair.strip().upper()
            if not pair:
                continue
            try:
                base, quote = pair.split("/")
                pairs.append((base, quote))
            except ValueError:
                logger.warning(f"Invalid currency pair format: {pair}")
                continue

        if not pairs:
            raise ValueError("No valid currency pairs configured")

        return pairs

    @classmethod
    def get_ingestion_period_days(cls) -> int:
        """Количество дней для загрузки (если не указан конкретный период)."""
        return cls.get_int("ingestion_period_days", 1)

    @classmethod
    def get_historical_start_date(cls) -> str:
        """Начальная дата для исторической загрузки."""
        return cls.get_var("historical_start_date", "2023-01-01")

    @classmethod
    def get_historical_end_date(cls) -> str:
        """Конечная дата для исторической загрузки."""
        # По умолчанию - вчера
        default_end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return cls.get_var("historical_end_date", default_end)

    @classmethod
    def get_max_ingestion_days(cls) -> int:
        """Максимальное количество дней за один запрос к API."""
        return cls.get_int("max_ingestion_days", 365)

    @classmethod
    def get_enable_incremental(cls) -> bool:
        """Использовать инкрементальную загрузку."""
        return cls.get_bool("enable_incremental", True)

    @classmethod
    def get_schedule_enabled(cls) -> bool:
        """Включено ли расписание для DAG."""
        return cls.get_bool("schedule_enabled", True)

    @classmethod
    def get_frankfurter_api_url(cls) -> str:
        """URL Frankfurter API."""
        return cls.get_var("frankfurter_api_url", "https://api.frankfurter.dev/v2")

    @classmethod
    def get_timezone(cls) -> str:
        """Часовой пояс."""
        return cls.get_var("timezone", "UTC")


# Функция для установки переменных через Python
def init_default_variables():
    """Инициализировать переменные Airflow значениями по умолчанию."""

    defaults = {
        "finance_currency_pairs": "EUR/USD,EUR/GBP,EUR/JPY,EUR/CHF,GBP/USD,USD/JPY,AUD/USD,USD/CAD",
        "finance_ingestion_period_days": "1",
        "finance_historical_start_date": "2023-01-01",
        "finance_historical_end_date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "finance_max_ingestion_days": "365",
        "finance_enable_incremental": "True",
        "finance_schedule_enabled": "True",
        "finance_frankfurter_api_url": "https://api.frankfurter.dev/v2",
        "finance_timezone": "UTC",
    }

    for key, value in defaults.items():
        try:
            existing = Variable.get(key)
            logger.info(f"Variable {key} already exists: {existing}")
        except AirflowException:
            Variable.set(key, value)
            logger.info(f"Set default variable: {key} = {value}")