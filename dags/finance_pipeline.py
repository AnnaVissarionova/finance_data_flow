from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from src.ingestion.frankfurter import ingest
from src.ingestion.yfinance import ingest as ingest_yfinance

from src.processing.dbt_export import (
    export_silver_to_minio,
    export_gold_to_minio,
)

from airflow.models import Variable

# Получаем конфигурацию
config = Variable.get(
    "ingestion_config",
    deserialize_json=True,
    default_var={"date_from": "2026-08-17", "date_to": "2026-08-25"}
)

date_from = config["date_from"]
date_to = config["date_to"]


def run_frankfurter_ingestion(**context):
    actual_date_from = context.get('params', {}).get('date_from', date_from)
    actual_date_to = context.get('params', {}).get('date_to', date_to)
    ingest(actual_date_from, actual_date_to)


def run_yfinance_ingestion(**context):
    actual_date_from = context.get('params', {}).get('date_from', date_from)
    actual_date_to = context.get('params', {}).get('date_to', date_to)
    ingest_yfinance(actual_date_from, actual_date_to)


with DAG(
        dag_id="finance_data_pipeline",
        start_date=datetime(2026, 8, 1),
        schedule=None,
        catchup=False,
        tags=["finance", "etl", "incremental"],
        params={
            "date_from": date_from,
            "date_to": date_to,
        },
) as dag:
    frankfurter_ingestion = PythonOperator(
        task_id="frankfurter_ingestion",
        python_callable=run_frankfurter_ingestion,
    )

    yfinance_ingestion = PythonOperator(
        task_id="yfinance_ingestion",
        python_callable=run_yfinance_ingestion,
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="""
            cd /opt/airflow/dbt

            dbt run \
                --select +silver_finance \
                --vars '{"date_from": "{{ params.date_from }}", "date_to": "{{ params.date_to }}"}'
        """,
        params={
            "date_from": date_from,
            "date_to": date_to,
        },
    )

    export_silver = PythonOperator(
        task_id="export_silver",
        python_callable=export_silver_to_minio,
    )

    # ИСПРАВЛЕНО: Передаем переменные и в dbt_gold
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="""
            cd /opt/airflow/dbt

            dbt run \
                --select +gold_market_data \
                --vars '{"date_from": "{{ params.date_from }}", "date_to": "{{ params.date_to }}"}'
        """,
        params={
            "date_from": date_from,
            "date_to": date_to,
        },
    )

    export_gold = PythonOperator(
        task_id="export_gold",
        python_callable=export_gold_to_minio,
    )

    # Определяем зависимости
    frankfurter_ingestion >> dbt_silver
    yfinance_ingestion >> dbt_silver
    dbt_silver >> export_silver
    export_silver >> dbt_gold
    dbt_gold >> export_gold