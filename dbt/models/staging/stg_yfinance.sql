{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date', 'instrument']
) }}

{% set date_from = var('date_from') %}
{% set date_to = var('date_to') %}

with source as (
    select *
    from read_json_auto(
        's3://{{ env_var("MINIO_BRONZE_BUCKET") }}/yfinance/market/**/*.json'
    )
    where
        date is not null
        and ticker is not null
        and open is not null
        and high is not null
        and low is not null
        and close is not null
        and adj_close is not null
        -- Добавляем фильтр по диапазону дат
        and cast(date as date) >= cast('{{ date_from }}' as date)
        and cast(date as date) <= cast('{{ date_to }}' as date)
),

cleaned as (
    select
        cast(date as date) as date,
        trim(ticker) as instrument,
        cast(open as double) as open,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(close as double) as close,
        cast(adj_close as double) as adj_close,
        cast(volume as bigint) as volume,
        source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        ingestion_id
    from source
),

validated as (
    select *
    from cleaned
    where
        date is not null
        and instrument is not null
        -- positive prices
        and open > 0
        and high > 0
        and low > 0
        and close > 0
        and adj_close > 0
        -- logical OHLC validation
        and high >= low
        and high >= open
        and high >= close
        and low <= open
        and low <= close
        -- volume cannot be negative
        and (volume is null or volume >= 0)
)

select *
from validated