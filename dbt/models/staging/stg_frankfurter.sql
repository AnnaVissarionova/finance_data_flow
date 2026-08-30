{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date', 'base', 'quote']
) }}

select
    cast(date as date) as date,
    upper(trim(base)) as base,
    upper(trim(quote)) as quote,
    cast(rate as double) as rate,
    source,
    ingestion_timestamp,
    ingestion_id

from read_json_auto(
    's3://{{ env_var("MINIO_BRONZE_BUCKET") }}/frankfurter/rates/**/*.json'
)

where
    date is not null
    and base is not null
    and quote is not null
    and rate is not null
    and cast(rate as double) > 0

{% if is_incremental() %}
    and cast(date as date) >= cast('{{ var("date_from") }}' as date)
    and cast(date as date) < cast('{{ var("date_to") }}' as date)
{% endif %}