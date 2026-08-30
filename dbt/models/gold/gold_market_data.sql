{{ config(
    materialized='table'
) }}

{% set date_from = var('date_from', '2026-08-17') %}
{% set date_to = var('date_to', '2026-08-25') %}

with source_data as (
    select
        date,
        asset_type,
        instrument,
        case
            when asset_type = 'FX'
                then rate
            when asset_type = 'EQUITY'
                then adj_close
            else null
        end as value
    from {{ ref('silver_finance') }}
    where
        date >= cast('{{ date_from }}' as date)
        and date <= cast('{{ date_to }}' as date)
),

cleaned as (
    select
        date,
        asset_type,
        instrument,
        value
    from source_data
    where value is not null
      and value > 0
),

with_returns as (
    select
        date,
        asset_type,
        instrument,
        value,
        ln(
            value
            / lag(value) over (
                partition by instrument
                order by date
            )
        ) as log_return
    from cleaned
),

with_volatility as (
    select
        date,
        asset_type,
        instrument,
        value,
        log_return,
        power(log_return, 2) as volatility
    from with_returns
),

gold as (
    select
        date,

        -- FX levels
        max(case when instrument = 'EUR/USD' then value end) as eur_usd,
        max(case when instrument = 'EUR/GBP' then value end) as eur_gbp,
        max(case when instrument = 'EUR/JPY' then value end) as eur_jpy,
        max(case when instrument = 'EUR/CHF' then value end) as eur_chf,
        max(case when instrument = 'GBP/USD' then value end) as gbp_usd,
        max(case when instrument = 'USD/JPY' then value end) as usd_jpy,

        -- Equity levels
        max(case when instrument = '^GSPC' then value end) as gspc,
        max(case when instrument = '^STOXX50E' then value end) as stoxx50e,
        max(case when instrument = '^GDAXI' then value end) as gdaxi,
        max(case when instrument = '^FTSE' then value end) as ftse,
        max(case when instrument = '^N225' then value end) as n225,
        max(case when instrument = '^HSI' then value end) as hsi,

        -- FX log returns
        max(case when instrument = 'EUR/USD' then log_return end) as eur_usd_log_return,
        max(case when instrument = 'EUR/GBP' then log_return end) as eur_gbp_log_return,
        max(case when instrument = 'EUR/JPY' then log_return end) as eur_jpy_log_return,
        max(case when instrument = 'EUR/CHF' then log_return end) as eur_chf_log_return,
        max(case when instrument = 'GBP/USD' then log_return end) as gbp_usd_log_return,
        max(case when instrument = 'USD/JPY' then log_return end) as usd_jpy_log_return,

        -- Equity log returns
        max(case when instrument = '^GSPC' then log_return end) as gspc_log_return,
        max(case when instrument = '^STOXX50E' then log_return end) as stoxx50e_log_return,
        max(case when instrument = '^GDAXI' then log_return end) as gdaxi_log_return,
        max(case when instrument = '^FTSE' then log_return end) as ftse_log_return,
        max(case when instrument = '^N225' then log_return end) as n225_log_return,
        max(case when instrument = '^HSI' then log_return end) as hsi_log_return,

        -- FX volatility
        max(case when instrument = 'EUR/USD' then volatility end) as eur_usd_volatility,
        max(case when instrument = 'EUR/GBP' then volatility end) as eur_gbp_volatility,
        max(case when instrument = 'EUR/JPY' then volatility end) as eur_jpy_volatility,
        max(case when instrument = 'EUR/CHF' then volatility end) as eur_chf_volatility,
        max(case when instrument = 'GBP/USD' then volatility end) as gbp_usd_volatility,
        max(case when instrument = 'USD/JPY' then volatility end) as usd_jpy_volatility,

        -- Equity volatility
        max(case when instrument = '^GSPC' then volatility end) as gspc_volatility,
        max(case when instrument = '^STOXX50E' then volatility end) as stoxx50e_volatility,
        max(case when instrument = '^GDAXI' then volatility end) as gdaxi_volatility,
        max(case when instrument = '^FTSE' then volatility end) as ftse_volatility,
        max(case when instrument = '^N225' then volatility end) as n225_volatility,
        max(case when instrument = '^HSI' then volatility end) as hsi_volatility

    from with_volatility
    group by date
)

select *
from gold
order by date