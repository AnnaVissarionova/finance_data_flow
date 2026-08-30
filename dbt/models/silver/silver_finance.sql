with fx as (

    select
        date,

        'FX' as asset_type,

        base || '/' || quote as instrument,

        base,
        quote,

        cast(null as double) as open,
        cast(null as double) as high,
        cast(null as double) as low,
        cast(null as double) as close,
        cast(null as double) as adj_close,
        cast(null as bigint) as volume,

        rate,

        source

    from {{ ref('stg_frankfurter') }}

),

equities as (

    select
        date,

        'EQUITY' as asset_type,

        instrument,

        cast(null as varchar) as base,
        cast(null as varchar) as quote,

        open,
        high,
        low,
        close,
        adj_close,
        volume,

        cast(null as double) as rate,

        source

    from {{ ref('stg_yfinance') }}

),

combined as (

    select * from fx

    union all

    select * from equities

),

deduplicated as (

    select *

    from (

        select
            *,

            row_number() over (
                partition by
                    date,
                    instrument,
                    source
                order by date
            ) as row_num

        from combined

    )

    where row_num = 1

),

with_returns as (

    select
        *,

        case

            when asset_type = 'FX'

            then
                (
                    rate
                    / lag(rate) over (
                        partition by instrument
                        order by date
                    )
                ) - 1


            when asset_type = 'EQUITY'

            then
                (
                    adj_close
                    / lag(adj_close) over (
                        partition by instrument
                        order by date
                    )
                ) - 1


            else null

        end as return

    from deduplicated

)

select
    date,
    asset_type,
    instrument,
    base,
    quote,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    rate,
    return,
    source

from with_returns