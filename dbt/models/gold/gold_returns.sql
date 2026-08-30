with silver as (

```
select
    date,
    instrument,
    return

from {{ ref('silver_finance') }}
```

),

gold as (

```
select
    date,

    max(
        case
            when instrument = 'EUR/USD'
            then return
        end
    ) as eur_usd,

    max(
        case
            when instrument = 'EUR/GBP'
            then return
        end
    ) as eur_gbp,

    max(
        case
            when instrument = 'EUR/JPY'
            then return
        end
    ) as eur_jpy,

    max(
        case
            when instrument = 'EUR/CHF'
            then return
        end
    ) as eur_chf,

    max(
        case
            when instrument = 'GBP/USD'
            then return
        end
    ) as gbp_usd,

    max(
        case
            when instrument = 'USD/JPY'
            then return
        end
    ) as usd_jpy,

    max(
        case
            when instrument = '^GSPC'
            then return
        end
    ) as gspc,

    max(
        case
            when instrument = '^STOXX50E'
            then return
        end
    ) as stoxx50e,

    max(
        case
            when instrument = '^GDAXI'
            then return
        end
    ) as gdaxi,

    max(
        case
            when instrument = '^FTSE'
            then return
        end
    ) as ftse,

    max(
        case
            when instrument = '^N225'
            then return
        end
    ) as n225,

    max(
        case
            when instrument = '^HSI'
            then return
        end
    ) as hsi

from silver

group by date
```

)

select
date,

```
eur_usd,
eur_gbp,
eur_jpy,
eur_chf,
gbp_usd,
usd_jpy,

gspc,
stoxx50e,
gdaxi,
ftse,
n225,
hsi
```

from gold

order by date
