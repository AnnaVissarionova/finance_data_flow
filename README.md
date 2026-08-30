# Finance Data Platform

An end-to-end **data platform for financial market data**, built around the **Medallion Architecture**.

The platform collects financial data from external sources, stores raw data in object storage, transforms and validates datasets with dbt, serves analytical data through DuckDB, and provides interactive analytics and dashboards through Apache Superset.

---
# Technology Stack

| Area                | Technology             |
| ------------------- | ---------------------- |
| Architecture        | Medallion Architecture |
| Data Platform       | Finance Data Platform  |
| Orchestration       | Apache Airflow         |
| Object Storage      | MinIO                  |
| Transformation      | dbt                    |
| Analytical Database | DuckDB                 |
| Data Formats        | JSON / Parquet         |
| Ingestion           | Python                 |
| BI & Visualization  | Apache Superset        |
| Infrastructure      | Docker                 |

---

## Architecture

<img src="finance data.drawio.png" alt="architecture picture">

---

## Platform Components

### Apache Airflow

Airflow serves as the orchestration layer of the platform.

---
### MinIO

MinIO provides the S3-compatible object storage layer.

Example of Bronze layer storage:

```text
s3://finance-bronze/
├── frankfurter/
│   └── rates/
│       └── YYYY/MM/DD/
│
└── yfinance/
    └── market/
        └── YYYY/MM/DD/
```

Raw data is preserved to provide traceability and reproducibility.

Silver and Gold datasets are stored as Parquet files in MinIO and queried and transformed using DuckDB.

---

### dbt

dbt is responsible for SQL-based data transformations and data modeling.

It transforms raw Bronze data into structured Silver datasets and builds ready-to-use Gold models.

dbt provides:

* modular SQL transformations;
* model dependencies;
* incremental processing;
* data quality tests;

---

### DuckDB

DuckDB is used as the analytical database and query engine.

---

### Apache Superset

Apache Superset provides the **BI and visualization layer** of the platform.

It connects to DuckDB and exposes analytical datasets through:

* interactive dashboards;
* charts;
* SQL exploration;
* data visualization;

---

# Incremental Processing

The platform supports incremental processing of arbitrary historical date ranges.

Airflow passes the processing window to dbt:

```bash
dbt run \
  --select +silver_finance \
  --vars '{"date_from": "2026-08-20", "date_to": "2026-08-25"}'
```

The Silver layer compares incoming records against existing data using composite business keys.

# Data Sources

## Frankfurter

Provides foreign exchange rate data.

The platform stores currency rates using:

```text
date
base
quote
rate
```

## Yahoo Finance

Provides market data for financial instruments.

The platform stores:

```text
date
instrument
open
high
low
close
adj_close
volume
```

---

# Data Quality

The Silver layer applies validation rules before data becomes available for analytical workloads.

For market data:

* valid dates;
* non-null instruments;
* positive prices;
* valid OHLC relationships;
* non-negative volume.

For currency data:

* valid dates;
* valid currency codes;
* valid currency pairs;
* positive exchange rates.

Composite business keys are used to prevent duplicate records during incremental processing.



