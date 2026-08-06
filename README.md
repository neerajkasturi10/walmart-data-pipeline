# 🛒 Walmart Data Engineering Pipeline
### End-to-End Data Pipeline with Databricks · dbt · Apache Airflow

---

## 📌 Overview

This project implements a **production-grade, end-to-end data engineering pipeline** for Walmart retail data. Raw transactional data is ingested into **Databricks**, transformed through a **multi-layer dbt medallion architecture** (Bronze → Silver → Gold), and fully orchestrated using **Apache Airflow** running in Docker.

The pipeline follows modern data engineering best practices including Change Data Capture (CDC), layered data transformations, data quality testing, and automated DAG-based orchestration.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│         Walmart Transactional Systems (CDC / Batch Ingest)          │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS (Ingestion Layer)                      │
│   • Databricks Job ingests raw data into the Bronze schema          │
│   • Triggered & monitored by Airflow via the Databricks SDK         │
│   • Writes to: walmart.bronze.*                                     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     dbt (Transformation Layer)                       │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  BRONZE (Source)         walmart.bronze.*                   │   │
│  │  Raw tables: customers, orders, products,                   │   │
│  │              order_items, stores, employees                  │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │  dbt source freshness check           │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  SILVER — Technical      walmart.silver_technical.*         │   │
│  │  Cleaned & standardized tables:                             │   │
│  │  customers_t, orders_t, products_t,                         │   │
│  │  order_items, stores_t, employees_t                         │   │
│  │  + data quality tests                                       │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  SILVER — Business       walmart.silver_business.*          │   │
│  │  One Big Table (OBT): obt_b                                 │   │
│  │  Joins all silver_technical tables into a wide denormalized │   │
│  │  table for downstream analytics                             │   │
│  │  + data quality tests                                       │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  GOLD                    walmart.gold.*                      │   │
│  │  ┌─────────────────────────────────────────────────────┐   │   │
│  │  │ Ephemeral (in-memory CTE staging views):            │   │   │
│  │  │ ephemeral_customers, ephemeral_employees,           │   │   │
│  │  │ ephemeral_orders, ephemeral_products,               │   │   │
│  │  │ ephemeral_stores                                    │   │   │
│  │  └─────────────────────────────────────────────────────┘   │   │
│  │  ┌─────────────────────────────────────────────────────┐   │   │
│  │  │ Fact Tables:   fact_orders                          │   │   │
│  │  │ Dim Snapshots: dim_customers, dim_employees,        │   │   │
│  │  │                dim_order, dim_products, dim_stores   │   │   │
│  │  └─────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│               APACHE AIRFLOW (Orchestration Layer)                   │
│                    Running via Docker Compose                        │
│                                                                     │
│  DAG 1: databricks_orchestration                                    │
│     trigger_databricks_job → check_databricks_job_status            │
│                                       │                             │
│                                       ▼                             │
│  DAG 2: orchestrate (dbt pipeline)                                  │
│     ingest_cdc → clean_target → source_freshness →                  │
│     silver_technical → silver_technical_tests →                     │
│     silver_business → silver_business_tests →                       │
│     gold_ephemeral_layer → gold_dim_tables → gold_fact_tables       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
data_project_dbt/
├── airflow/
│   ├── Dockerfile                  # Custom Airflow image (airflow 3.3.0 + dbt)
│   ├── docker-compose.yaml         # Full Airflow stack (webserver, scheduler, etc.)
│   ├── requirements.txt            # Python dependencies
│   ├── .env                        # Environment variables
│   ├── dags/
│   │   ├── databricks_orchestrate.py   # DAG 1: Databricks ingestion
│   │   └── orchestrate.py              # DAG 2: dbt transformation pipeline
│   └── walmart_project/            # dbt project root
│       ├── dbt_project.yml         # dbt project configuration
│       ├── profiles.yml            # Databricks connection config
│       ├── models/
│       │   ├── source/             # Source freshness definitions
│       │   ├── silver_technical/   # Cleaned/standardized tables
│       │   ├── silver_business/    # One Big Table (OBT) model
│       │   └── gold/
│       │       ├── ephemeral/      # In-memory CTE staging models
│       │       └── fact_tables/    # Final fact table (fact_orders)
│       ├── snapshots/              # SCD Type 2 dimension snapshots
│       │   ├── dim_customers.yml
│       │   ├── dim_employees.yml
│       │   ├── dim_order.yml
│       │   ├── dim_products.yml
│       │   └── dim_stores.yml
│       ├── seeds/                  # Static reference data
│       ├── macros/                 # Reusable Jinja macros
│       ├── analyses/               # Ad-hoc SQL analyses
│       └── tests/                  # Custom dbt data tests
├── main.py
└── pyproject.toml
```

---

## ⚙️ Tech Stack

| Component | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 3.3.0 |
| Transformation | dbt Core + dbt-Databricks | 1.12.x |
| Data Platform | Databricks (SQL Warehouse) | — |
| Containerization | Docker + Docker Compose | — |
| Language | Python | 3.x |
| Data Warehouse | Databricks Unity Catalog | `walmart` catalog |

---

## 🔄 Pipeline Workflow

### Step 1 — Databricks Ingestion (`databricks_orchestration` DAG)

Scheduled: **daily at 06:00 UTC** (`0 6 * * *`)

| Task | Description |
|---|---|
| `trigger_databricks_job` | Calls the Databricks Jobs API to start the ingestion job (Job ID: `686816196398445`) and returns the `run_id` |
| `check_databricks_job_status` | Polls the job every 10 seconds using `life_cycle_state` until the job reaches a terminal state (`TERMINATED`, `SKIPPED`, or `INTERNAL_ERROR`), then verifies `result_state == SUCCESS` |
| `trigger_dbt_workflow` | On success, triggers the `orchestrate` DAG using `TriggerDagRunOperator` and waits for it to complete |

### Step 2 — dbt Transformation Pipeline (`orchestrate` DAG)

Triggered by DAG 1 on successful Databricks job completion.

| Task | dbt Command | Description |
|---|---|---|
| `ingest_cdc` | Python task | Marks CDC data as ingested |
| `clean_target` | `rm -rf target/* logs/*` | Cleans dbt build artifacts |
| `source_freshness` | `dbt source freshness` | Validates Bronze source tables are up-to-date |
| `silver_technical` | `dbt run --select silver_technical` | Runs cleaned/standardized Silver models |
| `silver_technical_tests` | `dbt test --select silver_technical` | Runs data quality tests on Silver Technical layer |
| `silver_business` | `dbt run --select silver_business` | Builds the OBT (One Big Table) |
| `silver_business_tests` | `dbt test --select silver_business` | Runs data quality tests on Silver Business layer |
| `gold_ephemeral_layer` | `dbt run --select gold/ephemeral` | Builds ephemeral CTE staging models |
| `gold_dim_tables` | `dbt snapshot` | Runs SCD Type 2 snapshots for all dimension tables |
| `gold_fact_tables` | `dbt run --select gold/fact_tables` | Builds the final `fact_orders` table |

---

## 🗂️ Data Model

### Source Tables (Bronze Layer — `walmart.bronze`)

Raw CDC data ingested by Databricks:

| Table | Description |
|---|---|
| `customers` | Customer master data |
| `orders` | Order headers |
| `order_items` | Order line items |
| `products` | Product catalog |
| `stores` | Store locations |
| `employees` | Employee records |

### Silver Technical (`walmart.silver_technical`)

Cleaned, standardized, and type-cast versions of each source table. Each model adds:
- `is_active` flag (SCD tracking)
- `created_timestamp`, `updated_timestamp`, `processed_timestamp` audit columns
- Null coalescing for optional fields

### Silver Business (`walmart.silver_business`)

**`obt_b`** — One Big Table built dynamically using a Jinja macro loop. Joins all six silver_technical tables into a single wide, denormalized table for analytics consumption:

```
orders_t
  ├── customers_t     (on customer_id)
  ├── order_items     (on order_id)
  │     └── products_t  (on product_id)
  ├── stores_t        (on store_id)
  └── employees_t     (on store_id)
```

### Gold Layer (`walmart.gold`)

| Model | Type | Description |
|---|---|---|
| `ephemeral_customers` | Ephemeral | In-memory CTE — customer dimension prep |
| `ephemeral_employees` | Ephemeral | In-memory CTE — employee dimension prep |
| `ephemeral_orders` | Ephemeral | In-memory CTE — order dimension prep |
| `ephemeral_products` | Ephemeral | In-memory CTE — product dimension prep |
| `ephemeral_stores` | Ephemeral | In-memory CTE — store dimension prep |
| `fact_orders` | Table | Final fact table with order metrics |
| `dim_customers` | Snapshot (SCD2) | Slowly changing dimension for customers |
| `dim_employees` | Snapshot (SCD2) | Slowly changing dimension for employees |
| `dim_products` | Snapshot (SCD2) | Slowly changing dimension for products |
| `dim_stores` | Snapshot (SCD2) | Slowly changing dimension for stores |
| `dim_order` | Snapshot (SCD2) | Slowly changing dimension for orders |

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose installed
- Access to a Databricks workspace
- Databricks Personal Access Token

### 1. Configure Environment

Update the Databricks credentials in:
- `airflow/walmart_project/profiles.yml` — dbt connection
- `airflow/dags/databricks_orchestrate.py` — Databricks SDK connection

> ⚠️ **Security Note**: Do not commit credentials to version control. Use Airflow Connections or environment variables instead.

### 2. Start Airflow

```bash
cd airflow
docker compose up --build -d
```

Airflow UI will be available at: **http://localhost:8080**

### 3. Trigger the Pipeline

Either:
- **Manually**: Trigger the `databricks_orchestration` DAG from the Airflow UI
- **Automatically**: It runs daily at **06:00 UTC** on its cron schedule

---

## 🔗 DAG Dependencies

```
databricks_orchestration
    │
    ├── trigger_databricks_job
    │       ↓
    ├── check_databricks_job_status  (polls until terminal)
    │       ↓ (on SUCCESS)
    └── TriggerDagRunOperator ──────────► orchestrate
                                              │
                                              ├── ingest_cdc
                                              ├── clean_target
                                              ├── source_freshness
                                              ├── silver_technical
                                              ├── silver_technical_tests
                                              ├── silver_business
                                              ├── silver_business_tests
                                              ├── gold_ephemeral_layer
                                              ├── gold_dim_tables (dbt snapshot)
                                              └── gold_fact_tables
```

---

## 📦 Dependencies

```
apache-airflow>=3.3.0
airflow-operators>=0.11.0
dbt-core>=1.12.0
dbt-databricks>=1.12.3
databricks-sdk
```

---

## 📄 License

This project is for educational and demonstration purposes.
