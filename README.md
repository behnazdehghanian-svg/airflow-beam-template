# Airflow + Apache Beam ETL Template

A production-ready template for running Apache Beam pipelines orchestrated by Apache Airflow.
Reads CSV files from Google Cloud Storage and loads them into BigQuery.

## Pipeline Flow

GCS (CSV files) → Apache Beam (transform & validate) → BigQuery (tables)

## Project Structure
airflow-beam-template/
├── config/
│   └── pipeline_config.yaml             # All settings in one place
├── dags/
│   ├── dag_utils.py                     # Shared helpers (config loader, env vars)
│   ├── beam_pipeline_customers.py       # Beam ETL logic - customers
│   ├── beam_pipeline_orders.py          # Beam ETL logic - orders
│   ├── beam_pipeline_customers_dag.py   # Airflow DAG - customers
│   └── beam_pipeline_orders_dag.py      # Airflow DAG - orders
├── tests/
│   └── test_pipelines.py
├── test_data/
│   ├── customers.csv
│   └── orders.csv
└── requirements.txt


## Pipelines

- `beam_pipeline_orders.py` — ETL pipeline for the **orders** table
- `beam_pipeline_customers.py` — ETL pipeline for the **customers** table

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set environment variables
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCS_SOURCE_BUCKET="your-bucket-name"
export BQ_DATASET="your-dataset-name"
```

### 3. Run locally (test mode)
```bash
python3 dags/beam_pipeline_orders.py \
  --project=$GCP_PROJECT_ID \
  --input_path=test_data/orders.csv \
  --bq_dataset=$BQ_DATASET \
  --bq_table=orders \
  --bq_schema="order_id:STRING,customer_id:STRING,amount:FLOAT,order_date:STRING" \
  --temp_location=/tmp \
  --staging_location=/tmp/staging \
  --execution_ts="2024-01-01 02:00:00" \
  --local_mode

python3 dags/beam_pipeline_customers.py \
  --project=$GCP_PROJECT_ID \
  --input_path=test_data/customers.csv \
  --bq_dataset=$BQ_DATASET \
  --bq_table=customers \
  --bq_schema="customer_id:STRING,name:STRING,email:STRING" \
  --temp_location=/tmp \
  --staging_location=/tmp/staging \
  --execution_ts="2024-01-01 02:00:00" \
  --local_mode
```

## Switch to Dataflow (GCP Production)

In `config/pipeline_config.yaml` change:
```yaml
beam:
  runner: DataflowRunner
  temp_location: gs://your-bucket/tmp
  staging_location: gs://your-bucket/staging
```

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `GCP_PROJECT_ID` | GCP project ID | `my-gcp-project` |
| `GCS_SOURCE_BUCKET` | GCS bucket name | `my-data-bucket` |
| `BQ_DATASET` | BigQuery dataset name | `my_dataset` |