from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from dag_utils import env_replace, load_config

# Load config
CONFIG_FILE = "/home/airflow/gcs/dags/config/pipeline_config.yaml"
config = load_config(CONFIG_FILE)

DAG_ID           = env_replace(config["dag"].get("customers_dag_id", "beam_pipeline_customers"))
PROJECT_ID       = env_replace(config["project"].get("id", "<GCP_PROJECT_ID-not-set>"))
SOURCE_BUCKET    = env_replace(config["gcs"].get("source_bucket", "your-bucket"))
SOURCE_FOLDER    = config["gcs"].get("source_folder", "input")
BQ_DATASET       = env_replace(config["bigquery"].get("dataset", "your_dataset"))
PYTHON_SCRIPT    = env_replace(config["beam"].get("customers_python_file", "/home/airflow/gcs/dags/beam_pipeline_customers.py"))
TEMP_LOCATION    = env_replace(config["beam"].get("temp_location", "/tmp"))
STAGING_LOCATION = env_replace(config["beam"].get("staging_location", "/tmp/staging"))
PY_VERSION       = config["beam"].get("python_version", "python3")
RUNNER           = config["beam"].get("runner", "DirectRunner")
SCHEDULE         = config["dag"].get("schedule_interval", None)
TAGS             = config["dag"].get("tags", ["beam", "etl"])
RETRIES          = int(config["dag"].get("retries", 3))
RETRY_DELAY_MIN  = int(config["dag"].get("retry_delay", 1))
EXECUTION_TS     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

CUSTOMERS_TABLE = {
    "bq_table":    "customers",
    "bq_dataset":  BQ_DATASET,
    "source_file": "customers.csv",
    "schema":      "customer_id:STRING,name:STRING,email:STRING",
}

default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "email_on_failure": True,
    "retries":          RETRIES,
    "retry_delay":      timedelta(minutes=RETRY_DELAY_MIN),
}

with DAG(
    dag_id=DAG_ID,
    description="Beam ETL pipeline: load customers CSV into BigQuery.",
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
    default_args=default_args,
) as dag:

    load_customers = BeamRunPythonPipelineOperator(
        task_id=f"beam_load_{CUSTOMERS_TABLE['bq_table']}",
        runner=RUNNER,
        py_file=PYTHON_SCRIPT,
        py_interpreter=PY_VERSION,
        py_system_site_packages=True,
        pipeline_options={
            "project":          PROJECT_ID,
            "input_path":       f"gs://{SOURCE_BUCKET}/{SOURCE_FOLDER}/{CUSTOMERS_TABLE['source_file']}",
            "bq_dataset":       CUSTOMERS_TABLE["bq_dataset"],
            "bq_table":         CUSTOMERS_TABLE["bq_table"],
            "bq_schema":        CUSTOMERS_TABLE["schema"],
            "temp_location":    TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
            "execution_ts":     EXECUTION_TS,
        },
    )