from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from datetime import datetime, timedelta
import os
import yaml

# -------------------------
# Helper function
# -------------------------
def env_replace(value):
    """Replace placeholders ~ENV_VAR:VAR_NAME~ with actual environment variable values"""
    try:
        if value and "~ENV_VAR:" in value:
            var_name = value.split("~ENV_VAR:")[1].split("~")[0]
            env_val = os.getenv(var_name, f"<{var_name}-not-set>")
            return value.replace(f"~ENV_VAR:{var_name}~", env_val)
        return value
    except Exception as e:
        raise AirflowFailException(f"Error replacing environment variable: {e}")

# -------------------------
# Load config
# -------------------------
CONFIG_FILE = "/home/airflow/gcs/dags/config/beam_pipeline_config.yaml"
with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

# -------------------------
# DAG variables from config
# -------------------------
DAG_ID = env_replace(config.get("dag", {}).get("id", "beam_pipeline_template_customers"))
PROJECT_ID = env_replace(config.get("project", {}).get("id", "<GCP_PROJECT_ID-not-set>"))
SOURCE_BUCKET = env_replace(config.get("gcs", {}).get("source_bucket", "example-bucket"))
SOURCE_FOLDER = env_replace(config.get("gcs", {}).get("source_folder", "input"))
PYTHON_SCRIPT_PATH = env_replace(config.get("beam", {}).get("python_file", "/home/airflow/gcs/dags/beam_pipeline.py"))
TEMP_LOCATION = env_replace(config.get("beam", {}).get("temp_location", "/tmp"))
STAGING_LOCATION = env_replace(config.get("beam", {}).get("staging_location", "/tmp/staging"))
PY_VERSION = env_replace(config.get("beam", {}).get("python_version", "python3"))
SCHEDULE_INTERVAL = config.get("dag", {}).get("schedule_interval", None)
DAG_DESCRIPTION = "Beam ETL DAG for Customers Table"
DAG_TAGS = config.get("dag", {}).get("tags", ["beam", "etl"])
DAG_RETRIES = int(config.get("dag", {}).get("retries", 3))
DAG_RETRY_DELAY_MINUTES = int(config.get("dag", {}).get("retry_delay", 1))
DAG_EXEC_DATETIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# -------------------------
# Table info
# -------------------------
CUSTOMERS_TABLE = {
    "bq_table": "customers",
    "bq_dataset": "your_dataset_name",  # Replace if different
    "source_file": "customers.csv",
    "schema": "first_name:STRING,last_name:STRING,cellphone:STRING,full_name:STRING"
}

# -------------------------
# Default DAG args
# -------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": DAG_RETRIES,
    "retry_delay": timedelta(minutes=DAG_RETRY_DELAY_MINUTES),
}

# -------------------------
# DAG definition
# -------------------------
with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=datetime(2023, 1, 1),
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS,
    default_args=default_args,
) as dag:

    run_beam_pipeline = BeamRunPythonPipelineOperator(
        task_id=f"beam_load_{CUSTOMERS_TABLE['bq_table']}",
        runner="DirectRunner",  # Change to "DataflowRunner" for GCP
        py_file=PYTHON_SCRIPT_PATH,
        py_interpreter=PY_VERSION,
        py_system_site_packages=True,
        pipeline_options={
            "project": PROJECT_ID,
            "input_path": f"gs://{SOURCE_BUCKET}/{SOURCE_FOLDER}/{CUSTOMERS_TABLE['source_file']}",
            "bq_dataset": CUSTOMERS_TABLE['bq_dataset'],
            "bq_table": CUSTOMERS_TABLE['bq_table'],
            "bq_schema": CUSTOMERS_TABLE['schema'],
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
            "execution_ts": DAG_EXEC_DATETIME
        },
    )

