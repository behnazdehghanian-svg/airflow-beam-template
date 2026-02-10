import yaml
import os

# Path to your config file
CONFIG_FILE = "config/pipeline_config.yaml"

# Load YAML
with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

# Helper to replace env vars
def env_replace(value: str):
    if not value or "~ENV_VAR:" not in str(value):
        return value
    var_name = value.split("~ENV_VAR:")[1].split("~")[0]
    env_val = os.getenv(var_name, f"<{var_name}-not-set>")
    return value.replace(f"~ENV_VAR:{var_name}~", env_val)

# Test reading config
dag_id = env_replace(config.get("dag", {}).get("id"))
project_id = env_replace(config.get("project", {}).get("id"))
input_path = config.get("gcs", {}).get("input_path")

print("DAG ID:", dag_id)
print("Project ID:", project_id)
print("GCS Input Path:", input_path)
