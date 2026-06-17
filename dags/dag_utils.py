
import os
import logging
import yaml
from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load and return a YAML config file as a dictionary."""
    if not os.path.exists(config_path):
        raise AirflowFailException(f"Config file not found: {config_path}")
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise AirflowFailException(f"Failed to parse config file '{config_path}': {e}")


def env_replace(value: str) -> str:
    """Replace ~ENV_VAR:VAR_NAME~ placeholders with environment variable values."""
    try:
        if value and "~ENV_VAR:" in str(value):
            var_name = value.split("~ENV_VAR:")[1].split("~")[0]
            env_val = os.getenv(var_name, f"<{var_name}-not-set>")
            if env_val == f"<{var_name}-not-set>":
                log.warning("Environment variable '%s' is not set.", var_name)
            return value.replace(f"~ENV_VAR:{var_name}~", env_val)
        return value
    except Exception as e:
        raise AirflowFailException(f"Error substituting environment variable in '{value}': {e}")