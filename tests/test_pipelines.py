import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from beam_pipeline_customers import transform_customer
from beam_pipeline_orders import transform_order
from dag_utils import env_replace


class TestTransformCustomer:
    def test_valid_row(self):
        result = transform_customer("C1,John Doe,john@example.com")
        assert result == {
            "customer_id": "C1",
            "name": "John Doe",
            "email": "john@example.com",
        }

    def test_too_few_fields_returns_none(self):
        assert transform_customer("C1,John Doe") is None


class TestTransformOrder:
    def test_valid_row(self):
        result = transform_order("O1,C1,100.0,2024-01-01")
        assert result == {
            "order_id": "O1",
            "customer_id": "C1",
            "amount": 100.0,
            "order_date": "2024-01-01",
        }

    def test_non_numeric_amount_returns_none(self):
        assert transform_order("O3,C3,N/A,2024-03-01") is None


class TestEnvReplace:
    def test_replaces_known_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_PROJECT", "my-gcp-project")
        result = env_replace("~ENV_VAR:MY_PROJECT~")
        assert result == "my-gcp-project"

    def test_missing_env_var_returns_placeholder(self):
        result = env_replace("~ENV_VAR:DOES_NOT_EXIST_XYZ~")
        assert result == "<DOES_NOT_EXIST_XYZ-not-set>"