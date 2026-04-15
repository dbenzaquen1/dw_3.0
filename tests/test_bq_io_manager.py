"""Tests for the BigQuery IO manager helpers."""

import json
import os
import uuid

import pandas as pd
import pytest

from qmi_etl.defs.bq_io_manager import (
    _ensure_bigquery_safe_dataframe,
    _load_service_account_credentials,
    _sanitize_bigquery_column_names,
    _try_convert_time_column,
)

# ---------------------------------------------------------------------------
# _sanitize_bigquery_column_names
# ---------------------------------------------------------------------------

class TestSanitizeBigQueryColumnNames:
    def test_clean_names_unchanged(self):
        df = pd.DataFrame({"foo": [1], "bar_baz": [2]})
        result = _sanitize_bigquery_column_names(df)
        assert list(result.columns) == ["foo", "bar_baz"]

    def test_special_chars_replaced(self):
        df = pd.DataFrame({"col name!": [1], "col@2#": [2]})
        result = _sanitize_bigquery_column_names(df)
        assert list(result.columns) == ["col_name", "col_2"]

    def test_consecutive_underscores_collapsed(self):
        df = pd.DataFrame({"a---b": [1]})
        result = _sanitize_bigquery_column_names(df)
        assert list(result.columns) == ["a_b"]

    def test_leading_trailing_underscores_stripped(self):
        df = pd.DataFrame({" x ": [1]})
        result = _sanitize_bigquery_column_names(df)
        assert list(result.columns) == ["x"]

    def test_empty_column_becomes_unnamed(self):
        df = pd.DataFrame({" ": [1]})
        result = _sanitize_bigquery_column_names(df)
        assert list(result.columns) == ["unnamed"]

    def test_duplicate_names_get_suffix(self):
        df = pd.DataFrame({"a!": [1], "a@": [2]})
        result = _sanitize_bigquery_column_names(df)
        assert result.columns[0] == "a"
        assert result.columns[1] == "a_1"

    def test_original_dataframe_not_mutated(self):
        df = pd.DataFrame({"col name": [1]})
        original_cols = list(df.columns)
        _sanitize_bigquery_column_names(df)
        assert list(df.columns) == original_cols


# ---------------------------------------------------------------------------
# _ensure_bigquery_safe_dataframe
# ---------------------------------------------------------------------------

class TestTryConvertTimeColumn:
    def test_valid_time_strings(self):
        from datetime import time as dt_time
        s = pd.Series(["10:07:49", "23:59:59", "00:00:00"])
        result = _try_convert_time_column(s)
        assert result is not None
        assert list(result) == [dt_time(10, 7, 49), dt_time(23, 59, 59), dt_time(0, 0, 0)]

    def test_preserves_nulls(self):
        s = pd.Series(["10:07:49", None, "08:30:00"])
        result = _try_convert_time_column(s)
        assert result is not None
        assert result.iloc[0].hour == 10
        assert result.iloc[1] is None
        assert result.iloc[2].hour == 8

    def test_none_string_treated_as_null(self):
        s = pd.Series(["10:07:49", "None", "08:30:00"])
        result = _try_convert_time_column(s)
        assert result is None  # "None" doesn't match HH:MM:SS, so column is not time

    def test_rejects_non_time_strings(self):
        s = pd.Series(["hello", "world"])
        assert _try_convert_time_column(s) is None

    def test_rejects_mixed_content(self):
        s = pd.Series(["10:07:49", "not-a-time"])
        assert _try_convert_time_column(s) is None

    def test_all_null_returns_none(self):
        s = pd.Series([None, None, None])
        assert _try_convert_time_column(s) is None

    def test_single_digit_hour(self):
        from datetime import time as dt_time
        s = pd.Series(["9:05:01"])
        result = _try_convert_time_column(s)
        assert result is not None
        assert result.iloc[0] == dt_time(9, 5, 1)


class TestEnsureBigQuerySafeDataframe:
    def test_object_columns_become_string(self):
        df = pd.DataFrame({"ids": [uuid.uuid4(), uuid.uuid4()]})
        assert df["ids"].dtype == object
        result = _ensure_bigquery_safe_dataframe(df)
        assert all(isinstance(v, str) for v in result["ids"])

    def test_numeric_columns_unchanged(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = _ensure_bigquery_safe_dataframe(df)
        assert result["x"].dtype == df["x"].dtype

    def test_original_dataframe_not_mutated(self):
        df = pd.DataFrame({"ids": [uuid.uuid4()]})
        _ensure_bigquery_safe_dataframe(df)
        assert df["ids"].dtype == object

    def test_none_preserved_as_null(self):
        df = pd.DataFrame({"val": ["hello", None, "world"]})
        result = _ensure_bigquery_safe_dataframe(df)
        assert pd.isna(result["val"].iloc[1])
        assert result["val"].iloc[0] == "hello"

    def test_time_column_converted(self):
        from datetime import time as dt_time
        df = pd.DataFrame({
            "name": ["alice", "bob"],
            "start_time": ["09:00:00", "17:30:00"],
        })
        result = _ensure_bigquery_safe_dataframe(df)
        assert result["start_time"].iloc[0] == dt_time(9, 0, 0)
        assert result["start_time"].iloc[1] == dt_time(17, 30, 0)
        assert result["name"].dtype == pd.StringDtype()

    def test_time_column_with_nulls(self):
        df = pd.DataFrame({"t": ["10:07:49", None, "08:30:00"]})
        result = _ensure_bigquery_safe_dataframe(df)
        assert result["t"].iloc[0].hour == 10
        assert result["t"].iloc[1] is None
        assert result["t"].iloc[2].hour == 8


# ---------------------------------------------------------------------------
# _load_service_account_credentials
# ---------------------------------------------------------------------------

VALID_CREDS = {
    "token_uri": "https://oauth2.googleapis.com/token",
    "client_email": "test@proj.iam.gserviceaccount.com",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\\nfake\\n-----END RSA PRIVATE KEY-----\\n",
    "project_id": "test-project",
}


class TestLoadServiceAccountCredentials:
    def test_loads_from_json_string(self, monkeypatch):
        monkeypatch.setenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", json.dumps(VALID_CREDS))
        creds = _load_service_account_credentials()
        assert creds["client_email"] == VALID_CREDS["client_email"]

    def test_newlines_unescaped_in_private_key(self, monkeypatch):
        monkeypatch.setenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", json.dumps(VALID_CREDS))
        creds = _load_service_account_credentials()
        assert "\\n" not in creds["private_key"]
        assert "\n" in creds["private_key"]

    def test_loads_from_file(self, monkeypatch, tmp_path):
        cred_file = tmp_path / "creds.json"
        cred_file.write_text(json.dumps(VALID_CREDS))
        monkeypatch.setenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", str(cred_file))
        creds = _load_service_account_credentials()
        assert creds["client_email"] == VALID_CREDS["client_email"]

    def test_invalid_json_raises(self, monkeypatch):
        monkeypatch.setenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", "not-json")
        with pytest.raises(ValueError, match="valid JSON"):
            _load_service_account_credentials()

    def test_missing_fields_raises(self, monkeypatch):
        incomplete = {"token_uri": "https://example.com"}
        monkeypatch.setenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", json.dumps(incomplete))
        with pytest.raises(ValueError, match="missing required fields"):
            _load_service_account_credentials()

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS", raising=False)
        with pytest.raises(KeyError):
            _load_service_account_credentials()
