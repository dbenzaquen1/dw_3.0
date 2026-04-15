"""IO manager for writing pandas DataFrames to BigQuery tables.

Uses GCP service account credentials from environment variables:

- BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS: Full service account JSON key (single env var)
- GCP_PROJECT_ID: GCP project id (falls back to project_id in the JSON)
- BQ_DATASET: BigQuery dataset name
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import pandas as pd
from dagster import IOManager, InputContext, OutputContext
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from qmi_etl.defs.env_config import get_bq_dataset


def _load_service_account_credentials() -> dict:
    """Load and normalize service account credentials from env var.

    Accepts the full service account JSON as a string (BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS).
    json.loads handles \\n escaping in the private key correctly.
    """
    raw = os.environ["BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS"]

    if os.path.exists(raw):
        with open(raw, encoding="utf-8") as f:
            credentials = json.load(f)
    else:
        try:
            credentials = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS must be valid JSON or a path to a JSON file"
            ) from exc

    required = ["token_uri", "client_email", "private_key"]
    missing = [f for f in required if f not in credentials]
    if missing:
        raise ValueError(
            f"Service account JSON missing required fields: {', '.join(missing)}"
        )

    credentials["private_key"] = credentials["private_key"].replace("\\n", "\n")
    return credentials


def _gcp_credentials() -> tuple[service_account.Credentials, str]:
    """Return (credentials, project_id) from the service account JSON."""
    creds_info = _load_service_account_credentials()
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    project_id = os.environ.get("GCP_PROJECT_ID", creds_info.get("project_id", ""))
    return credentials, project_id


def _bq_client() -> bigquery.Client:
    credentials, project_id = _gcp_credentials()
    return bigquery.Client(project=project_id, credentials=credentials)


def _ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Create the BigQuery dataset if it doesn't already exist."""
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = os.environ.get("BQ_LOCATION", "US")
        client.create_dataset(dataset)


def _sanitize_bigquery_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns so they are valid BigQuery identifiers (letters, numbers, underscore only)."""
    df = df.copy()
    new_names = []
    seen: dict[str, int] = {}
    for col in df.columns:
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", str(col))
        safe = re.sub(r"_+", "_", safe).strip("_") or "unnamed"
        count = seen.get(safe, 0)
        seen[safe] = count + 1
        if count:
            safe = f"{safe}_{count}"
        new_names.append(safe)
    if new_names != list(df.columns):
        df.columns = new_names
    return df


_TIME_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")


def _try_convert_time_column(series: pd.Series) -> pd.Series | None:
    """If every non-null value matches HH:MM:SS, return the series as datetime.time objects."""
    from datetime import time as dt_time

    non_null = series.dropna()
    if non_null.empty:
        return None
    sample = non_null.head(50).astype(str)
    if not sample.str.strip().str.match(_TIME_RE).all():
        return None
    def _parse(v):
        if pd.isna(v) or str(v).strip() in ("", "None"):
            return None
        parts = str(v).strip().split(":")
        return dt_time(int(parts[0]), int(parts[1]), int(parts[2]))
    return series.map(_parse)


def _ensure_bigquery_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object columns for BigQuery compatibility.

    - Columns whose non-null values all match HH:MM:SS are converted to
      datetime.time so BigQuery stores them as TIME.
    - Remaining object columns are converted to pandas StringDtype to
      preserve NULL (instead of turning Python None into the string "None").
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object or df[col].dtype.name == "object":
            time_series = _try_convert_time_column(df[col])
            if time_series is not None:
                df[col] = time_series
            else:
                df[col] = df[col].astype(pd.StringDtype())
    return df


class BigQueryPandasIOManager(IOManager):
    """IO manager that writes DataFrames to BigQuery using GCP env credentials."""

    def _table_id_from_context(self, context: OutputContext | InputContext) -> str:
        _, project_id = _gcp_credentials()
        dataset = get_bq_dataset()
        table = context.asset_key.path[-1]
        return f"{project_id}.{dataset}.{table}"

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"BigQueryPandasIOManager expected a pandas.DataFrame, "
                f"got {type(obj)!r} for asset {context.asset_key.to_user_string()}."
            )

        table_id = self._table_id_from_context(context)
        context.log.info(
            "Writing %d rows (%d cols) to BigQuery table %s",
            len(obj), len(obj.columns), table_id,
        )

        client = _bq_client()
        dataset_ref = table_id.rsplit(".", 1)[0]
        _ensure_dataset(client, dataset_ref)

        obj = _sanitize_bigquery_column_names(obj)
        obj = _ensure_bigquery_safe_dataframe(obj)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        load_job = client.load_table_from_dataframe(obj, table_id, job_config=job_config)
        load_job.result()

        context.log.info("Successfully wrote to %s", table_id)
        context.add_output_metadata({
            "row_count": len(obj),
            "bigquery_table": table_id,
        })

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_id = self._table_id_from_context(context.upstream_output)
        client = _bq_client()
        return client.list_rows(client.get_table(table_id)).to_dataframe()


bigquery_io_manager = BigQueryPandasIOManager()
