"""Load the datawarehouse_2 dbt project as Dagster assets."""

import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from qmi_etl.defs.env_config import get_bq_dataset

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "datawarehouse_2"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)


class BqDbtTranslator(DagsterDbtTranslator):
    """Assigns dbt assets to groups by folder:
    base → base_dbt, staging → staging_dbt, reporting → reporting_dbt, seeds → seeds_dbt.
    """

    _FOLDER_TO_GROUP = {
        "base": "base_dbt",
        "staging": "staging_dbt",
        "reporting": "reporting_dbt",
        "seeds": "seeds_dbt",
    }

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props.get("resource_type") == "seed":
            return "seeds_dbt"
        fqn = dbt_resource_props.get("fqn") or []
        if len(fqn) >= 2:
            folder = fqn[1]
            return self._FOLDER_TO_GROUP.get(folder, f"{folder}_dbt")
        return "bq_dbt"


def _inject_bq_credentials() -> str:
    """Write BQ service account JSON to a temp file and set env so dbt subprocess can use it.

    Uses BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS (same as the BQ IO manager).
    Sets GOOGLE_APPLICATION_CREDENTIALS and DBT_BIGQUERY_KEYFILE so dbt sees the keyfile.
    Returns the keyfile path (caller should clean up or leave for process lifetime).
    """
    raw = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS")
    if not raw:
        raise ValueError(
            "BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS is not set; required for dbt to authenticate to BigQuery"
        )
    fd, path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(raw)
    except Exception:
        os.unlink(path)
        raise
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    os.environ["DBT_BIGQUERY_KEYFILE"] = path
    return path


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=BqDbtTranslator(),
)
def datawarehouse_2_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """dbt models, seeds, and snapshots for the datawarehouse_2 project."""
    keyfile_path = _inject_bq_credentials()
    dataset = get_bq_dataset()
    prev_dataset = os.environ.get("BQ_DATASET")
    os.environ["BQ_DATASET"] = dataset
    try:
        yield from dbt.cli(["build"], context=context).stream()
    finally:
        if prev_dataset is not None:
            os.environ["BQ_DATASET"] = prev_dataset
        else:
            os.environ.pop("BQ_DATASET", None)
        for var in ("GOOGLE_APPLICATION_CREDENTIALS", "DBT_BIGQUERY_KEYFILE"):
            os.environ.pop(var, None)
        if keyfile_path and os.path.isfile(keyfile_path):
            try:
                os.unlink(keyfile_path)
            except OSError:
                pass
