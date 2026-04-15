"""Centralised environment helpers for BigQuery dataset routing.

Branch deployments (detected via DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT) write to a
separate dev dataset so they never touch production tables.
"""

import os

_PROD_DATASET_DEFAULT = "dagster"


def is_branch_deployment() -> bool:
    return os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"


def get_bq_dataset() -> str:
    """Return the BigQuery dataset name, using DEV_DATASET for branch deploys."""
    if is_branch_deployment():
        return os.environ.get("DEV_DATASET")
    return os.environ.get("BQ_DATASET")
