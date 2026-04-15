"""Asset factory for loading many SQL Server tables as Dagster assets."""

import os

import pandas as pd
from dagster import AssetExecutionContext, asset

from qmi_etl.defs.sql_server_io_manager import EnitioSQLServerConnection


def _build_connection() -> EnitioSQLServerConnection:
    """Create a connection from environment variables (called at materialization time)."""
    return EnitioSQLServerConnection(
        host=os.environ["ENITIO_SQL_HOST"],
        database=os.environ["ENITIO_SQL_DATABASE"],
        username=os.environ["ENITIO_SQL_USERNAME"],
        password=os.environ["ENITIO_SQL_PASSWORD"],
        port=int(os.environ.get("ENITIO_SQL_PORT", "1433")),
        connection_string="",
        engine=None,  # type: ignore[arg-type]
    )


def build_sql_server_table_assets(
    table_names: list[str],
    *,
    schema: str = "dbo",
    group_name: str = "sql_server",
    key_prefix: list[str] | None = None,
):
    """Build one Dagster asset per SQL Server table.

    The connection is created lazily at materialization time from
    ENITIO_SQL_* environment variables, so the code location can load
    without a live SQL Server.

    Args:
        table_names: List of table names to load (e.g. ["customers", "orders"]).
        schema: SQL Server schema (default "dbo").
        group_name: Dagster asset group name.
        key_prefix: Optional key prefix for asset keys (e.g. ["sql_server"]).

    Returns:
        List of Dagster asset definitions.
    """
    prefix = key_prefix or ["sql_server"]
    assets = []
    # remove whitespace from table names
    table_names = [x.rstrip() for x in table_names]

    for table_name in table_names:
        def _make_asset(tbl: str):
            @asset(
                name=tbl,
                group_name=group_name,
                key_prefix=prefix,
                io_manager_key="bigquery_io_manager",
            )
            def _load_table(context: AssetExecutionContext) -> pd.DataFrame:
                conn = _build_connection()
                quoted = f"[{schema}].[{tbl}]"
                sql = f"SELECT * FROM {quoted}"
                return pd.read_sql(sql, conn.engine)

            return _load_table

        assets.append(_make_asset(table_name))

    return assets
