from qmi_etl.definitions import defs, SQL_SERVER_TABLES


def test_defs_can_load():
    """Dagster Definitions object loads without errors."""
    assert defs


def test_all_assets_job_exists():
    assert defs.get_job_def("all_assets_job")


def test_refresh_dbt_assets_job_exists():
    assert defs.get_job_def("refresh_dbt_assets")


def test_refresh_sql_server_assets_job_exists():
    assert defs.get_job_def("refresh_sql_server_assets")


def test_sql_server_tables_is_nonempty():
    assert len(SQL_SERVER_TABLES) > 0


def test_sql_server_tables_have_no_trailing_whitespace():
    for name in SQL_SERVER_TABLES:
        assert name == name.strip(), f"Table name has trailing whitespace: {name!r}"
