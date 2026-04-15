"""Tests for the SQL Server asset factory."""

from qmi_etl.defs.sql_server_assets import build_sql_server_table_assets


class TestBuildSqlServerTableAssets:
    def test_creates_one_asset_per_table(self):
        tables = ["Customers", "Orders", "Products"]
        assets = build_sql_server_table_assets(tables)
        assert len(assets) == 3

    def test_asset_names_match_table_names(self):
        tables = ["Customers", "Orders"]
        assets = build_sql_server_table_assets(tables)
        names = {a.key.path[-1] for a in assets}
        assert names == {"Customers", "Orders"}

    def test_default_key_prefix(self):
        assets = build_sql_server_table_assets(["Foo"])
        assert assets[0].key.path[:-1] == ["sql_server"]

    def test_custom_key_prefix(self):
        assets = build_sql_server_table_assets(["Foo"], key_prefix=["custom", "prefix"])
        assert assets[0].key.path[:-1] == ["custom", "prefix"]

    def test_group_name(self):
        assets = build_sql_server_table_assets(["Foo"], group_name="my_group")
        assert assets[0].group_names_by_key[assets[0].key] == "my_group"

    def test_whitespace_stripped_from_table_names(self):
        assets = build_sql_server_table_assets(["Customers  ", "Orders "])
        names = {a.key.path[-1] for a in assets}
        assert names == {"Customers", "Orders"}

    def test_empty_list_returns_empty(self):
        assets = build_sql_server_table_assets([])
        assert assets == []
