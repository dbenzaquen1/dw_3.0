"""Tests for the SQL Server connection helper."""

from unittest.mock import patch

import pytest

from qmi_etl.defs.sql_server_io_manager import EnitioSQLServerConnection


class TestEnitioSQLServerConnection:
    def test_connection_string_format(self):
        with patch("qmi_etl.defs.sql_server_io_manager.sqlalchemy.create_engine"):
            conn = EnitioSQLServerConnection(
                host="myhost",
                database="mydb",
                username="user",
                password="pass",
                port=1433,
                connection_string="",
                engine=None,
            )
        assert conn.connection_string == "mssql+pymssql://user:pass@myhost:1433/mydb"

    def test_custom_port(self):
        with patch("qmi_etl.defs.sql_server_io_manager.sqlalchemy.create_engine"):
            conn = EnitioSQLServerConnection(
                host="server",
                database="db",
                username="u",
                password="p",
                port=5000,
                connection_string="",
                engine=None,
            )
        assert ":5000/" in conn.connection_string

    def test_attributes_stored(self):
        with patch("qmi_etl.defs.sql_server_io_manager.sqlalchemy.create_engine"):
            conn = EnitioSQLServerConnection(
                host="h",
                database="d",
                username="u",
                password="p",
                port=1433,
                connection_string="",
                engine=None,
            )
        assert conn.host == "h"
        assert conn.database == "d"
        assert conn.username == "u"
        assert conn.password == "p"
        assert conn.port == 1433

    def test_engine_creation_failure_raises_runtime_error(self):
        with patch(
            "qmi_etl.defs.sql_server_io_manager.sqlalchemy.create_engine",
            side_effect=Exception("boom"),
        ):
            with pytest.raises(RuntimeError, match="Failed to create SQL Server engine"):
                EnitioSQLServerConnection(
                    host="h",
                    database="d",
                    username="u",
                    password="p",
                    port=1433,
                    connection_string="",
                    engine=None,
                )
