from typing import Any

import sqlalchemy
from sqlalchemy import text


class EnitioSQLServerConnection:
    """Simple helper for creating a SQLAlchemy engine backed by SQL Server.

    Parameters are passed explicitly rather than relying on environment
    variables so that calling code controls the connection details.

    Args:
        host: SQL Server host name or IP.
        database: Target database name.
        username: Username for SQL authentication.
        password: Password for SQL authentication.
        port: TCP port for SQL Server.
        connection_string: Full connection string (overridden by internal build).
        engine: Placeholder for a SQLAlchemy engine instance.
    """

    def __init__(
        self,
        host: str,
        database: str,
        username: str,
        password: str,
        port: int,
        connection_string: str,
        engine: sqlalchemy.engine.Engine,
    ) -> None:


        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection_string = (
            f"mssql+pymssql://{username}:{password}@{host}:{port}/{database}"
        )

        try:
            self.engine = sqlalchemy.create_engine(self.connection_string)
        except Exception as exc:
            raise RuntimeError(f"Failed to create SQL Server engine for {self.host}:{self.port}/{self.database}") from exc

    def execute_query(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        return_results: bool = True,
    ) -> list[tuple[Any, ...]] | None:
        """Execute a SQL query. See module doc for usage."""
        params = parameters or {}
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                if return_results:
                    return result.fetchall()
                conn.commit()
                return None
        except Exception as exc:
            raise RuntimeError(f"Query execution failed: {exc}") from exc
