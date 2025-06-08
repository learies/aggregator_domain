from typing import Any, Type

from psycopg import Connection, connect

from src.config.settings import settings


class DatabaseConnection:
    def __init__(self):
        self.connection: Connection | None = None
        self.database_url: str = settings.DATABASE_URL
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")

    def connect(self) -> Connection:
        """Connect to the database with reconnect support"""
        if self.connection is None or self.connection.closed:
            self.connection = connect(self.database_url)
        return self.connection

    def connect_with_transaction(self) -> Connection:
        """Connect to the database with reconnect support and start a transaction"""
        conn = self.connect()
        conn.autocommit = False
        return conn

    def close(self) -> None:
        """Close the database connection"""
        if self.connection is not None and not self.connection.closed:
            self.connection.close()
        self.connection = None

    def __enter__(self) -> Connection:
        return self.connect()

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        if self.connection is not None and not self.connection.closed:
            if exc_type is not None and not self.connection.autocommit:
                self.connection.rollback()
            self.close()


db = DatabaseConnection()
