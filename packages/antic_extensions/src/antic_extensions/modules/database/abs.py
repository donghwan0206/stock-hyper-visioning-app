from abc import ABC, abstractmethod
from typing import Protocol, Any, Generator
from contextlib import contextmanager
import logging


class SqlConnectorShape(ABC):
    logger = logging

    def __init__(
            self, 
            host=None, 
            user=None, 
            password=None, 
            database=None
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self._connection = None
        self._cursor = None

    @abstractmethod
    def _connection_impl(self):
        """conn 구현부"""
        ...

    @abstractmethod
    def _close_impl(self, conn=None, cursor=None):
        ...

    @contextmanager
    def cursor(self) -> Generator[Any, None, None]:
        conn = self._connection_impl()
        if not conn:
            raise RuntimeError(
                "Cannot connect to database. Connection is null."
            )
        if not hasattr(conn, 'rollback') or \
                not hasattr(conn, 'commit') or not hasattr(conn, 'cursor'):
            raise AssertionError(
                "Maybe this is not supported database."
            )
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            try:
                self._close_impl(conn)
            except Exception as e:
                self.logger.error(e)
