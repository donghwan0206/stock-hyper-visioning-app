from psycopg2 import pool, extensions
from typing import Generator
from contextlib import contextmanager
from .abs import SqlConnectorShape

__all__ = (
    'PsqlDBClient',
)

class PsqlDBClient(
    SqlConnectorShape
):
    """PostgreSQL에 연결합니다.
    
    """
    def __init__(
            self, 
            host: str, 
            user: str, 
            password: str, 
            database: str,
            minconn=1,
            maxconn=10
    ):
        """
        PostgreSQL에 연결합니다.  

        `with` 구문 내에서 트랜잭션은 자동으로 처리됩니다.  
        오류 발생시 `rollback()` 되며 DB에 영향주지 않습니다.  

        >>> client = PsqlDBClient(...)
        >>> with client.cursor() as cur:
                cur.execute('SELECT * FROM antic.database LIMIT 10')
                rows = cur.fetchall()
                print(rows)

        :param host: (str) 호스트 명
        :param user: (str) 유저 명
        :param database: (str) 데이터베이스 명
        :param minconn: (int) 최소 커넥션 풀 (기본값: 1)
        :param maxconn: (int) 최대 커넥션 풀 (기본값: 10)
        
        """
        super().__init__(host, user, password, database)
        if minconn < 0 or maxconn > 30:
            raise ValueError(
                f"Invalid connection pool settings. min: {minconn}, max: {maxconn}"
            )
        self._pool = None
        dsn = f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}"
        self._connect(dsn, minconn, maxconn)

    @contextmanager
    def cursor(self) -> Generator[extensions.cursor, None, None]:
        with super().cursor() as cur:
            yield cur

    def _connect(self, dsn, minconn, maxconn):
        if self._pool:
            self.logger.warning(
                "Psql connector pool is already created."
            )
            return
        try:
            # psycopg2 내부적으로 dsn 파라미터를 제공합니다.  
            # >>> conn = psycopg2.connect(*self._args, **self._kwargs)
            self._pool = pool.SimpleConnectionPool(
                minconn=minconn, maxconn=maxconn, dsn=dsn
            )
        except Exception as e:
            self.logger.error(
                f"Psql connection pool creation is failed: {e}"
            )

    def _connection_impl(self):
        if not self._pool:
            raise RuntimeError("Connection pool is not created yet.")
        return self._pool.getconn()

    def _close_impl(self, conn=None, cursor=None):
        if self._pool:
            self._pool.putconn(conn)
        elif conn:
            conn.close()
        if cursor:
            cursor.close()
