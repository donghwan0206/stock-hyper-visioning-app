import redis
import threading
import logging
from contextlib import contextmanager
from ssl import CERT_NONE, CERT_OPTIONAL


__all__ = (
    'RedisClient',
)
logger = logging.getLogger(__name__)

class RedisClient:
    """`Azure Managed Redis`에 연결하는 Client를 구성합니다.


    >>> client = RedisClient(...)
    >>> with client.connect() as conn:
            conn.ping()
            ...
    """
    
    _lock = threading.Lock()
    _pool = None

    SSL_CERT = CERT_OPTIONAL

    def __init__(
        self,
        host: str='localhost',
        port: int=6379,
        password=None,
        database: int=0,
        ssl: bool=True,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        health_check_interval=30,
        max_connections=10 
    ) -> None:
        self._host = host
        self._port = port
        self._password_masked = '*'*len(password) if password else password
        self.__connect(
            host=host,
            port=port,
            password=password,
            db=database,
            ssl=ssl,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            health_check_interval=health_check_interval,
            max_connections=max_connections
        )

    def __str__(self) -> str:
        return f"{__class__.__name__}(host={self._host}, port={self._port}, password={self._password_masked})"

    def __connect(self, **kwargs):
        with RedisClient._lock:
            ssl = kwargs.pop('ssl', False)
            _protocol = 'redis'
            if ssl:
                _protocol = 'rediss'
            decode_responses = kwargs.pop('decode_responses', False)
            socket_timeout = kwargs.pop('socket_timeout', None)
            socket_connect_timeout = kwargs.pop('socket_connect_timeout', None)
            health_check_interval = kwargs.pop('health_check_interval', 0)
            max_connections = kwargs.pop('max_connections', None)
            if RedisClient._pool is None:
                RedisClient._pool = redis.ConnectionPool()
            self._client = redis.Redis(
                connection_pool=RedisClient._pool.from_url(
                    f"{_protocol}://:{kwargs.get('password')}@{kwargs.get('host')}:{kwargs.get('port')}/{kwargs.get('database')}"
                ),
                ssl=ssl,
                decode_responses=decode_responses,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                health_check_interval=health_check_interval,
                ssl_cert_reqs=self.SSL_CERT,
                max_connections=max_connections
            )
            result = self._client.ping()
            if not result:
                raise ConnectionError(
                    f"Cannot connect to redis: {kwargs.get('host')}:{kwargs.get('port')}"
                )

    @contextmanager
    def connect(self):
        try:
            yield self._client
        except redis.AuthenticationError as e:
            logger.error(f"Check if Azure Entra ID authentication is properly configured: {e}")
        except redis.ConnectionError as e:
            import traceback
            print(traceback.format_exc())
            logger.error(f"Check if Redis host and port are correct, and ensure network connectivity: {e}")
        except redis.TimeoutError as e:
            logger.error(f"Check network latency and Redis server performance: {e}")
        except Exception as e:
            logger.error(e)
            if "999" in str(e):
                logger.error(
                    ("Error 999 typically indicates a network connectivity issue or firewall restriction"
                    "   - Verify the Redis hostname is correct"
                    "   - Verify that you have logged in with Az CLI"
                    "   - Ensure the Redis cache is running and accessible\n")
                )

    def close(self):
        try:
            if self._client:
                self._client.close()
        except Exception as e:
            logger.error(e)
