from typing import Union, Optional
from ..modules.database import (
    RedisClient
)
import logging
logger = logging.getLogger(__name__)

__all__ = (
    'RedisService',
)


class RedisService:
    """Redis 클라이언트를 연결하고 데이터 상호작용을 수행합니다.  

    ### 참고사항

    **실시간 Consumer 수신:**  

    ```python
    service = RedisService(...)
    rc = service.client
    ps = rc.pubsub()
    ps.subscribe("stock_channel")

    for msg in ps.listen():
        if msg["type"] == "message":
            data = msg["data"].decode()
            print(f"[REAL-TIME] {data}")
    ```
    """
    def __init__(
        self,
        host: str='localhost',
        port: int=6379,
        password=None,
        database: int=0,
        client: Optional[RedisClient]=None
    ) -> None:
        """Redis 클라이언트를 연결하고 데이터 상호작용을 수행합니다.  

        **client** 주입시 해당 client로 사용하며, 주입하지 않을시 자격 증명 데이터를 입력하세요.  

        >>> service = RedisService('localhost', 6379, 'mypassword', database=0)

        Args:
            host (str, optional): 호스트 명
            port (int, optional): 포트 (정수)
            password (_type_, optional): 암호 (엑세스 키)
            database (int, optional): 데이터베이스. 보통의 경우 기본값 0으로 건들지 않아도 됩니다.
            client (Optional[RedisClient], optional): 클라이언트 강제 주입. 자격 증명 입력시 해당 인자값은 넣지 마세요.
        """
        self._rclient = client
        if not self._rclient:
            self._rclient = RedisClient(host, port, password, database)
        logger.info(f'Redis initialized: {host}:{port}/{database}')

    @property
    def client(self):
        if not self._rclient:
            raise ConnectionError(
                "Redis client is null. Is database running correctly?"
            )
        return self._rclient
    
    def set(self, name: str, value: str, **kwargs):
        """
        해당하는 `name`으로 (인코딩 가능한) 데이터 `value`를 Redis에 저장.  
        
        Returns
        -------
        bool: 저장 성공 여부.
        """
        if not isinstance(name, str):
            raise TypeError(f"Invalid name type: {name}")
        if not isinstance(value, str):
            raise TypeError(f"Invalid value type: {value}")
        try:
            with self.client.connect() as conn:
                conn.set(name, value, **kwargs)
            return True
        except Exception as e:
            logger.error(e)
        return False

    
    def get(self, name: str, default=None, decode_to_utf_8: bool=True):
        """Redis로 부터 해당 Key의 Plain 데이터를 가져옵니다.

        없는 경우 `default`를 반환합니다. (기본값 `None`)  

        `decode_to_utf_8`가 True(기본)인 경우 utf-8로 디코드하여 반환합니다.  

        >>> STOCK = 'SKT'
        >>> service = RedisService(...)
        >>> data = service.get(f"stock:{STOCK}:amount", None)
        >>> print(data, type(data))
            ...
        >>> "23523000, <class 'str'>"

        """
        if not isinstance(name, str):
            raise TypeError(f"Invalid name type: {name}")
        try:
            with self.client.connect() as conn:
                v = conn.get(name) or default
            if decode_to_utf_8 and v:
                try:
                    return v.decode('utf-8')    # type: ignore
                except Exception as e:
                    logger.warning(e)
            return v
        except Exception as e:
            logger.error(e)

    def set_hash(self, name: str, mapping: dict, **kwargs):
        """해시 데이터를 특정 이름으로 저장합니다.
        
        >>> STOCK = 'SAMSUNG'
        >>> service = RedisService(...)
        >>> service.set_hash(f"stock:{STOCK}:detail", mapping={
                "price": price,
                "timestamp": time.time()
            })
        """
        if not isinstance(mapping, dict):
            raise TypeError(f"Invalid mapping type: {mapping}")
        if not isinstance(name, str):
            raise TypeError(f"Invalid name type: {name}")
        try:
            with self.client.connect() as conn:
                conn.hset(name, mapping=mapping, **kwargs)
        except Exception as e:
            logger.error(e)

    def get_hash(self, name: str, key: Optional[str]=None):
        """Hash 데이터를 가져옵니다.  
        key가 지정되지 않은 경우 (None), 모든 해시 키-값을 가져오고,  
        키가 지정된 경우 특정 해시(name)에서 특정 키(key)의 값(value)을 참조합니다.  

        >>> service = RedisService(...)
        >>> price = service.get_hash("stock:SAMSUNG:detail", "price")
            print(price)
            '23213213'
        >>> all_dict = service.get_hash("stock:SAMSUNG:detail")
            print(all_dict)
            "
            {
                'price': 23213213,
                'code': '052332'
            }
            "
        """
        if not isinstance(key, str) or (key and not isinstance(key, str)):
            raise TypeError(f"Invalid key or name type: {name}")
        try:
            with self.client.connect() as conn:
                if key:
                    return conn.hget(name, key)
                else:
                    return conn.hgetall(name)
        except Exception as e:
            logger.error(e)

