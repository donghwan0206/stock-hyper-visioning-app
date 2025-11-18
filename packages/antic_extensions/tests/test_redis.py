import pytest
import os
from dotenv import load_dotenv
load_dotenv()

def test_redis():
    from src.antic_extensions.modules.database import RedisClient

    REDIS_HOST=os.getenv('REDIS_HOST')
    REDIS_PORT=int(
                os.getenv('REDIS_PORT', 6379)
            )
    REDIS_PASSWORD=os.getenv('REDIS_PASSWORD')
    assert isinstance(REDIS_HOST, str)
    assert isinstance(REDIS_PORT, int) and REDIS_PORT == 10000
    assert isinstance(REDIS_PASSWORD, str)

    print(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD[:5])
    client = RedisClient(
        REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, 0
    )
    with client.connect() as session:
        print('Redis Client ')
        print(session.ping())
        print(session.get('test'))
        t = session.ping()
        print('Redis ping:', t)
        assert t is True
