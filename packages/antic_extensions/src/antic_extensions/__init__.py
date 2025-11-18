"""
# AnticSignal Extensions

모듈과 함수 앱에서 필요한 여러가지 기능을 제공합니다.

**현재 기능:**
    - PostgreSQL 세션 관리/쿼리
    - Redis 세션 관리/조회/삭제 등

## PostgreSQL
```
from antic_extensions import PsqlDBClient

# PsqlDBClient내에 자격 증명 정보를 입력합니다.
client = PsqlDBClient(...)
with client.cursor() as cur:
    cur.execute('SELECT * from database LIMIT 100')
    rows = cur.fetchall()
```

## Redis (Azure Managed)
```
from antic_extensions import RedisService

# RedisService내에 자격 증명 정보를 입력합니다.
service = RedisService(...)
service.set('test_name', 'test_value')
service.get('test_name')
service.set_hash('test_hash', {'test': 2323 })
service.get_hash('test_hash')
```

"""
from .core import *


