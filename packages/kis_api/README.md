# kis_api

KIS OpenAPI 연동을 위한 경량 Python 패키지입니다.  
토큰 발급과 공통 HTTP 호출은 `KISClient`가 담당하고, API 별 수집 로직은 `kis_api.collectors` 모듈로 분리되어 Functions/백엔드/배치 작업에서 재사용할 수 있습니다.

## 설치
```bash
# wheel 설치 (배포/CI용)
pip install dist/kis_api-0.1.0-py3-none-any.whl

# 개발 모드
pip install -e packages/kis_api[dev]
```

## 사용 예시
```python
from kis_api import KISClient, fetch_volume_rank_top30

client = KISClient(app_key="...", app_secret="...")
top30 = fetch_volume_rank_top30(client)
```

## 모듈 구성
- `kis_api.client.KISClient`: 토큰 발급, 인증 헤더, HTTP 요청을 담당.
- `kis_api.collectors.volume_rank.fetch_volume_rank_top30`: 거래량 순위 API 호출 및 결과 가공.

추가 API는 `kis_api/collectors/` 아래에 파일을 추가해 확장하며, `KISClient` 인스턴스를 주입받아 동일한 방식으로 동작하도록 설계합니다.
