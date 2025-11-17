"""
kis_api 패키지는 KIS OpenAPI 수집 클라이언트를 제공합니다.

Azure Functions, 백엔드 서비스 등에서 동일 로직을 재사용하려면
`pip install -e packages/kis_api` 또는 배포용 wheel을 설치하세요.
"""

from .client import KISClient
from .collectors.volume_rank import fetch_volume_rank_top30

__all__ = ["KISClient", "fetch_volume_rank_top30"]
