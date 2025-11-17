"""
kis_api 패키지는 KIS OpenAPI 수집 클라이언트를 제공합니다.

Azure Functions, 백엔드 서비스 등에서 동일 로직을 재사용하려면
`pip install -e packages/kis_api` 또는 배포용 wheel을 설치하세요.
"""

from .client import KISClient
# 국내업종현재지수_API collector -> inquire-index-price
from .collectors.inquire_index_price import fetch_inquire_index_price
# 종목별 투자자매매동향(일별) collector -> investor-trade-by-stock-daily
from .collectors.investor_trade_by_stock_daily import fetch_investor_trade_by_stock_daily
# 주식현재가시세_API collector -> inquire-price
from .collectors.inquire_price import fetch_inquire_price
# 주식현재가_당일시간대별체결_API collector -> inquire-time-itemconclusion
from .collectors.inquire_time_itemconclusion import fetch_inquire_time_itemconclusion
# 거래량 순위 API collector -> volume-rank
from .collectors.volume_rank import fetch_volume_rank

__all__ = [
    "KISClient",
    "fetch_inquire_index_price",
    "fetch_investor_trade_by_stock_daily",
    "fetch_inquire_price",
    "fetch_inquire_time_itemconclusion",
    "fetch_volume_rank",
]
