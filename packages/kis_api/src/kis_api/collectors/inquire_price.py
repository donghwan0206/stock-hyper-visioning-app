from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from ..client import KISClient, KST

# 주식현재가시세_API 명세서 기반 collector
__all__ = ["fetch_inquire_price"]

API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-price
TR_ID = "FHKST01010100"
METHOD = "GET"


def fetch_inquire_price(
    client: KISClient,
    fid_input_iscd: str,
    *,
    fid_cond_mrkt_div_code: str = "J",
    custtype: str = "P",
) -> Dict[str, Any]:
    """주식 현재가 시세 API 호출 래퍼."""
    params = {
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
        "FID_INPUT_ISCD": fid_input_iscd,
    }
    response = client.request(
        METHOD,
        API_PATH,
        params=params,
        headers={"tr_id": TR_ID, "custtype": custtype},
    )
    collected_at = datetime.now(KST).replace(second=0, microsecond=0)
    payload = response.get("output") or {}
    return {
        "rt_cd": response.get("rt_cd"),
        "msg_cd": response.get("msg_cd"),
        "msg1": response.get("msg1"),
        "collected_at": collected_at,
        "requested_fid_input_iscd": fid_input_iscd,
        **payload,
    }
