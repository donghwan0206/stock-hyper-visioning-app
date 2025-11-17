from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from ..client import KISClient, KST

# 국내업종현재지수_API 명세서 기반 collector
__all__ = ["fetch_inquire_index_price"]

API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-index-price
TR_ID = "FHPUP02100000"
METHOD = "GET"


def fetch_inquire_index_price(
    client: KISClient,
    fid_input_iscd: str,
    *,
    fid_cond_mrkt_div_code: str = "U",
    custtype: str = "P",
) -> Dict[str, Any]:
    """국내 업종 현재 지수 API를 호출해 응답 본문을 반환한다."""
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
