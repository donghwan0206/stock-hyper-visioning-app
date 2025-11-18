from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from ..client import KISClient, KST

# 국내업종 시간별지수(초) API 명세서 기반 collector
__all__ = ["fetch_inquire_index_tickprice"]

API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-tickprice"
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-index-tickprice
TR_ID = "FHPUP02110100"
METHOD = "GET"


def fetch_inquire_index_tickprice(
    client: KISClient,
    fid_input_iscd: str,
    *,
    fid_cond_mrkt_div_code: str = "U",
    custtype: str = "P",
) -> List[Dict[str, Any]]:
    """국내 업종 시간별(초) 지수를 조회한다."""
    params = {
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
    }
    response = client.request(
        METHOD,
        API_PATH,
        params=params,
        headers={"tr_id": TR_ID, "custtype": custtype},
    )
    collected_at = datetime.now(KST).replace(second=0, microsecond=0)
    metadata = {
        "rt_cd": response.get("rt_cd"),
        "msg_cd": response.get("msg_cd"),
        "msg1": response.get("msg1"),
        "collected_at": collected_at,
        "requested_fid_input_iscd": fid_input_iscd,
        "requested_fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
    }
    rows: List[Dict[str, Any]] = []
    for item in response.get("output") or []:
        rows.append({**metadata, **item})
    return rows
