from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from ..client import KISClient, KST

# 주식현재가_당일시간대별체결_API 명세서 기반 collector
__all__ = ["fetch_inquire_time_itemconclusion"]

API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-itemconclusion"
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-time-itemconclusion
TR_ID = "FHPST01060000"
METHOD = "GET"


def fetch_inquire_time_itemconclusion(
    client: KISClient,
    fid_input_iscd: str,
    fid_input_hour_1: str,
    *,
    fid_cond_mrkt_div_code: str = "J",
    custtype: str = "P",
) -> Dict[str, Any]:
    """주식 현재가 당일 시간대별 체결 API 래퍼."""
    params = {
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_INPUT_HOUR_1": fid_input_hour_1,
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
        "requested_fid_input_hour_1": fid_input_hour_1,
        "requested_fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
    }
    summary = {**metadata, **(response.get("output1") or {})}
    ticks: List[Dict[str, Any]] = []
    for item in response.get("output2") or []:
        ticks.append({**metadata, **item})
    return ticks
