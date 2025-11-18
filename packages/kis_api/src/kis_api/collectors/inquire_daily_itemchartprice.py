from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from ..client import KISClient, KST

# 국내주식기간별시세(일/주/월/년) API 명세서 기반 collector
__all__ = ["fetch_inquire_daily_itemchartprice"]

API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice
TR_ID = "FHKST03010100"
METHOD = "GET"


DATE_FMT = "%Y%m%d"


def _parse(date_str: str) -> datetime:
    return datetime.strptime(date_str, DATE_FMT)


def _format(date_value: datetime) -> str:
    return date_value.strftime(DATE_FMT)


def _build_date_ranges(start: datetime, end: datetime) -> Sequence[Tuple[str, str]]:
    """Split the requested 기간 into 최대 3 구간으로 나눈다."""
    ranges: List[Tuple[str, str]] = []
    remaining_chunks = 3
    current = start
    while current <= end and remaining_chunks > 0:
        remaining_days = (end - current).days + 1
        span = max(1, (remaining_days + remaining_chunks - 1) // remaining_chunks)
        chunk_end = min(current + timedelta(days=span - 1), end)
        ranges.append((_format(current), _format(chunk_end)))
        current = chunk_end + timedelta(days=1)
        remaining_chunks -= 1
    return ranges


def fetch_inquire_daily_itemchartprice(
    client: KISClient,
    fid_input_iscd: str,
    fid_input_date_1: str | None = None,
    fid_input_date_2: str | None = None,
    *,
    fid_cond_mrkt_div_code: str = "J",
    fid_period_div_code: str = "D",
    fid_org_adj_prc: str = "1",
    custtype: str = "P",
) -> Dict[str, Any]:
    """국내주식 기간별 시세(일/주/월/년) API 래퍼."""
    if fid_input_date_2 is None:
        fid_input_date_2 = (datetime.now(KST) - timedelta(days=1)).strftime(DATE_FMT)
    if fid_input_date_1 is None:
        fid_input_date_1 = (_parse(fid_input_date_2) - timedelta(days=365)).strftime(
            DATE_FMT
        )

    start_dt = _parse(fid_input_date_1)
    end_dt = _parse(fid_input_date_2)
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    collected_at = datetime.now(KST).replace(second=0, microsecond=0)
    metadata = {
        "rt_cd": None,
        "msg_cd": None,
        "msg1": None,
        "collected_at": collected_at,
        "requested_fid_input_iscd": fid_input_iscd,
        "requested_fid_input_date_1": _format(start_dt),
        "requested_fid_input_date_2": _format(end_dt),
        "requested_fid_period_div_code": fid_period_div_code,
        "requested_fid_org_adj_prc": fid_org_adj_prc,
    }

    summary: Dict[str, Any] | None = None
    rows: List[Dict[str, Any]] = []
    chunks = list(_build_date_ranges(start_dt, end_dt))
    for idx, (chunk_start, chunk_end) in enumerate(chunks):
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_INPUT_DATE_1": chunk_start,
            "FID_INPUT_DATE_2": chunk_end,
            "FID_PERIOD_DIV_CODE": fid_period_div_code,
            "FID_ORG_ADJ_PRC": fid_org_adj_prc,
        }
        response = client.request(
            METHOD,
            API_PATH,
            params=params,
            headers={"tr_id": TR_ID, "custtype": custtype},
        )
        metadata["rt_cd"] = response.get("rt_cd")
        metadata["msg_cd"] = response.get("msg_cd")
        metadata["msg1"] = response.get("msg1")
        if summary is None:
            summary = {**metadata, **(response.get("output1") or {})}
        for item in response.get("output2") or []:
            rows.append({**metadata, **item})
        if idx < len(chunks) - 1:
            time.sleep(0.5)

    return rows
