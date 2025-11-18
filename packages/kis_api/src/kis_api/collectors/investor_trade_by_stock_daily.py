from __future__ import annotations

from datetime import datetime
from typing import Any, List, Mapping, Optional

from ..client import KISClient, KST

# 종목별 투자자 매매동향(일별) 명세서 기반 collector
__all__ = ["fetch_investor_trade_by_stock_daily"]

API_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"  # api에 맞게 수정
# API 문서: https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily
TR_ID = "FHPTJ04160001"  # api에 맞게 수정
METHOD = "GET"
CUSTTYPE = "P"


def fetch_investor_trade_by_stock_daily(
    client: KISClient,
    fid_input_iscd: str,
    fid_input_date: str,  # YYYYMMDD
    *,
    fid_cond_mrkt_div_code: str = "J",
) -> List[Mapping[str, Any]]:
    """Call the volume-rank API and return the enriched payload."""
    params = {
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_INPUT_DATE_1": fid_input_date,
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "",
    }

    response = client.request(
        METHOD,
        API_PATH,
        params=params,
        headers={"tr_id": TR_ID, "custtype": CUSTTYPE},
    )

    collected_at = datetime.now(KST).replace(second=0, microsecond=0)
    enriched: List[Mapping[str, Any]] = []
    item = response.get("output2", [])[0]
    enriched.append(
        {
            "rt_cd": response.get("rt_cd"),
            "msg_cd": response.get("msg_cd"),
            "msg1": response.get("msg1"),
            "collected_at": collected_at,
            "requested_fid_input_iscd": fid_input_iscd,
            "requested_fid_input_date": fid_input_date,
            **item,
        }
    )
    return enriched
