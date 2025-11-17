from __future__ import annotations

from datetime import datetime
from typing import Any, List, Mapping, Optional

from ..client import KISClient, KST

__all__ = ["fetch_volume_rank_top30"]

API_PATH = "/uapi/domestic-stock/v1/quotations/volume-rank"
TR_ID_VOLUME_RANK = "FHPST01710000"


def fetch_volume_rank_top30(
    client: KISClient,
    *,
    fid_cond_mrkt_div_code: str = "J",
    fid_cond_scr_div_code: str = "20171",
    fid_input_iscd: str = "0000",
    fid_div_cls_code: str = "0",
    fid_blng_cls_code: str = "0",
    fid_trgt_cls_code: str = "11111111",
    fid_trgt_exls_cls_code: str = "0000000000",
    fid_input_price_1: str = "",
    fid_input_price_2: str = "",
    fid_vol_cnt: str = "",
    fid_input_date_1: str = "",
) -> List[Mapping[str, Any]]:
    """Call the volume-rank API and return the enriched payload."""
    params = {
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
        "FID_COND_SCR_DIV_CODE": fid_cond_scr_div_code,
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_DIV_CLS_CODE": fid_div_cls_code,
        "FID_BLNG_CLS_CODE": fid_blng_cls_code,
        "FID_TRGT_CLS_CODE": fid_trgt_cls_code,
        "FID_TRGT_EXLS_CLS_CODE": fid_trgt_exls_cls_code,
        "FID_INPUT_PRICE_1": fid_input_price_1,
        "FID_INPUT_PRICE_2": fid_input_price_2,
        "FID_VOL_CNT": fid_vol_cnt,
        "FID_INPUT_DATE_1": fid_input_date_1,
    }

    response = client.request(
        "get",
        API_PATH,
        params=params,
        headers={"tr_id": TR_ID_VOLUME_RANK},
    )

    collected_at = datetime.now(KST).replace(second=0, microsecond=0)
    enriched: List[Mapping[str, Any]] = []
    for item in response.get("output", []):
        enriched.append(
            {
                "rt_cd": response.get("rt_cd"),
                "msg_cd": response.get("msg_cd"),
                "msg1": response.get("msg1"),
                "collected_at": collected_at,
                **item,
            }
        )
    return enriched
