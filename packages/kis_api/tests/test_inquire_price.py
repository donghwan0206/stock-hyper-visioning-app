from __future__ import annotations

from kis_api.collectors.inquire_price import fetch_inquire_price


def test_fetch_inquire_price(kis_client) -> None:
    """Ensure the stock price quote API responds with a payload."""
    result = fetch_inquire_price(kis_client, fid_input_iscd="005930")
    assert isinstance(result, dict)
    print(
        "Stock price quote:",
        result.get("stck_prpr"),
        result.get("acml_vol"),
        result.get("rprs_mrkt_kor_name"),
    )
