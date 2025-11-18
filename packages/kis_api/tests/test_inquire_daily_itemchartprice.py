from __future__ import annotations

from kis_api.collectors.inquire_daily_itemchartprice import fetch_inquire_daily_itemchartprice


def test_fetch_inquire_daily_itemchartprice(kis_client) -> None:
    """Verify the period price API returns summary and row data."""
    result = fetch_inquire_daily_itemchartprice(
        kis_client,
        fid_input_iscd="005930",
        fid_input_date_1="20240101",
        fid_input_date_2="20240201",
    )
    assert isinstance(result, dict)
    rows = result.get("rows") or []
    print(
        "Daily itemchart summary:",
        result.get("summary", {}).get("stck_prpr"),
        f"rows={len(rows)}",
    )
    if rows:
        first = rows[0]
        print(
            "First candle:",
            first.get("stck_bsop_date"),
            first.get("stck_clpr"),
        )
