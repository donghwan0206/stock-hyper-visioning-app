from __future__ import annotations

from kis_api.collectors.investor_trade_by_stock_daily import fetch_investor_trade_by_stock_daily


def test_fetch_investor_trade_by_stock_daily(kis_client) -> None:
    """Fetch investor trading details for a specific stock/date and print a sample row."""
    results = fetch_investor_trade_by_stock_daily(
        kis_client,
        fid_input_iscd="000660",
        fid_input_date="20251113",
    )
    assert isinstance(results, list)
    if results:
        first = results[0]
        print(
            "Investor trade sample:",
            first.get("stck_shrn_iscd"),
            first.get("stck_prpr"),
            first.get("tr_pbmn"),
        )
    else:
        print("No investor trade data returned")
