from __future__ import annotations

from kis_api.collectors.inquire_time_itemconclusion import fetch_inquire_time_itemconclusion


def test_fetch_inquire_time_itemconclusion(kis_client) -> None:
    """Exercise the intraday time-conclusion API and log sample rows."""
    result = fetch_inquire_time_itemconclusion(
        kis_client,
        fid_input_iscd="005930",
        fid_input_hour_1="150000",
    )
    assert isinstance(result, list)
    ticks = result
    print(
        "Time conclusion ticks:",
        f"count={len(ticks)}",
    )
    if ticks:
        first = ticks[0]
        print(
            "First tick:",
            first.get("stck_cntg_hour"),
            first.get("stck_prpr"),
            first.get("cnqn"),
        )
