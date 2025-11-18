from __future__ import annotations

from kis_api.collectors.inquire_index_tickprice import fetch_inquire_index_tickprice


def test_fetch_inquire_index_tickprice(kis_client) -> None:
    """Ensure the index tick price API responds with rows."""
    rows = fetch_inquire_index_tickprice(kis_client, fid_input_iscd="0001")
    assert isinstance(rows, list)
    print("index tick rows:", len(rows))
    if rows:
        first = rows[0]
        print(
            "First tick:",
            first.get("stck_cntg_hour"),
            first.get("bstp_nmix_prpr"),
            first.get("cntg_vol"),
        )
