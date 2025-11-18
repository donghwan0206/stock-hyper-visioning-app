from __future__ import annotations

from kis_api.collectors.inquire_index_price import fetch_inquire_index_price


def test_fetch_inquire_index_price(kis_client) -> None:
    """Call the domestic industry index API and inspect the payload."""
    result = fetch_inquire_index_price(kis_client, fid_input_iscd="0001")
    assert isinstance(result, dict)
    print(
        "Industry index sample:",
        result.get("bstp_nmix_prpr"),
        result.get("acml_vol"),
        result.get("collected_at"),
    )
