from __future__ import annotations

from kis_api.collectors.volume_rank import fetch_volume_rank


def test_fetch_volume_rank(kis_client) -> None:
    """Call the volume rank API and print the first row for quick inspection."""
    results = fetch_volume_rank(kis_client)
    assert isinstance(results, list)
    if results:
        first = results[0]
        print("Top ranked symbol:", first.get("mksc_shrn_iscd"), first.get("hts_kor_isnm"))
    else:
        print("No data returned from volume rank API")
