import json
import logging
import os
from typing import Iterable, List, Sequence

import azure.functions as func
from kis_api import KISClient, fetch_inquire_price, fetch_volume_rank


app = func.FunctionApp()  # type: ignore

client = KISClient(
    app_key=os.environ["KIS_APP_KEY"],
    app_secret=os.environ["KIS_APP_SECRET"],
    request_interval=0.5
)


@app.function_name(name="kis-volumn_rank_collect_5min")
@app.event_hub_output(
    arg_name="kis_volume_rank",
    event_hub_name=os.environ["AnticSignalEventHubName"],
    connection="AnticSignalEventConnectionString",
)
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def volumn_rank_collect_5min(myTimer: func.TimerRequest, kis_volume_rank: func.Out[str]) -> None:  # type: ignore
    if myTimer.past_due:
        logging.info("The timer is past due!")

    data = fetch_volume_rank(client)
    kis_volume_rank.set(json.dumps(data, default=str))
    logging.info("Python timer trigger function executed.")


def _extract_stock_codes(payload: str) -> List[str]:
    """volume-rank 메시지에서 종목코드를 추출한다."""
    try:
        data = json.loads(payload)['output']
    except json.JSONDecodeError:
        logging.warning("Skip message, invalid JSON: %s", payload)
        return []

    candidates: Iterable[str] = []
    if isinstance(data, list):
        candidates = (item.get("mksc_shrn_iscd") for item in data if isinstance(item, dict))
    elif isinstance(data, dict):
        candidates = [data.get("mksc_shrn_iscd") or data.get("stck_shrn_iscd")]
    else:
        logging.info("Unsupported payload type for extracting stock codes: %s", type(data))
        return []

    codes = [code for code in candidates if code]
    return codes[:30]


@app.function_name(name="kis_inquire_price_from_event")
@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name=os.environ["AnticSignalEventHubName"],
    connection="AnticSignalEventConnectionString",
    consumer_group="$Default",
)
def inquire_price_from_event(events: Sequence[func.EventHubEvent]) -> None:  # type: ignore
    """Event Hub 메시지를 받아 주식 현재가 시세를 조회한다."""
    if not isinstance(events, Sequence):
        events = [events]

    for event in events:
        raw = event.get_body().decode("utf-8")
        stock_codes = _extract_stock_codes(raw)
        if not stock_codes:
            logging.info("No stock codes found in message: %s", raw)
            continue

        enriched_payloads = []
        for code in stock_codes:
            try:
                enriched_payloads.append(fetch_inquire_price(client, fid_input_iscd=code))
            except Exception as exc:  # pylint: disable=broad-except
                logging.exception("Failed to fetch current price for %s: %s", code, exc)

        _persist_snapshot(enriched_payloads)

        logging.info(
            "Fetched %d current price rows for event sequence=%s",
            len(enriched_payloads),
            getattr(event, "sequence_number", None),
        )


def _persist_snapshot(payloads: List[dict]) -> None:
    """현재가 응답 목록을 JSON 파일로 저장한다."""
    output_path = os.environ.get("CURRENT_PRICE_SNAPSHOT_PATH", "./tmp/current_price_snapshot.json")
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fp:
            json.dump(payloads, fp, default=str, ensure_ascii=False, indent=2)
        logging.info("Saved %d rows to %s", len(payloads), output_path)
    except OSError as exc:
        logging.exception("Failed to persist current price snapshot to %s: %s", output_path, exc)
