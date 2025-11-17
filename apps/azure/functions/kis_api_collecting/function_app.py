import logging
import os
import azure.functions as func
from kis_api import KISClient, fetch_volume_rank_top30
import json

app = func.FunctionApp()

client = KISClient(
    app_key=os.environ["KIS_APP_KEY"],
    app_secret=os.environ["KIS_APP_SECRET"]
)

@app.function_name(name="kis-volumn_rank_collect_5min")
@app.event_hub_output(
    arg_name="kis_volume_rank",
    event_hub_name= os.environ["AnticSignalEventHubName"],
    connection="AnticSignalEventConnectionString",
)
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def volumn_rank_collect_5min(myTimer: func.TimerRequest, kis_volume_rank: func.Out[str]) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    data = fetch_volume_rank_top30(client)
    kis_volume_rank.set(json.dumps(data, default=str))
    logging.info('Python timer trigger function executed.')
