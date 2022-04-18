from datetime import datetime, timezone
import os
import time
import logging
import traceback
from binance.lib.utils import config_logging
from binance.websocket.futures.websocket_client import FuturesWebsocketClient as Client
from itsdangerous import json
import pandas as pd

from live.utils import get_close_price

config_logging(logging, logging.DEBUG)


DATA_PATH = "./feed"


def read_price_data(symbol):
    filename = f"{DATA_PATH}/{symbol}.csv"
    if os.path.exists(filename):
        df = pd.read_csv(filename)

    df = get_close_price(symbol, interval=TF)
    df.reset_index(inplace=True)
    df.columns = ['open_time', 'close']
    df.to_csv(filename, index=False)
    return df


symbols = ['XRPUSDT','ADAUSDT']
hedge = [30.68800711, -158.6123163]
TF = '5m'
div = 0.5
qty_ = [100, 100]

data = {
    symbols[0]: read_price_data(symbol=symbols[0]),
    symbols[1]: read_price_data(symbol=symbols[1]),
}


def message_handler(message):
    try:
        # print(message)
        kline = message["k"]
        symbol = message["ps"]
        t = datetime.fromtimestamp(int(kline["t"] / 1000), tz=timezone.utc)
        print(f'{symbol} - {t} close {kline["c"]}')

        data[symbol] = data[symbol].append({"open_time": t, "close": kline["c"]}, ignore_index=True)
        data[symbol] = data[symbol].drop_duplicates(subset=['open_time'], keep='last')
        data[symbol].sort_index(inplace=True)
        data[symbol] = data[symbol].iloc[-100:].copy()
        data[symbol].to_csv(f"{DATA_PATH}/{symbol}.csv", index=False)
    except:
        traceback.print_exc()
        pass


my_client = Client()
my_client.start()

my_client.continuous_kline(
    pair=symbols[0],
    id=1,
    contractType="perpetual",
    interval=TF,
    callback=message_handler,
)

my_client.continuous_kline(
    pair=symbols[1],
    id=2,
    contractType="perpetual",
    interval=TF,
    callback=message_handler,
)

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        logging.debug("closing ws connection")
        my_client.stop()
        break
