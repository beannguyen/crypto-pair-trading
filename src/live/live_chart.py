import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.animation as animation

import matplotlib.pyplot as plt

import requests
import time
from datetime import datetime

from live.utils import get_pair


def to_dataframe(json_data):
    df = pd.DataFrame(
        json_data,
        columns=[
            "OpenTime",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "CloseTime",
            "BaseAssetVolume",
            "NbTrades",
            "TakerBuyVol",
            "TakerBuyBaseAssetVol",
            "Ignore",
        ],
    )
    df.drop(
        columns=[
            "CloseTime",
            "BaseAssetVolume",
            "NbTrades",
            "TakerBuyVol",
            "TakerBuyBaseAssetVol",
            "Ignore",
        ],
        inplace=True,
    )
    df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="ms")
    df.set_index(keys=["OpenTime"], inplace=True)

    df[["Open", "High", "Low", "Close", "Volume"]] = df[
        ["Open", "High", "Low", "Close", "Volume"]
    ].astype("float32")

    return df


def get_klines(base_url, symbol, interval="1m"):
    end_time = int(datetime.utcnow().timestamp() * 1000)
    url = f"{base_url}/klines?symbol={symbol}&limit=1000&interval={interval}&endTime={end_time}"

    headers = {"Content-Type": "application/json"}

    response = requests.request("GET", url, headers=headers)

    df = to_dataframe(response.json())
    return df


def get_futures_data(symbol, interval="1m"):
    #
    return get_klines("https://fapi.binance.com/fapi/v1", symbol, interval=interval)


def get_coinm_data(symbol, interval="1m"):
    #
    return get_klines("https://dapi.binance.com/dapi/v1", symbol, interval=interval)


def get_spot_data(symbol, interval="1m"):
    return get_klines("https://api.binance.com/api/v3", symbol, interval=interval)


def zscore(series):
    return (series - series.mean()) / np.std(series)


filename = input('Enter the file name ')
symbols, hedge, TF, precisions = get_pair(filename)
print(f'Start charting {symbols} {hedge} {TF}')


def concat_price(dfs):
    data = []

    for symbol, df in zip(symbols, dfs):
        df.columns = [symbol]
        # the data is too long, just limit to recent period
        data.append(np.log(df[symbol]))

    df = pd.concat(data, axis=1)
    df = df.dropna(axis=1, how="all")

    return df


DATA_PATH = "./feed"

# Create figure for plotting
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
xs = []
ys = []


# animation function.  This is called sequentially
def animate(i):
    try:
        with open(f"{DATA_PATH}/{symbols[0]}.csv", "r") as f1:
            df1 = pd.read_csv(f1, index_col=["open_time"])

        with open(f"{DATA_PATH}/{symbols[1]}.csv", "r") as f2:
            df2 = pd.read_csv(f2, index_col=["open_time"])

        df = concat_price([df1, df2])
        spread = df[symbols[0]] * hedge[0] + df[symbols[1]] * hedge[1]
        spread = spread.iloc[-1000:]
        _z = zscore(spread)

        # Draw x and y lists
        ax.clear()
        ax.plot(_z.index, _z.values)
        ax.axhline(_z.mean(), color="black")
        ax.axhline(2.0, color="red", linestyle="--")
        ax.axhline(1, color="red", linestyle="--")
        ax.axhline(-1, color="green", linestyle="--")
        ax.axhline(-2.0, color="green", linestyle="--")
        # ax.legend(["Spread z-score", "Mean", "+1", "-1", "+2", "-2"])
    except:
        pass


# Set up plot to call animate() function periodically
ani = animation.FuncAnimation(fig, animate, interval=5000)
plt.show()
