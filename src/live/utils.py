from itsdangerous import json
import pandas as pd
import numpy as np
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from datetime import datetime


import requests
from datetime import datetime


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
    df.columns = [c.lower() for c in df.columns]

    return df


def get_klines(base_url, symbol, interval="1m"):
    end_time = int(datetime.now().timestamp() * 1000)
    url = f"{base_url}/klines?symbol={symbol}&limit=1000&interval={interval}"

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


def get_close_price(symbol, interval="1m"):
    df = get_futures_data(symbol=symbol, interval=interval)
    df.drop(columns=["open", "high", "low", "volume"], inplace=True)
    return df


def get_pair(filename):
    with open(f"live/{filename}.json") as f:
        data = json.load(f)
        return data["pair"], data["ratios"], data["tf"], data["precisions"]
