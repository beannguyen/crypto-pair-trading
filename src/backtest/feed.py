import backtrader as bt
import numpy as np
import pandas as pd


class ZScoreData(bt.feeds.PandasData):
    params = (
        ('zscore', -1),
    )

    lines = (
        'zscore',
    )


def zscore(series) -> pd.Series:
    return (series - series.mean()) / np.std(series)
