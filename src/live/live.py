import logging
import time
from binance.futures import Futures as Client
from binance.lib.utils import config_logging
from black import InvalidInput
import numpy as np
import pandas as pd

from live.utils import zscore


BASE_URL = "https://testnet.binancefuture.com"

config_logging(logging, logging.DEBUG)

futures_client = Client(
    key="hexOZJzHOvADRn6ock1iUMCbT3aLZuY2gzLzTq89DXAflBWO4DXf1zQK0NJscrtV",
    secret="RqTwt7XPa8wex68sAI62lUobSVpCOkbSIenvlDHmo42prdBCJz8NrtBqA1E3QGSH",
    # base_url=BASE_URL,
)


def get_depth(symbol):
    depth = futures_client.depth(symbol, **{"limit": 5})
    depth["bids"] = sorted(depth["bids"], key=lambda x: x[0], reverse=True)
    depth["asks"] = sorted(depth["asks"], key=lambda x: x[0], reverse=False)
    return depth


def new_order(symbol, side, quantity, price):
    order = futures_client.new_order(
        **{
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "price": price,
            "quantity": quantity,
            "timeInForce": "GTC",
        }
    )
    return order


def get_size(usdt_qty, price, precision):
    return round(usdt_qty / float(price), precision)


def long_spread():
    depth1 = get_depth(symbols[0])["asks"][0]
    depth2 = get_depth(symbols[1])["bids"][0]

    order1 = new_order(
        symbol=symbols[0],
        side="BUY",
        quantity=get_size(qty_[0], depth1[0], precision=precisions[0]),
        price=depth1[0],
    )
    order2 = new_order(
        symbol=symbols[1],
        side="SELL",
        quantity=get_size(qty_[1], depth2[0], precision=precisions[1]),
        price=depth2[0],
    )

    print(f'Long order {order1["orderId"]} for {symbols[0]} at {depth1[0]}')
    print(f'Short order {order2["orderId"]} for {symbols[1]} at {depth1[1]}')

    return order1, order2


def short_spread():
    depth1 = get_depth(symbols[0])["bids"][0]
    depth2 = get_depth(symbols[1])["asks"][0]

    order1 = new_order(
        symbol=symbols[0],
        side="SELL",
        quantity=get_size(qty_[0], depth1[0], precision=precisions[0]),
        price=depth1[0],
    )
    order2 = new_order(
        symbol=symbols[1],
        side="BUY",
        quantity=get_size(qty_[1], depth2[0], precision=precisions[1]),
        price=depth2[0],
    )

    print(f'Short order {order1["orderId"]} for {symbols[0]} at {depth1[0]}')
    print(f'Long order {order2["orderId"]} for {symbols[1]} at {depth1[1]}')

    return order1, order2


def close_order(order):
    depth_side = "bids" if order["side"] == "BUY" else "asks"
    side = "SELL" if order["side"] == "BUY" else "BUY"

    depth1 = get_depth(order["symbol"])[depth_side][0]
    org_order = futures_client.get_all_orders(
        symbol=order["symbol"], orderId=order["orderId"]
    )[0]

    order = new_order(
        symbol=order["symbol"],
        side=side,
        quantity=org_order["executedQty"],
        price=depth1[0],
    )
    print(f'Closed order {order["orderId"]} for {symbols[0]} at {depth1[0]}')


def close_all(order1, order2):
    close_order(order1)
    close_order(order2)


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
symbols = ['XRPUSDT','ADAUSDT']
hedge = [30.68800711, -158.6123163]
precisions = [1, 0]

qty_ = [200, 200]
order1, order2 = None, None

upper_limit, middle, lower_limit = 1.5, 0, -1.5
sl_upper, sl_lower = 3, -3
status = 0
position = False

for symbol in symbols:
    futures_client.change_leverage(symbol=symbol, leverage=2, recvWindow=6000)


print("Resuming...")
a1 = input("Do you have any open position:")
if a1 == "y":
    position = True

    a2 = input("The last position is?:")
    if a2 == "long":
        status = 2
    elif a2 == "short":
        status = 1
    else:
        raise InvalidInput


while True:
    with open(f"{DATA_PATH}/{symbols[0]}.csv", "r") as f1:
        with open(f"{DATA_PATH}/{symbols[1]}.csv", "r") as f2:
            try:
                df1 = pd.read_csv(f1, index_col=["open_time"])
                df2 = pd.read_csv(f2, index_col=["open_time"])

                df = concat_price([df1, df2])
                spread = df[symbols[0]] * hedge[0] + df[symbols[1]] * hedge[1]
                spread = spread.iloc[:200]
                _z = zscore(spread)

                if (
                    sl_upper > _z.iloc[-1] > upper_limit
                    and status != 1
                    and not position
                ):
                    print(f"Short spread at zscore {_z.iloc[-1]} > {upper_limit}")
                    order1, order2 = short_spread()
                    status = 1
                    position = True

                elif (
                    sl_lower < _z.iloc[-1] < lower_limit
                    and status != 2
                    and not position
                ):
                    print(f"Long spread at zscore {_z.iloc[-1]} > {upper_limit}")
                    order1, order2 = long_spread()
                    status = 2
                    position = True

                elif position and (
                    (_z.iloc[-1] <= 0 and status == 1)
                    or (_z.iloc[-1] >= 0 and status == 2)
                ):
                    print(f"Take profit {_z.iloc[-1]} <> 0")
                    close_all(order1, order2)
                    position = False
                    status = 0

                elif position and (
                    (_z.iloc[-1] >= sl_upper and status == 1)
                    or (_z.iloc[-1] <= sl_lower and status == 2)
                ):
                    print(f"Stop loss {_z.iloc[-2]} +- 2")
                    close_all(order1, order2)
                    position = False
                    status = 0

                time.sleep(0.5)
            except KeyboardInterrupt:
                break
            except pd.errors.EmptyDataError as e:
                print(e)
                time.sleep(0.1)
