from datetime import datetime
import enum
import backtrader as bt
import numpy as np
from pykalman import KalmanFilter
import backtrader.feeds as btfeeds
import pandas as pd
import numpy as np

from backtest.backtest import print_analysis
from backtest.feed import zscore


DATA_PATH = "/mnt/d/Working/PersonalProjects/Trading/trading-agent/crypto-pair-trading/data/crypto/1h"


class PositionSide(enum.Enum):
    LONG = 1
    SHORT = 2


def read_data(symbol):
    df = pd.read_csv(
        f"{DATA_PATH}/{symbol}.csv",
        usecols=["open_time", "open", "high", "low", "close", "volume"],
        parse_dates=["open_time"],
        index_col=["open_time"],
    )
    df = df[~df.index.duplicated(keep="first")]
    # df = df.shift(1)
    df.dropna(inplace=True)

    return df


class PairTradingWithKalmanFilter(bt.Strategy):
    def __init__(self):
        super().__init__()

        self.bar_count = 0
        self.hedge_ratio = None
        self.in_position = False
        self.position_size = 0.1
        self.status = None
        self.delta = 1e-5
        self.trans_cov = self.delta / (1 - self.delta) * np.eye(2)
        self.lookback = 1425

    def log(self, txt, dt=None):
        # if self.p.printout:
        dt = dt or self.data.datetime[0]
        dt = bt.num2date(dt)
        print("%s, %s" % (dt.isoformat(), txt))

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(
            "OPERATION PROFIT, GROSS %s %.4f, NET %.4f"
            % (trade.data._name, trade.pnl, trade.pnlcomm)
        )
        # self.log(f"ACCOUNT VALUE: {self.broker.get_value()}")

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = (
                    f"BUY COMPLETE {order.data._name}, "
                    f"price: {order.executed.price:.4f}, "
                    f"size: {order.executed.size}, "
                    f"cost {order.executed.price * order.executed.size:.4f}"
                )
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = (
                    f"SELL COMPLETE {order.data._name}, "
                    f"price: {order.executed.price:.4f}, "
                    f"size: {order.executed.size}"
                    f"cost {order.executed.price * order.executed.size:.4f}"
                )
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log("Order cancelled %s ," % order.Status[order.status])
            pass  # Simply log

    @property
    def price1(self):
        d = self.data0.close.get(size=self.lookback)
        t = [bt.num2date(dt) for dt in self.data0.datetime.get(size=self.lookback)]
        s = pd.Series(d, index=t)

        return s

    @property
    def price2(self):
        d = self.data1.close.get(size=self.lookback)
        t = [bt.num2date(dt) for dt in self.data1.datetime.get(size=self.lookback)]
        s = pd.Series(d, index=t)

        return s

    def calculate_hedge_ratio(self):
        state_means, _ = self._recalculate_hedge_ratio(
            np.log(self.price1), np.log(self.price2)
        )
        return state_means[:, 0][0], state_means[:, 1][0]

    def _recalculate_hedge_ratio(self, price1, price2):
        obs_mat = np.vstack([price1, np.ones(price1.shape)]).T[:, np.newaxis]

        kf = KalmanFilter(
            n_dim_obs=1,
            n_dim_state=2,
            initial_state_mean=np.zeros(2),
            initial_state_covariance=np.ones((2, 2)),
            transition_matrices=np.eye(2),
            observation_matrices=obs_mat,
            observation_covariance=1.0,
            transition_covariance=self.trans_cov,
        )

        state_means, state_covs = kf.filter(price2.values)
        return state_means, state_covs

    def short_spread(self):
        allocated_cash = self.broker.getcash() * self.position_size
        size2 = allocated_cash / self.data1.close[0]
        size1 = allocated_cash / self.data0.close[0]

        self.sell(self.data1, size=size2)
        self.buy(self.data0, size=size1)
        self.status = PositionSide.SHORT
        self.in_position = True

    def long_spread(self):
        allocated_cash = self.broker.getcash() * self.position_size
        size2 = allocated_cash / self.data1.close[0]
        size1 = allocated_cash / self.data0.close[0]
        # size2 = 100
        # size1 = self.hedge_ratio * 100

        self.buy(self.data1, size=size2)
        self.sell(self.data0, size=size1)
        self.status = PositionSide.LONG
        self.in_position = True

    def close_all(self):
        self.close(self.data0)
        self.close(self.data1)
        self.in_position = False
        self.status = None

    def next(self):
        self.bar_count += 1
        if self.bar_count <= self.lookback:
            return

        if self.hedge_ratio == None or self.bar_count % self.lookback == 0:
            self.hedge_ratio, self.beta = self.calculate_hedge_ratio()

        spread = self.price2 - self.price1 * self.hedge_ratio - self.beta
        z_score = zscore(spread)

        if not self.in_position:
            if (
                (3 > z_score.iloc[-1] > 2)
                and (self.status != PositionSide.SHORT)
                and not self.in_position
            ):
                self.short_spread()
                self.log(f'Short spread at {self.data0.close[0]} {self.data1.close[0]}. zscore {z_score.iloc[-1]}')

            elif (
                (-3 < z_score.iloc[-1] < -2)
                and (self.status != PositionSide.LONG)
                and not self.in_position
            ):
                self.long_spread()
                self.log(f'Long spread at {self.data0.close[0]} {self.data1.close[0]}. zscore {z_score.iloc[-1]}')
        else:
            if (self.status == PositionSide.SHORT and z_score.iloc[-1] <= 0) or (
                self.status == PositionSide.LONG and z_score.iloc[-1] >= 0
            ):
                self.log(f"Take profit, zscore: {z_score.iloc[-1]}. Price {self.data0.close[0]} {self.data1.close[0]}")
                self.close_all()
            elif (self.status == PositionSide.SHORT and z_score.iloc[-1] > 4) or (
                self.status == PositionSide.LONG and z_score.iloc[-1] < -4
            ):
                self.log(f"Stop loss, zscore: {z_score.iloc[-1]}. Price {self.data0.close[0]} {self.data1.close[0]}")
                self.close_all()


def runstrategy():
    symbols = ["DOT-USDT", "LINK-USDT"]
    # Create a cerebro
    cerebro = bt.Cerebro()

    # Get the dates from the args
    fromdate = datetime.strptime("2020-10-01", "%Y-%m-%d")
    todate = datetime.strptime("2021-08-30", "%Y-%m-%d")

    for pair in symbols:
        df = read_data(pair)
        # Create the 1st data
        data0 = btfeeds.PandasData(
            dataname=df,
            # fromdate=fromdate,
            # todate=todate,
            plot=False,
        )

        # Add the 1st data to cerebro
        cerebro.adddata(data0, name=pair)

    # Add the strategy
    cerebro.addstrategy(PairTradingWithKalmanFilter)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addobserver(bt.observers.Value)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.Returns)
    cerebro.addanalyzer(bt.analyzers.DrawDown)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(10000)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(commission=0.0004, mult=3)

    # And run it
    results = cerebro.run()
    first_strat = results[0]
    print_analysis(first_strat.analyzers.ta.get_analysis())

    # Print out the final result
    print(
        f"Norm. Annual Return: {results[0].analyzers.returns.get_analysis()['rnorm100']:.2f}%"
    )
    print(
        f"Max Drawdown: {results[0].analyzers.drawdown.get_analysis()['max']['drawdown']:.2f}%"
    )


if __name__ == "__main__":
    runstrategy()
