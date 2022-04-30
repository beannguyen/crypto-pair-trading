from datetime import datetime

import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd

from backtest import feed
from backtest.base_strategy import BasePairTradingStrategy
from backtest.feed import zscore
from stats_arb.johansen import find_cointegration_pairs

DATA_PATH = "/mnt/d/Working/PersonalProjects/Trading/trading-agent/crypto-pair-trading/data/crypto/1m"

symbols = [
    "BTC",
    "ETH",
    "ADA",
    "BNB",
    "SOL",
    "XRP",
    "DOGE",
    "DOT",
    "LUNA",
    "UNI",
    "LINK",
    "BCH",
    "LTC",
    "ALGO",
    "AVAX",
    "ICP",
    "WBTC",
    "FTT",
    "MATIC",
    "FIL",
    "XLM",
    "VET",
    "ETC",
    "TRX",
    "THETA",
]
symbols = [f"{s}USDT" for s in symbols]

PAIR = ["ADAUSDT", "DOTUSDT"]
HEDGE_RATIO = [322.98447988, -83.42285969]


def read_data(symbol):
    df = pd.read_csv(
        f"{DATA_PATH}/{symbol}.csv",
        usecols=["open_time", "open", "high", "low", "close", "volume"],
        parse_dates=["open_time"],
        index_col=["open_time"],
    )
    df = df[~df.index.duplicated(keep="first")]
    df = df.shift(1)
    df.dropna(inplace=True)

    return df


class PairTradingStrategy(BasePairTradingStrategy):
    params = dict(zscore=None)

    def __init__(self):
        super().__init__()
        self.names = PAIR
        self.portfolio = [d for d in self.datas if d._name in self.names]
        self.hedge_ratio = HEDGE_RATIO
        self.zscore = self.p.zscore
        
        self.nb_symbols = 2
        self.sl_upper_limit = 2
        self.upper_limit = 1
        self.lower_limit = -1
        self.sl_lower_limit = -2

    def next(self):
        self.trade()

    def trade(self):
        dt = self.data.datetime[0]
        dt = bt.num2date(dt)
        zscore = self.zscore.loc[dt.isoformat()]

        if (
            (zscore > self.upper_limit)
            and (self.status != 1)
            and not self.in_position
        ):
            self.log(f'Short spread zscore {zscore} > {self.upper_limit}')
            self.short_spread()
        elif (
            (zscore < self.lower_limit)
            and (self.status != 2)
            and not self.in_position
        ):
            self.log(f'Long spread zscore {zscore} > {self.upper_limit}')
            self.long_spread()
        elif self.getposition(self.data0) and (
            (zscore <= -1 and self.status == 1)
            or (zscore >= 1 and self.status == 2)
        ):
            self.log(f'Take profit {zscore} <> 0')
            self.close_all()
        
        elif self.getposition(self.data0) and (
            (zscore >= self.sl_upper_limit and self.status == 1)
            or (zscore <= self.sl_lower_limit and self.status == 2)
        ):
            self.log(f'Stop Loss {zscore} <> +- 2')
            self.close_all()

    def stop(self):
        print("==================================================")
        print("Starting Value - %.2f" % self.broker.startingcash)
        print("Ending   Value - %.2f" % self.broker.getvalue())
        print("==================================================")


def calculate_zscore(data):
    hedge_ratios = HEDGE_RATIO
    s1 = data[PAIR[0]]
    s2 = data[PAIR[1]]

    spread = s1 * hedge_ratios[0] + hedge_ratios[1] * s2
    zscore_df = zscore(spread)  # .to_frame()
    # zscore_df.columns = ["zscore"]
    # zscore_df.dropna(inplace=True)
    return zscore_df


def print_analysis(analyzer):
    """
    Function to print the Technical Analysis results in a nice format.
    """
    # Get the results we are interested in
    total_open = analyzer.total.open
    total_closed = analyzer.total.closed
    total_won = analyzer.won.total
    total_lost = analyzer.lost.total
    win_streak = analyzer.streak.won.longest
    lose_streak = analyzer.streak.lost.longest
    pnl_net = round(analyzer.pnl.net.total, 2)
    strike_rate = (total_won / total_closed) * 100
    # Designate the rows
    h1 = ["Total Open", "Total Closed", "Total Won", "Total Lost"]
    h2 = ["Strike Rate", "Win Streak", "Losing Streak", "PnL Net"]
    r1 = [total_open, total_closed, total_won, total_lost]
    r2 = [strike_rate, win_streak, lose_streak, pnl_net]
    # Check which set of headers is the longest.
    if len(h1) > len(h2):
        header_length = len(h1)
    else:
        header_length = len(h2)
    # Print the rows
    print_list = [h1, r1, h2, r2]
    row_format = "{:<15}" * (header_length + 1)
    print("Trade Analysis Results:")
    for row in print_list:
        print(row_format.format("", *row))


def runstrategy():
    # Create a cerebro
    cerebro = bt.Cerebro()

    # Get the dates from the args
    fromdate = datetime.strptime("2021-06-01", "%Y-%m-%d")
    todate = datetime.strptime("2021-08-30", "%Y-%m-%d")

    pairs = PAIR
    data = {}
    for pair in pairs:
        df = read_data(pair)
        data[pair] = df.close
        # Create the 1st data
        data0 = btfeeds.PandasData(
            dataname=df,
            # fromdate=fromdate,
            # todate=todate,
            plot=False,
        )

        # Add the 1st data to cerebro
        cerebro.adddata(data0, name=pair)

    zscore_df = calculate_zscore(data)
    # zscore_data = feed.ZScoreData(
    #     dataname=zscore_df,
    #     # fromdate=fromdate,
    #     # todate=todate,
    #     plot=False,
    # )
    # cerebro.adddata(zscore_data, name="zscore")

    # Add the strategy
    cerebro.addstrategy(PairTradingStrategy, zscore=zscore_df)

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

    # Plot if requested
    # cerebro.plot(numfigs=1, volume=False, zdown=False)


if __name__ == "__main__":
    runstrategy()
