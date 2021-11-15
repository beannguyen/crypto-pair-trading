from datetime import datetime

import backtrader as bt
import backtrader.feeds as btfeeds
import numpy as np
import pandas as pd

from notebooks.Strategies.CryptoPairTradingV1.backtest.simple_pair import SimplePairTradingStrategy

DATA_PATH = './data/stock/vn30'


def read_data(symbol):
    df = pd.read_csv(f'{DATA_PATH}/{symbol}.csv',
                     parse_dates=['timestamp'],
                     index_col=['timestamp'])
    df = df[~df.index.duplicated(keep='first')]
    # df = df.shift(1)
    df.dropna(inplace=True)

    return df


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
    strike_rate = round((total_won / total_closed) * 100, 2)
    # Designate the rows
    h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost']
    h2 = ['Strike Rate', 'Win Streak', 'Losing Streak', 'PnL Net']
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
        print(row_format.format('', *row))


def runstrategy():
    # Create a cerebro
    cerebro = bt.Cerebro()

    symbols = ['VN30F1M', 'HDB', 'TPB', 'VIC']

    # Get the dates from the args
    fromdate = datetime.strptime('2020-01-01', '%Y-%m-%d')
    todate = datetime.strptime('2021-10-01', '%Y-%m-%d')

    data = []
    for pair in symbols:
        df = read_data(pair)

        # Create the 1st data
        data0 = btfeeds.PandasData(
            dataname=df,
            fromdate=fromdate,
            # todate=todate,
            plot=False)

        # Add the 1st data to cerebro
        cerebro.adddata(data0, name=pair)

        _df = df.copy()
        _df = _df.rename(columns={'close': pair})
        data.append(np.log(_df[pair]))

    df = pd.concat(data, axis=1)
    # Add the strategy
    cerebro.addstrategy(SimplePairTradingStrategy, df=df, names=symbols,
                        hedge_ratio=[36.19455479, -20.40481992, -13.08806022, -15.34892716],
                        nb_symbols=4, lookback=12*20)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addobserver(bt.observers.Value)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.Returns)
    cerebro.addanalyzer(bt.analyzers.DrawDown)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(100000)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(commission=0.0025)

    # And run it
    results = cerebro.run()
    first_strat = results[0]
    print_analysis(first_strat.analyzers.ta.get_analysis())

    # Print out the final result
    print(f"Norm. Annual Return: {results[0].analyzers.returns.get_analysis()['rnorm100']:.2f}%")
    print(f"Max Drawdown: {results[0].analyzers.drawdown.get_analysis()['max']['drawdown']:.2f}%")

    # Plot if requested
    cerebro.plot(numfigs=1, volume=False, zdown=False)


if __name__ == '__main__':
    runstrategy()
