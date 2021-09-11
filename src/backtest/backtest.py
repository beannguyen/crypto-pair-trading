from datetime import datetime

import backtrader as bt
import backtrader.feeds as btfeeds
import numpy as np
import pandas as pd

from src.backtest import BasePairTradingStrategy, feed
from src.backtest.feed import zscore

DATA_PATH = '../data/5min'


def read_data(symbol):
    df = pd.read_csv(f'{DATA_PATH}/{symbol}.csv', usecols=['open_time', 'open', 'high', 'low', 'close', 'volume'],
                     parse_dates=['open_time'], index_col=['open_time'])
    df = df[~df.index.duplicated(keep='first')]
    df = df.shift(1)
    df.dropna(inplace=True)

    return df


class PairTradingStrategy(BasePairTradingStrategy):
    params = dict(
        period=10,
        stake=10,
        qty1=0,
        qty2=0,
        qty3=0,
        printout=True,
        upper=2.1,
        lower=-2.1,
        up_medium=0.5,
        low_medium=-0.5,
        status=0,
        portfolio_value=10000,
    )

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.qty1 = self.p.qty1
        self.qty2 = self.p.qty2
        self.qty3 = self.p.qty3
        self.upper_limit = self.p.upper
        self.lower_limit = self.p.lower
        self.up_medium = self.p.up_medium
        self.low_medium = self.p.low_medium
        self.status = self.p.status
        self.portfolio_value = self.p.portfolio_value

        self.zscore = self.data3.zscore

    def next(self):
        if self.orderid:
            return  # if an order is active, no new orders are allowed

        # Step 2: Check conditions for SHORT & place the order
        # Checking the condition for SHORT
        if (self.zscore[0] > self.upper_limit) and (self.status != 1):
            # Calculating the number of shares for each stock
            value = (1 / 3) * self.portfolio_value  # Divide the cash equally
            s1 = int(value / self.data0.close)
            s2 = int(value / self.data1.close)
            s3 = int(value / self.data2.close)
            print('s1 + self.qty1 is', s1 + self.qty1)
            print('s2 + self.qty1 is', s2 + self.qty2)
            print('s3 + self.qty1 is', s3 + self.qty3)

            # Placing the order
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % (self.data0._name, self.data0.close[0], s1 + self.qty1))
            self.sell(data=self.data0, size=(s1 + self.qty1))
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % (self.data1._name, self.data1.close[0], s2 + self.qty2))
            self.buy(data=self.data1, size=(s2 + self.qty2))
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % (self.data2._name, self.data2.close[0], s3 + self.qty3))
            self.buy(data=self.data2, size=(s3 + self.qty3))

            # Updating the counters with new value
            self.qty1 = s1  # The new open position quantity for Stock1 is s1 shares
            self.qty2 = s2  # The new open position quantity for Stock2 is s2 shares
            self.qty3 = s3  # The new open position quantity for Stock3 is s3 shares

            self.status = 1  # The current status is "short the spread"
        elif (self.zscore[0] < self.lower_limit) and (self.status != 2):
            # Step 3: Check conditions for LONG & place the order
            # Checking the condition for LONG
            value = (1 / 3) * self.portfolio_value  # Divide the cash equally
            s1 = int(value / self.data0.close)
            s2 = int(value / self.data1.close)
            s3 = int(value / self.data2.close)
            print('s1 + self.qty1 is', s1 + self.qty1)
            print('s2 + self.qty1 is', s2 + self.qty2)
            print('s3 + self.qty1 is', s3 + self.qty3)

            # Place the order
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % (self.data0._name, self.data0.close[0], s1 + self.qty1))
            self.buy(data=self.data0, size=(s1 + self.qty1))  # Place an order for buying x + qty1 shares
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % (self.data1._name, self.data1.close[0], s2 + self.qty2))
            self.sell(data=self.data1, size=(s2 + self.qty2))  # Place an order for selling y + qty2 shares
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % (self.data2._name, self.data2.close[0], s3 + self.qty2))
            self.sell(data=self.data2, size=(s3 + self.qty2))  # Place an order for selling y + qty2 shares

            # Updating the counters with new value
            self.qty1 = s1
            self.qty2 = s2
            self.qty3 = s3
            self.status = 2


def calculate_zscore(data):
    hedge_ratios = [12.77306775, -7.67378009, -13.42090259]
    s1 = np.log(data['DOT-USDT'])
    s2 = np.log(data['LINK-USDT'])
    s3 = np.log(data['BCH-USDT'])
    spread = s1 * hedge_ratios[0] + s2 * hedge_ratios[1] + s3 * hedge_ratios[2]
    zscore_df = zscore(spread).to_frame()
    zscore_df.columns = ['zscore']
    zscore_df.dropna(inplace=True)
    return zscore_df


def runstrategy():
    # Create a cerebro
    cerebro = bt.Cerebro()

    # Get the dates from the args
    fromdate = datetime.strptime('2021-06-01', '%Y-%m-%d')
    todate = datetime.strptime('2021-08-30', '%Y-%m-%d')

    pairs = ['DOT-USDT', 'LINK-USDT', 'BCH-USDT']
    data = {}
    for pair in pairs:
        df = read_data(pair)
        data[pair] = df.close
        # Create the 1st data
        data0 = btfeeds.PandasData(
            dataname=df,
            fromdate=fromdate,
            todate=todate,
            plot=False)

        # Add the 1st data to cerebro
        cerebro.adddata(data0, name=pair)

    zscore_df = calculate_zscore(data)
    zscore_data = feed.ZScoreData(dataname=zscore_df,
                                  fromdate=fromdate,
                                  todate=todate)
    cerebro.adddata(zscore_data, name='zscore')

    # Add the strategy
    cerebro.addstrategy(PairTradingStrategy,
                        period=10,
                        stake=10)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(100000)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(commission=0.005)

    # And run it
    cerebro.run()

    # Plot if requested
    # cerebro.plot(numfigs=1, volume=False, zdown=False)


if __name__ == '__main__':
    runstrategy()
