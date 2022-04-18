import pandas as pd
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from ta.volatility import BollingerBands
import matplotlib.pyplot as plt

from backtest.base_strategy import BasePairTradingStrategy


class SimplePairTradingStrategy(BasePairTradingStrategy):

    def __init__(self):
        super().__init__()
        assert self.p.names is not None, "Please select your pair "
        assert self.p.hedge_ratio is not None, "Please select your pair"
        self.names = self.p.names
        self.hedge_ratio = self.p.hedge_ratio

    @property
    def today(self):
        return self.datas[0].datetime.datetime(0)

    def next(self):
        current_date = self.datas[0].datetime.datetime(0)
        # if no portfolio constructed yet
        if self.portfolio is None:
            self.construct_portfolio()

        self.trade()

    def construct_portfolio(self):
        current_date = self.datas[0].datetime.datetime(0)

        self.portfolio = [d for d in self.datas if d._name in self.names]
        self.log(f'Constructing new portfolio {self.hedge_ratio}, {self.names}')

        # calculate the spread for all data, so we don't need to calculate the spread every steps
        # until the portfolio no longer mean reversion
        self.spread = self.calculate_spread(self.names)
        self.portfolio_constructed_at = current_date

    def calculate_spread(self, names) -> pd.Series:
        spread = None
        for i in range(self.nb_symbols):
            if spread is None:
                spread = self.df[names[i]] * self.hedge_ratio[i]
            else:
                spread += self.df[names[i]] * self.hedge_ratio[i]
        spread.dropna(inplace=True)
        return spread

    def trade(self):
        if self.spread is None:
            return

        current_date = self.datas[0].datetime.datetime(0)
        spread = self.spread[self.spread.index <= current_date][-self.lookback:].copy()
        spread_up = spread.mean() + 2 * spread.std()
        spread_dn = spread.mean() - 2 * spread.std()
        spread_m = spread.mean()
        # spread.plot()
        std = spread.std()
        if (spread.iloc[-1] <= spread_up <= spread.iloc[-2]) and (self.status != 1) and not self.in_position:
            self.short_spread()
            self.sl_spread = spread_up + std
        # elif (spread.iloc[-1] >= spread_dn >= spread.iloc[-2]) and (self.status != 2) and not self.in_position:
        #     self.long_spread()
        #     self.sl_spread = spread_dn - std
        elif self.in_position:
            # take profit
            if (self.status == 1 and spread[current_date] <= spread_m) \
                    or (self.status == 2 and spread[current_date] >= spread_m):
                self.log(f'Take profit ')
                self.close_all()

            # stop loss
            if (self.status == 1 and spread[current_date] >= spread_up + std) \
                    or (self.status == 2 and spread[current_date] <= spread_dn - std):
                self.log(f'Stop Loss')
                self.close_all()

    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('Ending   Cash - %.2f' % self.broker.getcash())
        print('==================================================')


