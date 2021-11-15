from datetime import timedelta

from notebooks.Strategies.CryptoPairTradingV1.backtest.base_strategy import BasePairTradingStrategy
from notebooks.Strategies.CryptoPairTradingV1.stats_arb.johansen import find_stationary_portfolio, calculate_spread, \
    test_stationary


class PairTradingStrategy(BasePairTradingStrategy):
    def __init__(self):
        super().__init__()

    @property
    def today(self):
        return self.datas[0].datetime.datetime(0)

    def next(self):

        # if no portfolio constructed yet
        if self.portfolio is None or (
                self.today - self.portfolio_constructed_at > timedelta(days=30) and not self.in_position):
            self.construct_portfolio()
        # else:
        #     if not self.in_position:
        #         if not self.test_stationary():
        #             self.construct_portfolio()

        self.trade()

    def construct_portfolio(self):
        current_date = self.datas[0].datetime.datetime(0)
        # we will use time range from current date and lookback period to construct portfolio
        _df = self.df[self.df.index <= current_date][-self.lookback:].copy()
        _df.dropna(axis=1, how='all', inplace=True)
        if len(_df) < self.lookback or len(_df.columns) < self.nb_symbols * 2:
            return

        stationary_df, coint_df = find_stationary_portfolio(_df, symbols=self.symbols, nb_symbols=self.nb_symbols)
        if len(stationary_df) == 0:
            return

        selected_row = stationary_df['i'].iloc[0]
        selected_pair = coint_df.iloc[selected_row]
        names = [n for n in coint_df.columns if 'sid' in n]
        self.portfolio = [d for d in self.datas if d._name in selected_pair[names].values]
        self.hedge_ratio = selected_pair['hedge_ratio']
        self.log(f'Constructing new portfolio {self.hedge_ratio}, {names}')

        # calculate the spread for all data, so we don't need to calculate the spread every steps
        # until the portfolio no longer mean reversion
        self.spread = calculate_spread(self.df, coint_df, selected_row, self.hedge_ratio, self.nb_symbols)
        self.portfolio_constructed_at = current_date

    def test_stationary(self):
        current_date = self.datas[0].datetime.datetime(0)
        spread = self.spread[self.spread.index <= current_date][-self.lookback:]
        return test_stationary(spread)

    def trade(self):
        if self.spread is None:
            return

        current_date = self.datas[0].datetime.datetime(0)
        spread = self.spread[self.spread.index <= current_date][-self.lookback:]
        # spread.plot()
        mean = spread.mean()
        std = spread.std()
        if (spread[current_date] > mean + 2 * std) and (self.status != 1) and not self.in_position:
            self.short_spread()
            self.sl_spread = spread[current_date] + std
        elif (spread[current_date] < mean - 2 * std) and (self.status != 2) and not self.in_position:
            self.long_spread()
            self.sl_spread = spread[current_date] - std
        elif self.in_position:
            # take profit
            if (self.status == 1 and spread[current_date] <= mean) \
                    or (self.status == 2 and spread[current_date] >= mean):
                self.log(f'Take profit ')
                self.close_all()

            # stop loss
            if (self.status == 1 and spread[current_date] >= self.sl_spread) \
                    or (self.status == 2 and spread[current_date] <= self.sl_spread):
                self.log(f'Stop Loss')
                self.close_all()

    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')