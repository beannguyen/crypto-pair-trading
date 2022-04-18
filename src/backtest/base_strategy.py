import backtrader as bt
import numpy as np

LONG = 'long'
SHORT = 'short'


class BasePairTradingStrategy(bt.Strategy):
    params = dict(
        lookback=1000,
        printout=True,
        status=0,
        df=None,
        nb_symbols=4,
        symbols=None,
        names=None,
        hedge_ratio=None
    )

    def __init__(self):
        self.status = self.p.status
        self.nb_symbols = self.p.nb_symbols
        self.lookback = self.p.lookback
        self.df = self.p.df
        self.in_position = False
        self.portfolio = None
        self.zscore = None
        self.spread = None
        self.portfolio_constructed_at = None
        self.sl_spread = None
        self.hedge_ratio = self.p.hedge_ratio
        self.symbols = self.p.symbols

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.4f, NET %.4f' %
                 (trade.pnl, trade.pnlcomm))
        self.log(f'ACCOUNT VALUE: {self.broker.get_value()}')

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = f'BUY COMPLETE {order.data._name}, ' \
                         f'price: {order.executed.price:.4f}, ' \
                         f'size: {order.executed.size}, ' \
                         f'cost {order.executed.price * order.executed.size:.4f}'
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = f'SELL COMPLETE {order.data._name}, ' \
                          f'price: {order.executed.price:.4f}, ' \
                          f'size: {order.executed.size}' \
                          f'cost {order.executed.price * order.executed.size:.4f}'
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('Order cancelled %s ,' % order.Status[order.status])
            pass  # Simply log

    def short_spread(self):
        self.log(f'GO SHORT')
        self.place_orders(SHORT)
        self.status = 1  # The current status is "short the spread"
        self.in_position = True

    def long_spread(self):
        self.log(f'GO LONG')
        self.place_orders(LONG)
        self.status = 2
        self.in_position = True

    def place_orders(self, side):
        weights = self.cal_weights(side)
        value = self.broker.get_value() * 0.3
        for i in range(self.nb_symbols):
            size = (value * np.sign(weights[i])) / self.portfolio[i].close[0]  # * weights[i]
            # size = self.hedge_ratio[i] * 0.05

            if size > 0:
                self.buy(data=self.portfolio[i], size=size)
            else:
                self.sell(data=self.portfolio[i], size=abs(size))

    def cal_weights(self, side):
        weights = []
        total_cap = 0

        hedge_ratio = self.hedge_ratio

        if side == SHORT:
            hedge_ratio = [-h for h in self.hedge_ratio]

        for i in range(self.nb_symbols):
            hedge_r = hedge_ratio[i]
            total_cap += self.portfolio[i].close * abs(hedge_r)

        for i in range(self.nb_symbols):
            size_i = self.portfolio[i].close * hedge_ratio[i] / total_cap
            weights.append(size_i)

        return weights

    def close_all(self):
        for i in range(self.nb_symbols):
            self.close(self.portfolio[i])
        self.status = 0
        self.in_position = False
