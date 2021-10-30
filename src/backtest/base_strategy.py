import backtrader as bt


class BasePairTradingStrategy(bt.Strategy):
    params = dict(
        period=10,
        stake=10,
        qty1=0,
        qty2=0,
        qty3=0,
        printout=True,
        upper=1,
        lower=-1,
        up_medium=0.8,
        low_medium=-0.8,
        status=0,
        portfolio_value=1000,
        df=None
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
        self.df = self.p.df
        self.in_position = False

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = 'BUY COMPLETE, %.2f' % order.executed.price
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = 'SELL COMPLETE, %.2f' % order.executed.price
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('%s ,' % order.Status[order.status])
            pass  # Simply log

    def short_spread(self):
        # Calculating the number of shares for each stock
        value = (1 / 3) * self.portfolio_value  # Divide the cash equally
        s1 = value / self.data0.close
        s2 = value / self.data1.close
        s3 = value / self.data2.close
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
        self.in_position = True

    def long_spread(self):
        value = (1 / 3) * self.portfolio_value  # Divide the cash equally
        s1 = value / self.data0.close
        s2 = value / self.data1.close
        s3 = value / self.data2.close
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
        self.in_position = True

    def close_all(self):
        self.close(self.data0)
        self.close(self.data1)
        self.close(self.data2)
        self.status = 0
        self.in_position = False
