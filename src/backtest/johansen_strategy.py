from backtest import BasePairTradingStrategy


class JohansenPairTradingStrategy(BasePairTradingStrategy):
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

        self.zscore = self.data4.zscore

    def next(self):
        pass
