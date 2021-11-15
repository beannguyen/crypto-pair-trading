import numpy as np
import pandas as pd

from notebooks.Strategies.CryptoPairTradingV1.stats_arb.johansen import find_stationary_portfolio, calculate_spread

symbols = ['BTC', 'ETH', 'ADA', 'BNB', 'XRP', 'LINK', 'LTC', 'ETC']
symbols = [f'{s}-USDT' for s in symbols]

data = []
start_date = '2019-01-01'
test_end = '2020-01-01'
end_date = '2020-06-01'
DATA_PATH = '../data/1h'


def read_data():
    for symbol in symbols:
        try:
            file = f'{DATA_PATH}/{symbol}.csv'
            df = pd.read_csv(file,
                             parse_dates=['open_time'],
                             index_col=['open_time'])
            df = df[df.index < end_date].copy()
            df = df[~df.index.duplicated(keep='first')]

            df.rename(columns={'close': symbol}, inplace=True)
            # the data is too long, just limit to recent period
            data.append(np.log(df[symbol]))
        except:
            pass

    df = pd.concat(data, axis=1)
    df = df.dropna(axis=1, how='all')
    return df


def main(nb_symbols=3):
    df = read_data()
    df = df[(df.index <= test_end) & (df.index >= start_date)].copy()
    stationary_df, coint_df = find_stationary_portfolio(df, symbols=symbols, nb_symbols=nb_symbols)
    if len(stationary_df) == 0:
        return

    selected_row = stationary_df['i'].iloc[0]
    selected_pair = coint_df.iloc[selected_row]
    hedge_ratio = selected_pair['hedge_ratio']
    spread = calculate_spread(df, coint_df, selected_row, hedge_ratio, nb_symbols)
    spread.plot()


if __name__ == '__main__':
    main(nb_symbols=2)
