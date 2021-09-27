import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller, kpss
from arch.unitroot import PhillipsPerron
import statsmodels.api as sm
import itertools as it

from stats_arb.johansen import test_johansen


def kpss_test(timeseries, verbose=False):
    kpsstest = kpss(timeseries, nlags="auto")
    kpss_output = pd.Series(kpsstest[0:3], index=[
        'Test Statistic', 'p-value', 'Lags Used'])

    if verbose:
        print('Results of KPSS Test:')
        for key, value in kpsstest[3].items():
            kpss_output['Critical Value (%s)' % key] = value

        p_val = kpss_output['p-value']
        print(
            f'Result: The series is {"not " if p_val < 0.05 else ""}stationary')
    return kpss_output['p-value']


def adf_test(timeseries, verbose=False):
    dftest = adfuller(timeseries)
    dfoutput = pd.Series(dftest[0:4], index=[
        'Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])

    if verbose:
        print('Results of Dickey-Fuller Test:')
        for key, value in dftest[4].items():
            dfoutput['Critical Value (%s)' % key] = value
        p_val = dfoutput['p-value']
        print(
            f'Result: The series is {"" if p_val < 0.05 else " not "} stationary')
    return dfoutput['p-value']


def hurst(ts):
    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)

    # Calculate the array of the variances of the lagged differences
    tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a linear fit to estimate the Hurst Exponent
    poly = np.polyfit(np.log10(lags), np.log10(tau), 1)

    # Return the Hurst exponent from the polyfit output
    return poly[0] * 2.0


def pp_test(timeseries, verbose=False):
    pp = PhillipsPerron(timeseries)
    if verbose:
        print(pp.summary().as_text())

    return pp.pvalue


def cal_half_life(spread):
    lag = np.roll(spread, 1)
    lag[0] = 0
    ret = spread - lag
    ret[0] = 0

    # adds intercept terms to X variable for regression
    lag2 = sm.add_constant(lag)

    model = sm.OLS(ret, lag2)
    res = model.fit()

    return -np.log(2) / res.params[1]


def find_coint_pairs(df, symbols, nb_symbols):
    cointegrating_pairs = []

    # get symbol pairs
    pairs = list(it.combinations(symbols, nb_symbols))

    for pair in pairs:
        try:
            cointegrating_pairs = test_johansen(df, list(pair), cointegrating_pairs)
        except KeyError:
            pass

    return pd.DataFrame(cointegrating_pairs)


def calculate_spread(df, coint_df, nb_symbols, selected_row, hedge_ratio):
    spread = None
    for i in range(nb_symbols):
        if spread is None:
            spread = df[coint_df[f'sid_{i + 1}'].iloc[selected_row]] * hedge_ratio[i]
        else:
            spread += df[coint_df[f'sid_{i + 1}'].iloc[selected_row]] * hedge_ratio[i]
    return spread


critical_val = 0.005


def find_stationary_portfolio(df: pd.DataFrame, coint_df: pd.DataFrame, nb_symbols) -> pd.DataFrame:
    data = []
    for i, _ in coint_df.iterrows():
        selected_row = i
        hedge_ratio = coint_df.iloc[selected_row]['hedge_ratio']
        spread = calculate_spread(df, coint_df, nb_symbols, selected_row, hedge_ratio)
        adf_p = adf_test(spread)
        pp_p = pp_test(spread)
        kpss_p = kpss_test(spread)

        if adf_p < critical_val < kpss_p and pp_p < critical_val:
            half_life = cal_half_life(spread)
            pairs_name = coint_df[[col for col in coint_df.columns if col != 'hedge_ratio']].iloc[0].values
            # print(i, pairs_name, 'is stationary with half life', half_life)
            # print(' ')
            data.append({
                'i': i,
                'pairs': pairs_name,
                'half_life': half_life
            })

    return pd.DataFrame(data)
