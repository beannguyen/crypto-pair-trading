import numpy as np
import pandas as pd
import statsmodels.api as sm
from arch.unitroot import PhillipsPerron
from statsmodels.tsa.stattools import adfuller, kpss


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

