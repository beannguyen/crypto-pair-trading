import itertools as it
import numpy as np
import pandas as pd
from statsmodels.tsa.vector_ar.vecm import coint_johansen

p = 1
COINTEGRATION_CONFIDENCE_LEVEL = 90

# the 90%, 95%, and 99% confidence levels for the trace statistic and maximum
# eigenvalue statistic are stored in the first, second, and third column of
# cvt and cvm, respectively
confidence_level_cols = {
    90: 0,
    95: 1,
    99: 2
}
confidence_level_col = confidence_level_cols[COINTEGRATION_CONFIDENCE_LEVEL]


def test_johansen(df, symbol_pairs):
    cointegration_pairs = []
    df_t = df[symbol_pairs].copy()

    # The second and third parameters indicate constant term, with a lag of 1.
    result = coint_johansen(df_t, 0, p)

    trace_crit_value = result.cvt[:, confidence_level_col]
    eigen_crit_value = result.cvm[:, confidence_level_col]

    # The trace statistic and maximum eigenvalue statistic are stored in lr1 and lr2;
    # see if they exceeded the confidence threshold
    if np.all(result.lr1 >= trace_crit_value) and np.all(result.lr2 >= eigen_crit_value):
        # print(f"{symbol_pairs} are cointegrated")
        # The first i.e. leftmost column of eigenvectors matrix, result.evec, contains the best weights.
        v1 = result.evec[:, 0:1]
        coint_pair = dict(hedge_ratio=v1[:, 0])
        for i, s in enumerate(symbol_pairs):
            coint_pair[f'sid_{i + 1}'] = s

        cointegration_pairs.append(coint_pair)

    return cointegration_pairs


def find_cointegration_pairs(df, symbols, nb_symbols=3):
    cointegration_pairs = []
    # get symbol pairs
    pairs = list(it.combinations(symbols, nb_symbols))

    for pair in pairs:
        try:
            cointegration_pairs = test_johansen(df, list(pair))
        except KeyError:
            pass

    coint_df = pd.DataFrame(cointegration_pairs)
    return coint_df
