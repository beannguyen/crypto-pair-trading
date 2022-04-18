import itertools as it

import numpy as np
import pandas as pd
from statsmodels.tsa.vector_ar.vecm import coint_johansen

from stats_arb.tests import adf_test, cal_half_life, kpss_test, pp_test


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

        return coint_pair

    return None


def find_cointegration_pairs(df, symbols, nb_symbols=3):
    cointegration_pairs = []
    # get symbol pairs
    pairs = list(it.combinations(symbols, nb_symbols))

    for pair in pairs:
        try:
            res = test_johansen(df, list(pair))
            if res is not None:
                cointegration_pairs.append(res)
        except Exception:
            # traceback.print_exc()
            pass

    coint_df = pd.DataFrame(cointegration_pairs)
    return coint_df


def calculate_spread(df, coint_df, selected_row, hedge_ratio, nb_symbols=3):
    spread = None
    for i in range(nb_symbols):
        if spread is None:
            spread = df[coint_df[f'sid_{i + 1}'].iloc[selected_row]] * hedge_ratio[i]
        else:
            spread += df[coint_df[f'sid_{i + 1}'].iloc[selected_row]] * hedge_ratio[i]
    return spread


critical_val = 0.005


def test_stationary(spread):
    adf_p = adf_test(spread)
    pp_p = pp_test(spread)
    kpss_p = kpss_test(spread)

    if adf_p < critical_val < kpss_p and pp_p < critical_val:
        return True

    return False


def find_stationary_portfolio(df, symbols, nb_symbols=3):
    coint_df = find_cointegration_pairs(df, symbols, nb_symbols=nb_symbols)

    data = []
    for i, _ in coint_df.iterrows():
        selected_row = i
        hedge_ratio = coint_df.iloc[selected_row]['hedge_ratio']
        spread = calculate_spread(df, coint_df, selected_row, hedge_ratio, nb_symbols)
        # adf_p = adf_test(spread)
        # pp_p = pp_test(spread)
        # kpss_p = kpss_test(spread)
        #
        # if adf_p < critical_val < kpss_p and pp_p < critical_val:
        #
        half_life = cal_half_life(spread)
        pairs_name = coint_df[[col for col in coint_df.columns if col != 'hedge_ratio']].iloc[0].values
        # print(i, pairs_name, 'is stationary with half life', half_life)
        # print(' ')
        data.append({
            'i': i,
            'pairs': pairs_name,
            'half_life': half_life
        })

    stationary_df = pd.DataFrame(data)
    if len(stationary_df) == 0:
        return stationary_df, coint_df

    stationary_df.sort_values(by=['half_life'], inplace=True)
    return stationary_df, coint_df
