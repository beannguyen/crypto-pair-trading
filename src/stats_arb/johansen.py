import numpy as np
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


def test_johansen(df, symbol_pairs, cointegrating_pairs):
    df_t = df[symbol_pairs].copy()

    # The second and third parameters indicate constant term, with a lag of 1.
    result = coint_johansen(df_t, 0, p)

    trace_crit_value = result.cvt[:, confidence_level_col]
    eigen_crit_value = result.cvm[:, confidence_level_col]
    # print("trace_crit_value",trace_crit_value)
    # print("eigen_crit_value",eigen_crit_value)
    # print("lr1",result.lr1)
    # print("lr2",result.lr2)

    # The trace statistic and maximum eigenvalue statistic are stored in lr1 and lr2;
    # see if they exceeded the confidence threshold
    if np.all(result.lr1 >= trace_crit_value) and np.all(result.lr2 >= eigen_crit_value):
        # print(f"{symbol_pairs} are cointegrated")
        # The first i.e. leftmost column of eigenvectors matrix, result.evec, contains the best weights.
        v1 = result.evec[:, 0:1]
        hr = v1 / -v1[1]
        # to get the hedge ratio divide the best_eigenvector by the negative of the second component of best_eigenvector
        # the regression will be: close of symbList[1] = hr[0]*close of symbList[0] + error
        # where the beta of the regression is hr[0], also known as the hedge ratio, and
        # the error of the regression is the mean reverting residual signal that you need to predict, it is also known as the "spread"
        # the spread = close of symbList[1] - hr[0]*close of symbList[0] or alternatively (the same thing):
        # do a regression with close of symbList[0] as x and lose of symbList[1] as y, and take the residuals of the regression to be the spread.
        coint_pair = dict(hedge_ratio=v1[:, 0])
        for i, s in enumerate(symbol_pairs):
            coint_pair[f'sid_{i + 1}'] = s

        cointegrating_pairs.append(coint_pair)

    return cointegrating_pairs
