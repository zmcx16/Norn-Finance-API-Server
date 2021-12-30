import numpy as np


def historical_volatility(quotes, trading_days):
    log_returns = np.log(quotes / quotes.shift(1))
    # return square root * trading days * log_returns variance
    return np.sqrt(trading_days * log_returns.var())


def avg_historical_volatility(data, period=21, trading_days=252):
    total_cnt = len(data) - period + 1
    vol_sum = 0
    for i in range(total_cnt):
        vol_sum = vol_sum + historical_volatility(data[i:i+period], trading_days)

    return vol_sum / total_cnt


"""
EWMA
def exp_weight_historical_volatility(data, period=21, trading_days=252, ):
    total_cnt = len(data) - period + 1
    vol_sum = 0
    for i in range(total_cnt):
        vol_sum = vol_sum + historical_volatility(data[i:i+period], trading_days)

    return vol_sum / total_cnt
"""
