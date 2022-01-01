import numpy as np


def historical_volatility(quotes, trading_days):
    log_returns = np.log(quotes / quotes.shift(1))
    # return square root * trading days * log_returns variance
    return np.sqrt(trading_days * log_returns.var())


def avg_historical_volatility(data, period=21, trading_days=252):
    total_cnt = len(data) - period + 1 - 1  # -1 for shift data
    vol_sum = 0
    for i in range(total_cnt):
        vol_sum = vol_sum + historical_volatility(data[i:i+period+1], trading_days)  # +1 for shift data

    return vol_sum / total_cnt


def ewma_historical_volatility(data, period=21, trading_days=252, p_lambda=0.94):
    # a_i = l^(i-1) * (1-l) / (1-l^n)
    if p_lambda >= 1.0:
        p_lambda = 0.9999999999999999  # prevent divided by 0 if p_lambda = 1

    total_cnt = len(data) - period + 1 - 1  # -1 for shift data
    output = 0
    for i in range(total_cnt):
        alpha_i = total_cnt - i - 1  # i: start->latest data, alpha_i: latest->start data factor
        p_alpha_i = (p_lambda**alpha_i) * (1-p_lambda) / (1-p_lambda**total_cnt)
        output = output + p_alpha_i * historical_volatility(data[i:i+period+1], trading_days)

    return output
