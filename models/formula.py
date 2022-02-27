import logging
import traceback
import scipy.stats as sps
import numpy as np


class Common:
    @staticmethod
    def compounded_return(quotes):
        return (quotes.iloc[-1] - quotes.iloc[0]) / quotes.iloc[0]

    @staticmethod
    def compounded_return_all(quotes):
        log_returns = np.log(quotes / quotes.shift(1))
        tr = np.exp(np.cumsum(log_returns)) - 1
        return tr

    @staticmethod
    def compounded_return_year(quotes):
        tr = Common.compounded_return(quotes)
        irr = (1 + tr) ** (252 / len(quotes)) - 1
        return irr


class Volatility:
    @staticmethod
    def historical_volatility(quotes, trading_days):
        # log_returns = np.log(quotes).diff(1)
        log_returns = np.log(quotes / quotes.shift(1))
        # return square root * trading days * log_returns variance
        return np.sqrt(trading_days * log_returns.var())

    @staticmethod
    def avg_historical_volatility(data, period=21, trading_days=252):
        total_cnt = len(data) - period + 1 - 1  # -1 for shift data
        vol_sum = 0
        for i in range(total_cnt):
            vol_sum = vol_sum + Volatility.historical_volatility(data[i:i+period+1], trading_days)  # +1 for shift data

        return vol_sum / total_cnt

    @staticmethod # ref https://blog.raymond-investment.com/stock-simulation-monte-carlo/
    def ewma_historical_volatility(data, period=21, trading_days=252, p_lambda=0.94):
        # a_i = l^(i-1) * (1-l) / (1-l^n)
        if p_lambda >= 1.0:
            p_lambda = 0.9999999999999999  # prevent divided by 0 if p_lambda = 1

        total_cnt = len(data) - period + 1 - 1  # -1 for shift data
        output = 0
        for i in range(total_cnt):
            alpha_i = total_cnt - i - 1  # i: start->latest data, alpha_i: latest->start data factor
            p_alpha_i = (p_lambda**alpha_i) * (1-p_lambda) / (1-p_lambda**total_cnt)
            output = output + p_alpha_i * Volatility.historical_volatility(data[i:i+period+1], trading_days)

        return output


class Stock:
    # mu        Expected Return of asset
    # sigma     Volatility
    # dt        Instantaneous time (ex. 1/252)
    # days      predict days

    #  Monte Carlo
    @staticmethod
    def predict_price_by_mc(s0, mu, sigma, days, dt=1.0/252, iteration=1000000):
        line_list = [s0 * np.ones(iteration)]
        s = s0
        for d in range(days):
            zt = np.random.normal(0, 1, iteration)
            s = s * np.exp((mu - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * zt)
            line_list.append(s)

        output = np.array(line_list).transpose()
        return output


#  reference: https://github.com/QSCTech-Sange/Options-Calculator/blob/master/Backend/Option.py
class Option:
    # european      True: european option, False: american option
    # kind          Put: -1, Call: 1
    # s0            Underlying price
    # k             Strike price of the option
    # t             Time until option exercise (years to maturity)
    # r             Continuously compounding risk-free interest rate
    # sigma         Volatility
    # dv            Dividends of underlying during the option’s life

    # B-S-M (Black-Scholes-Merton)
    @staticmethod
    def d_1(s0, k, t, r, sigma, dv):
        return (np.log(s0 / k) + (r - dv + .5 * sigma ** 2) * t) / sigma / np.sqrt(t)

    @staticmethod
    def d_2(s0, k, t, r, sigma, dv):
        return Option.d_1(s0, k, t, r, sigma, dv) - sigma * np.sqrt(t)

    @staticmethod
    def delta(kind, s0, k, t, r, sigma, dv):
        return kind * sps.norm.cdf(kind * Option.d_1(s0, k, t, r, sigma, dv))

    @staticmethod
    def gamma(s0, k, t, r, sigma, dv):
        return sps.norm.pdf(Option.d_1(s0, k, t, r, sigma, dv)) / (s0 * sigma * np.sqrt(t))

    @staticmethod
    def vega(s0, k, t, r, sigma, dv):
        return 0.01 * (s0 * sps.norm.pdf(Option.d_1(s0, k, t, r, sigma, dv)) * np.sqrt(t))

    @staticmethod
    def theta(kind, s0, k, t, r, sigma, dv):
        return 0.01 * (-(s0 * sps.norm.pdf(Option.d_1(s0, k, t, r, sigma, dv)) * sigma) / (2 * np.sqrt(t)) -
                       kind * r * k * np.exp(-r * t) * sps.norm.cdf(kind * Option.d_2(s0, k, t, r, sigma, dv)))

    @staticmethod
    def rho(kind, s0, k, t, r, sigma, dv):
        return 0.01 * (kind * k * t * np.exp(-r * t) * sps.norm.cdf(kind * Option.d_2(s0, k, t, r, sigma, dv)))

    @staticmethod
    def bs(european, kind, s0, k, t, r, sigma, dv):
        try:
            if european or kind == 1:
                d_1 = Option.d_1(s0, k, t, r, sigma, dv)
                d_2 = Option.d_2(s0, k, t, r, sigma, dv)
                return kind * s0 * np.exp(-dv * t) * sps.norm.cdf(
                    kind * d_1) - kind * k * np.exp(-r * t) * sps.norm.cdf(kind * d_2)
            else:
                return -1

        except Exception:
            logging.error(traceback.format_exc())
            return -1

    #  Monte Carlo
    @staticmethod
    def mc(european, kind, s0, k, t, r, sigma, dv, iteration=1000000):
        try:
            if european or kind == 1:
                zt = np.random.normal(0, 1, iteration)
                st = s0 * np.exp((r - dv - .5 * sigma ** 2) * t + sigma * t ** .5 * zt)
                st = np.maximum(kind * (st - k), 0)
                return np.average(st) * np.exp(-r * t)
            else:
                return -1

        except Exception:
            logging.error(traceback.format_exc())
            return -1

    #  Binomial Tree
    @staticmethod
    def bt(european, kind, s0, k, t, r, sigma, dv, iteration=1000):
        try:
            delta = t / iteration
            u = np.exp(sigma * np.sqrt(delta))
            d = 1 / u
            p = (np.exp((r - dv) * delta) - d) / (u - d)

            tree = np.arange(0, iteration * 2 + 2, 2, dtype=np.longdouble)
            tree[iteration // 2 + 1:] = tree[:(iteration + 1) // 2][::-1]
            np.multiply(tree, -1, out=tree)
            np.add(tree, iteration, out=tree)
            np.power(u, tree[:iteration // 2], out=tree[:iteration // 2])
            np.power(d, tree[iteration // 2:], out=tree[iteration // 2:])
            np.maximum((s0 * tree - k) * kind, 0, out=tree)

            for j in range(iteration):
                newtree = tree[:-1] * p + tree[1:] * (1 - p)
                newtree = newtree * np.exp(-r * delta)
                if not european:
                    compare = np.abs(iteration - j - 1 - np.arange(tree.size - 1) * 2).astype(np.longdouble)
                    np.power(u, compare[:len(compare) // 2], out=compare[:len(compare) // 2])
                    np.power(d, compare[len(compare) // 2:], out=compare[len(compare) // 2:])
                    np.multiply(s0, compare, out=compare)
                    np.subtract(compare, k, out=compare)
                    np.multiply(compare, kind, out=compare)
                    np.maximum(newtree, compare, out=newtree)
                tree = newtree

            if np.isnan(tree[0]):
                return -1
            else:
                return tree[0]

        except Exception:
            logging.error(traceback.format_exc())
            return -1
