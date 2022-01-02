import scipy.stats as sps
import numpy as np


class Volatility:
    @staticmethod
    def historical_volatility(quotes, trading_days):
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

    @staticmethod
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


#  reference: https://github.com/QSCTech-Sange/Options-Calculator/blob/master/Backend/Option.py
class Option:
    # european      True: european option, False: american option
    # kind          Put: -1, Call: 1
    # s0            Underlying price
    # k             Strike price of the option
    # t             Time to maturity (days)
    # r             Continuously compounding risk-free interest rate
    # sigma         Volatility
    # dv            Dividends of underlying during the option’s life
    def __init__(self, european, kind, s0, k, t, r, sigma, dv):
        self.european = european
        self.kind = kind
        self.s0 = s0
        self.k = k
        self.t = t / 252
        self.sigma = sigma
        self.r = r
        self.dv = dv

    # B-S-M (Black-Scholes-Merton)
    def bs(self):
        if self.european or self.kind == 1:
            d_1 = (np.log(self.s0 / self.k) + (
                    self.r - self.dv + .5 * self.sigma ** 2) * self.t) / self.sigma / np.sqrt(
                self.t)
            d_2 = d_1 - self.sigma * np.sqrt(self.t)
            return self.kind * self.s0 * np.exp(-self.dv * self.t) * sps.norm.cdf(
                self.kind * d_1) - self.kind * self.k * np.exp(-self.r * self.t) * sps.norm.cdf(self.kind * d_2)
        else:
            return -1

    #  Monte Carlo
    def mc(self, iteration):
        if self.european or self.kind == 1:
            zt = np.random.normal(0, 1, iteration)
            st = self.s0 * np.exp((self.r - self.dv - .5 * self.sigma ** 2) * self.t + self.sigma * self.t ** .5 * zt)
            st = np.maximum(self.kind * (st - self.k), 0)
            return np.average(st) * np.exp(-self.r * self.t)
        else:
            return -1

    #  Binomial Tree
    def bt(self, iteration):
        delta = self.t / iteration
        u = np.exp(self.sigma * np.sqrt(delta))
        d = 1 / u
        p = (np.exp((self.r - self.dv) * delta) - d) / (u - d)

        tree = np.arange(0, iteration * 2 + 2, 2, dtype=np.float128)
        tree[iteration // 2 + 1:] = tree[:(iteration + 1) // 2][::-1]
        np.multiply(tree, -1, out=tree)
        np.add(tree, iteration, out=tree)
        np.power(u, tree[:iteration // 2], out=tree[:iteration // 2])
        np.power(d, tree[iteration // 2:], out=tree[iteration // 2:])
        np.maximum((self.s0 * tree - self.k) * self.kind, 0, out=tree)

        for j in range(iteration):
            newtree = tree[:-1] * p + tree[1:] * (1 - p)
            newtree = newtree * np.exp(-self.r * delta)
            if not self.european:
                compare = np.abs(iteration - j - 1 - np.arange(tree.size - 1) * 2).astype(np.float128)
                np.power(u, compare[:len(compare) // 2], out=compare[:len(compare) // 2])
                np.power(d, compare[len(compare) // 2:], out=compare[len(compare) // 2:])
                np.multiply(self.s0, compare, out=compare)
                np.subtract(compare, self.k, out=compare)
                np.multiply(compare, self.kind, out=compare)
                np.maximum(newtree, compare, out=newtree)
            tree = newtree
        return tree[0]
