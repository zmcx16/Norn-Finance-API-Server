import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pytest import approx

from models import formula


sns.set_style('dark')


def test_historical_volatility():
    quotes = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564]))
    output = formula.Volatility.historical_volatility(quotes, 252)
    assert output == approx(0.46795, rel=1e-5)


def test_avg_historical_volatility():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    output = formula.Volatility.avg_historical_volatility(data, 21, 252)
    assert output == approx(0.468298, rel=1e-5)


def test_ewma_historical_volatility_lambda_approx1():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    # lambda ~= 1 mean avg_historical_volatility
    output = formula.Volatility.ewma_historical_volatility(data, 21, 252, 1.0)
    assert output == approx(formula.Volatility.avg_historical_volatility(data, 21, 252), rel=1e-3)


def test_ewma_historical_volatility_lambda_0():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    # lambda = 0 mean latest historical_volatility
    output = formula.Volatility.ewma_historical_volatility(data, 21, 252, 0)
    assert output == formula.Volatility.historical_volatility(data[len(data)-21-1:len(data)], 252)


def test_compounded_return():
    quotes = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564]))
    output = formula.Common.compounded_return(quotes)
    assert output == approx(-0.012389, rel=1e-4)


def test_compounded_return_all():
    quotes = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564]))
    output = formula.Common.compounded_return_all(quotes)
    assert len(output) == len(quotes)
    assert output.iloc[-1] == approx(-0.012389, rel=1e-4)


def test_price_simulation_by_mc():
    output = formula.Stock.price_simulation_by_mc(100, 0.15, 0.15, 252, dt=1.0/252, iteration=10)
    assert len(output) != 0
    """
    # plot
    plt.xlabel("days")
    plt.ylabel("Close Price")
    plt.grid(linestyle='dotted')
    for i in range(len(output)):
        plt.plot(output[i], linewidth=0.5)
    plt.show()
    """


def test_benford_digit_probs():
    error = 0.01
    digit_probs = formula.Common.benford_digit_probs()
    assert len(digit_probs) == 9
    assert abs(digit_probs[0] - 0.301) < error
    assert abs(digit_probs[1] - 0.176) < error
    assert abs(digit_probs[2] - 0.125) < error
    assert abs(digit_probs[3] - 0.097) < error
    assert abs(digit_probs[4] - 0.079) < error
    assert abs(digit_probs[5] - 0.067) < error
    assert abs(digit_probs[6] - 0.058) < error
    assert abs(digit_probs[7] - 0.051) < error
    assert abs(digit_probs[8] - 0.046) < error

    """
    digits = np.arange(1, 10)
    plt.rc('font', size=16)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(digits, digit_probs)
    plt.xticks(digits)
    plt.xlabel('Digits')
    plt.ylabel('Probability')
    plt.title("Benford's Law: Probability of Leading Digits")
    plt.show()
    """


def test_leading_digit_count():
    fib_nums = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765]
    leading_digit_prob = formula.Common.leading_digit_count(fib_nums)
    assert len(leading_digit_prob) == 2
    assert len(leading_digit_prob['prob']) == len(leading_digit_prob['count']) == 9
    assert len(leading_digit_prob['prob']) == 9
    assert leading_digit_prob['prob'][0] == 0.25
    assert leading_digit_prob['prob'][1] == 0.2
    assert leading_digit_prob['prob'][2] == 0.15
    assert leading_digit_prob['prob'][3] == 0.05
    assert leading_digit_prob['prob'][4] == 0.1
    assert leading_digit_prob['prob'][5] == 0.1
    assert leading_digit_prob['prob'][6] == 0.0
    assert leading_digit_prob['prob'][7] == 0.1
    assert leading_digit_prob['prob'][8] == 0.05


def test_calc_benfords_law():
    """
    def fibonacci(n):
        fibs = [1, 1]
        for i in range(2, n + 1):
            fibs.append(fibs[i - 1] + fibs[i - 2])
        return fibs

    digit_probs = formula.Common.benford_digit_probs()
    digits = np.arange(1, 10)
    fig, axs = plt.subplots(1, 4, figsize=(20, 5))
    for i, ax in enumerate(axs):
        n = 10 ** (i + 1)
        fib_nums = fibonacci(n)
        leading_digit_prob = formula.Common.leading_digit_count(fib_nums)
        sse0 = np.sum((leading_digit_prob['prob'] - digit_probs) ** 2)

        ax.bar(digits, leading_digit_prob['prob'], width=0.25)
        ax.bar(digits + 0.25, digit_probs, width=0.25)  # 0.25 is the width of the bar & 0.25 is the distance between the bars

        ax.set_xticks(digits)
        ax.set_xlabel('Digits')
        ax.set_ylabel('Probability')
        ax.set_title(f'n = {n}, SSE = {sse0:.2e}')

        ax.legend(labels=['Fibonacci', "Benford's Law"])

    plt.suptitle(f'Probability of Leading Digits', fontsize=16)
    plt.show()
    """
