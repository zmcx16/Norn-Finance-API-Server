import pandas as pd
import numpy as np
from pytest import approx

from models import formula


def test_historical_volatility():
    quotes = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564]))
    output = formula.historical_volatility(quotes, 252)
    assert output == approx(0.46795, rel=1e-5)


def test_avg_historical_volatility():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    output = formula.avg_historical_volatility(data, 21, 252)
    assert output == approx(0.468298, rel=1e-5)


def test_ewma_historical_volatility_lambda_approx1():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    # lambda ~= 1 mean avg_historical_volatility
    output = formula.ewma_historical_volatility(data, 21, 252, 1.0)
    assert output == approx(formula.avg_historical_volatility(data, 21, 252), rel=1e-3)


def test_ewma_historical_volatility_lambda_0():
    data = pd.Series(
        np.array([14.387821, 14.226541, 14.769797, 15.015962, 15.414914, 16.068521, 16.060032, 15.796894, 15.881777,
                  15.406426, 16.077011, 14.973518, 14.948055, 14.812241, 14.829216, 14.608518, 14.387821, 14.574565,
                  14.014332, 14.218052, 13.708749, 14.209564, 13.903981, 14.336889, 14.005842]))

    # lambda = 0 mean latest historical_volatility
    output = formula.ewma_historical_volatility(data, 21, 252, 0)
    assert output == formula.historical_volatility(data[len(data)-21-1:len(data)], 252)
