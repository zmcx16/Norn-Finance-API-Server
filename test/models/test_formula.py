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
    assert output == approx(0.468, rel=1e-3)
