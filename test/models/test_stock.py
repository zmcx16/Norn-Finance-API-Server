from models import stock


def test_get_stock_history_yahoo():
    output = stock.get_stock_history("T", "1y", proxy=None, stock_src="yahoo")
    assert output is not None
    print(output)


def test_get_stock_history_marketwatch():
    output = stock.get_stock_history("T", "1y", proxy=None, stock_src="marketwatch")
    assert output is not None
    print(output)


def test_predict_price_by_mc():
    output = stock.predict_price_by_mc("T", 252, 0.92, 21, 100000, stock_src="yahoo")
    assert output is not None
    print(output)
