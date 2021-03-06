import matplotlib.pyplot as plt

from models import stock


def test_get_stock_history_yahoo():
    output = stock.get_stock_history("T", "1y", proxy=None, stock_src="yahoo")
    assert output is not None
    print(output)


def test_get_stock_history_marketwatch():
    output = stock.get_stock_history("T", "1y", proxy=None, stock_src="marketwatch")
    assert output is not None
    print(output)


def test_price_simulation_mean_by_mc():
    output = stock.price_simulation_mean_by_mc("T", 252, 0.92, 21, 100000, stock_src="yahoo")
    assert output is not None
    print(output)


def test_price_simulation_all_by_mc():
    symbol = "T"
    output = stock.price_simulation_all_by_mc(symbol, 252, 0.92, 21, 100, stock_src="yahoo")
    assert output is not None
    print(output)
    """
    # plot
    plt.title("Stock: " + symbol)
    plt.xlabel("days")
    plt.ylabel("Close Price")
    plt.grid(linestyle='dotted')
    for i in range(len(output)):
        plt.plot(output[i], linewidth=0.5)
    plt.show()
    """
