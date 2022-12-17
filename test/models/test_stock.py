import matplotlib.pyplot as plt

from models import stock


def test_get_stock():
    output = stock.get_stock("T")
    assert output is not None
    print("******* actions *******")
    print(output.actions)
    print("***********************")
    print("****** dividends ******")
    print(output.dividends)
    print("***********************")
    print("****** calendar *******")
    print(output.calendar)
    print("***********************")
    # print("*** earnings_dates ****")
    # print(output.earnings_dates)
    # print("***********************")
    print("******** news *********")
    print(output.news)
    print("***********************")


def test_get_ex_dividend_list():
    output = stock.get_ex_dividend_list()
    assert output is not None
    assert len(output['data']) > 0
    print(output)
    print(len(output['data']))


def test_get_stock_history_yahoo():
    output, extra_info = stock.get_stock_history("T", "1y", proxy=None, stock_src="yahoo")
    assert output is not None
    print(output)


def test_get_stock_history_marketwatch():
    output, extra_info = stock.get_stock_history("T", "1y", proxy=None, stock_src="marketwatch")
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
