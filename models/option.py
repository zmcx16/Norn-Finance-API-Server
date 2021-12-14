from typing import Optional
import yfinance as yf


def get_option_date(symbol: str):
    ticker = yf.Ticker(symbol)
    return ticker.options


def get_option_chain(symbol: str):
    ticker = yf.Ticker(symbol)
    date_list = get_option_date(symbol)
    for date in date_list:
        option_chain = ticker.option_chain(date)
        print(option_chain)
        break
