import pandas as pd
import yfinance as yf
import logging
from datetime import date, datetime, timedelta


def get_option_date(symbol: str):
    ticker = yf.Ticker(symbol)
    return ticker.options


def get_option_chain(symbol: str, min_next_days, max_next_days):
    # option_chain dataframe column:
    # calls, contractSymbol, lastTradeDate, strike, lastPrice, bid, ask,
    # change, percentChange, volume, openInterest, impliedVolatility
    opt_list = pd.DataFrame()
    now = datetime.now()
    expiry_min_datetime = (now + timedelta(days=min_next_days)).date()
    expiry_max_datetime = (now + timedelta(days=max_next_days)).date()

    try:
        ticker = yf.Ticker(symbol)
        date_list = get_option_date(symbol)
        for expiry_date in date_list:
            expiry_datetime = date.fromisoformat(expiry_date)
            if expiry_max_datetime >= expiry_datetime >= expiry_min_datetime:
                option_chain = ticker.option_chain(expiry_date)
                opt_list = opt_list.append(option_chain)

    except Exception as ex:
        logging.error('Generated an exception: {ex}'.format(ex=ex))

    return opt_list
