import time
import pandas as pd
import traceback
import yfinance as yf
import logging
from datetime import date, datetime, timedelta


def get_option_date(symbol: str):
    ticker = yf.Ticker(symbol)
    return ticker.options


def get_option_chain(symbol: str, min_next_days: int, max_next_days: int, min_volume: int, last_trade_days: int,
                     proxy=None):
    # option_chain dataframe column:
    # calls, contractSymbol, lastTradeDate, strike, lastPrice, bid, ask,
    # change, percentChange, volume, openInterest, impliedVolatility,
    # inTheMoney, contractSize, currency
    contracts = []

    now = datetime.now()
    expiry_min_datetime = (now + timedelta(days=min_next_days)).date()
    expiry_max_datetime = (now + timedelta(days=max_next_days)).date()

    try:
        ticker = yf.Ticker(symbol)
        date_list = get_option_date(symbol)
        for expiry_date in date_list:
            expiry_datetime = date.fromisoformat(expiry_date)
            if expiry_max_datetime >= expiry_datetime >= expiry_min_datetime:
                option_chain = ticker.option_chain(expiry_date, proxy=proxy)

                expiry_calls_puts = {"expiryDate": expiry_date, "calls": None, "puts": None}
                calls_puts = [None] * 2
                for calls_puts_index in range(len(option_chain)):
                    d = option_chain[calls_puts_index]
                    d.drop(d[d.volume < min_volume].index, inplace=True)
                    d.drop(d[pd.to_datetime(d.lastTradeDate) < now - timedelta(days=last_trade_days)].index, inplace=True)
                    d.dropna(subset=["volume", "lastTradeDate"], inplace=True)
                    d["lastTradeDate"] = d["lastTradeDate"].apply(lambda x: x.strftime('%Y-%m-%d'))
                    calls_puts[calls_puts_index] = d.to_dict(orient='records')

                expiry_calls_puts["calls"] = calls_puts[0]
                expiry_calls_puts["puts"] = calls_puts[1]
                contracts.append(expiry_calls_puts)

    except Exception:
        logging.error(traceback.format_exc())
        return None, None

    # print(contracts)
    return contracts
