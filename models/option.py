import numpy as np
import pandas as pd
import traceback
import yfinance as yf
import logging
from datetime import date, datetime, timedelta

from models.formula import Option

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
                    d.drop(d[pd.to_datetime(d.lastTradeDate).dt.date < (now - timedelta(days=last_trade_days)).date()].index, inplace=True)
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


def calc_option_valuation(contracts, stock_price, volatility, risk_free_interest_rate=0.0152, dividends=0):
    now = datetime.now().date()
    for contract in contracts:
        expiry_date = contract['expiryDate']
        expiry_datetime = date.fromisoformat(expiry_date)
        time_2_maturity_year = np.busday_count(now, expiry_datetime) / 252.0
        for call in contract["calls"]:
            call["valuationData"] = {"BSM_EWMAHisVol": -1, "MC_EWMAHisVol": -1, "BT_EWMAHisVol": -1}
            call["valuationData"]["BSM_EWMAHisVol"] = Option.bs(False, 1, stock_price, call['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call["valuationData"]["MC_EWMAHisVol"] = Option.mc(False, 1, stock_price, call['strike'],
                                                               time_2_maturity_year, risk_free_interest_rate,
                                                               volatility, dividends)
            call["valuationData"]["BT_EWMAHisVol"] = Option.bt(False, 1, stock_price, call['strike'],
                                                               time_2_maturity_year, risk_free_interest_rate,
                                                               volatility, dividends)
        for put in contract["puts"]:
            put["valuationData"] = {"BSM_EWMAHisVol": -1, "MC_EWMAHisVol": -1, "BT_EWMAHisVol": -1}
            put["valuationData"]["BSM_EWMAHisVol"] = Option.bs(False, -1, stock_price, put['strike'],
                                                               time_2_maturity_year, risk_free_interest_rate,
                                                               volatility, dividends)
            put["valuationData"]["MC_EWMAHisVol"] = Option.mc(False, -1, stock_price, put['strike'],
                                                              time_2_maturity_year, risk_free_interest_rate,
                                                              volatility, dividends)
            put["valuationData"]["BT_EWMAHisVol"] = Option.bt(False, -1, stock_price, put['strike'],
                                                              time_2_maturity_year, risk_free_interest_rate,
                                                              volatility, dividends)
    print(contracts)
