import numpy as np
import pandas as pd
import traceback
import yfinance as yf
import logging
from datetime import date, datetime, timedelta

from models import formula, stock


def get_option_date(symbol: str):
    ticker = yf.Ticker(symbol)
    return ticker.options


def get_option_chain(symbol: str, min_next_days: int, max_next_days: int, min_volume: int, min_price: float,
                     last_trade_days: int, proxy=None):
    # option_chain dataframe column:
    # calls, contractSymbol, lastTradeDate, strike, lastPrice, bid, ask,
    # change, percentChange, volume, openInterest, impliedVolatility,
    # inTheMoney, contractSize, currency
    contracts = []

    now = datetime.now()
    expiry_min_datetime = (now + timedelta(days=min_next_days)).date()
    expiry_max_datetime = (now + timedelta(days=max_next_days)).date()

    last_trade_days_wo_weekend = last_trade_days
    weekday = datetime.today().isoweekday()
    if weekday == 7:
        last_trade_days_wo_weekend = last_trade_days_wo_weekend + 1
    elif weekday == 1:
        last_trade_days_wo_weekend = last_trade_days_wo_weekend + 2
    elif weekday == 2:
        last_trade_days_wo_weekend = last_trade_days_wo_weekend + 3

    try:
        ticker = yf.Ticker(symbol)
        date_list = get_option_date(symbol)
        for expiry_date in date_list:
            expiry_datetime = date.fromisoformat(expiry_date)
            if expiry_max_datetime >= expiry_datetime >= expiry_min_datetime:
                option_chain = ticker.option_chain(expiry_date, proxy=proxy)
                if len(option_chain) == 0:
                    logging.warning("{symbol}-{expiry_date} option_chain length = 0".format(symbol=symbol,
                                                                                            expiry_date=expiry_date))

                expiry_calls_puts = {"expiryDate": expiry_date, "calls": None, "puts": None}
                calls_puts = [None] * 2
                for calls_puts_index in range(len(option_chain)):
                    d = option_chain[calls_puts_index]
                    d.drop(d[d.volume < min_volume].index, inplace=True)
                    d.drop(d[d.lastPrice < min_price].index, inplace=True)
                    d.drop(d[pd.to_datetime(d.lastTradeDate).dt.date <
                             (now - timedelta(days=last_trade_days_wo_weekend)).date()].index, inplace=True)
                    d.dropna(subset=["lastTradeDate", "strike", "lastPrice", "bid", "ask", "change", "percentChange",
                                     "volume", "openInterest", "impliedVolatility"], inplace=True)
                    d["lastTradeDate"] = d["lastTradeDate"].apply(lambda x: x.strftime('%Y-%m-%d'))
                    calls_puts[calls_puts_index] = d.to_dict(orient='records')

                if len(calls_puts[0]) > 0 or len(calls_puts[1]) > 0:
                    expiry_calls_puts["calls"] = calls_puts[0]
                    expiry_calls_puts["puts"] = calls_puts[1]
                    contracts.append(expiry_calls_puts)

    except Exception:
        logging.error(traceback.format_exc())
        return None

    # print(contracts)
    return contracts


def calc_option_valuation(contracts, stock_price, volatility, risk_free_interest_rate=0.0152, dividends=0):
    now = datetime.now().date()
    for contract in contracts:
        expiry_date = contract['expiryDate']
        expiry_datetime = date.fromisoformat(expiry_date)
        time_2_maturity_year = (np.busday_count(now, expiry_datetime)+1) / 252.0

        def calc(call_put, kind):  # kind: call: 1, put: -1
            call_put["valuationData"] = {"BSM_EWMAHisVol": -1, "MC_EWMAHisVol": -1, "BT_EWMAHisVol": -1}
            call_put["valuationData"]["BSM_EWMAHisVol"] = formula.Option.bs(False, kind, stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call_put["valuationData"]["MC_EWMAHisVol"] = formula.Option.mc(False, kind, stock_price, call_put['strike'],
                                                               time_2_maturity_year, risk_free_interest_rate,
                                                               volatility, dividends)
            call_put["valuationData"]["BT_EWMAHisVol"] = formula.Option.bt(False, kind, stock_price, call_put['strike'],
                                                               time_2_maturity_year, risk_free_interest_rate,
                                                               volatility, dividends)

            call_put["valuationData"]["delta"] = formula.Option.delta(kind, stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call_put["valuationData"]["gamma"] = formula.Option.gamma(stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call_put["valuationData"]["vega"] = formula.Option.vega(stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call_put["valuationData"]["theta"] = formula.Option.theta(kind, stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)
            call_put["valuationData"]["rho"] = formula.Option.rho(kind, stock_price, call_put['strike'],
                                                                time_2_maturity_year, risk_free_interest_rate,
                                                                volatility, dividends)

        for call in contract["calls"]:
            if time_2_maturity_year <= 0:
                continue
            calc(call, 1)

        for put in contract["puts"]:
            if time_2_maturity_year <= 0:
                continue
            calc(put, -1)

    #  print(contracts)


def calc_kelly_criterion(stock_close_data, ewma_his_vol, contracts, force_zero_mu=False):
    now = datetime.now().date()
    key = "KellyCriterion"
    if force_zero_mu:
        key = "KellyCriterion_MU_0"

    mu = 0
    if not force_zero_mu:
        mu = formula.Common.compounded_return(stock_close_data)

    expiry_days_dict = {}
    for contract in contracts:
        expiry_date = contract['expiryDate']
        expiry_datetime = date.fromisoformat(expiry_date)
        expiry_days_dict[expiry_date] = np.busday_count(now, expiry_datetime) + 1

    max_days = max(expiry_days_dict.values())
    output = formula.Stock.predict_price_by_mc(stock_close_data[len(stock_close_data)-1], mu, ewma_his_vol, max_days+1,
                                               iteration=60000)
    for contract in contracts:
        expiry_date = contract['expiryDate']
        days = expiry_days_dict[expiry_date]
        expiry_predict_prices = output[:, days]

        def kelly(call_put, kind):  # kind: call: 1, put: -1
            strike = call_put['strike']
            last_price = call_put['lastPrice']

            """
            for p_price in expiry_predict_prices:
                if kind * p_price > kind * (strike + (kind * last_price)):
                    gain_list.append(kind * (p_price - (strike + kind * last_price)))
                elif kind * p_price > kind * strike:
                    loss_list.append(kind * ((strike + kind * last_price) - p_price))
                else:
                    loss_list.append(last_price)
            """
            var1_list = np.where(kind * expiry_predict_prices > kind * (strike + (kind * last_price)),
                                 kind * (expiry_predict_prices - (strike + kind * last_price)), 0)
            var2_list = np.where((kind * expiry_predict_prices > kind * strike) &
                                 (kind * expiry_predict_prices <= kind * (strike + (kind * last_price))),
                                 kind * ((strike + kind * last_price) - expiry_predict_prices), 0)
            fixed_list = np.where(kind * expiry_predict_prices <= kind * strike, last_price, 0)

            def calc(buy_sell, gain_list, loss_list):
                gain_all = sum(gain_list)
                loss_all = sum(loss_list)
                p = np.count_nonzero(gain_list) * 1.0 / len(expiry_predict_prices)
                q = np.count_nonzero(loss_list) * 1.0 / len(expiry_predict_prices)
                if loss_all == 0:
                    call_put["valuationData"][key + "_" + buy_sell] = p
                else:
                    b = gain_all / loss_all
                    if b == 0:
                        call_put["valuationData"][key + "_" + buy_sell] = -2147483648
                    else:
                        call_put["valuationData"][key + "_" + buy_sell] = p - (q / b)

            calc("buy", var1_list, var2_list + fixed_list)
            calc("sell", var2_list + fixed_list, var1_list)

        for call in contract["calls"]:
            kelly(call, 1)

        for put in contract["puts"]:
            kelly(put, -1)


def options_chain_quotes_valuation(symbol, min_next_days, max_next_days, min_volume, min_price, last_trade_days,
                                   ewma_his_vol_period, ewma_his_vol_lambda, proxy, stock_src="yahoo"):
    contracts = get_option_chain(symbol, min_next_days, max_next_days, min_volume, min_price, last_trade_days, proxy)
    if len(contracts) == 0:
        return None, None, None

    stock_data = stock.get_stock_history(symbol, "1y", proxy, stock_src)
    ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"], period=ewma_his_vol_period,
                                                                 p_lambda=ewma_his_vol_lambda)
    stock_price = stock_data["Close"][len(stock_data["Close"])-1]
    calc_option_valuation(contracts, stock_price, ewma_his_vol)

    # calc kelly criterion
    calc_kelly_criterion(stock_data["Close"], ewma_his_vol, contracts, True)
    calc_kelly_criterion(stock_data["Close"], ewma_his_vol, contracts, False)

    return stock_price, ewma_his_vol, contracts
