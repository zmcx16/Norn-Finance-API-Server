import numpy as np
import pandas as pd
import traceback
import yfinance as yf
import logging
from enum import Enum
from datetime import date, datetime, timedelta

from models import formula, stock


class CalcKellyType(Enum):
    KellyCriterion = 1
    KellyCriterion_MU_0 = 2
    KellyCriterion_IV = 3


def get_option_date(symbol: str):
    ticker = yf.Ticker(symbol)
    return ticker.options


def get_option_chain(symbol: str, min_next_days: int, max_next_days: int, min_volume: int, min_price: float,
                     last_trade_days: int, specific_contract=None, proxy=None):
    # option_chain dataframe column:
    # calls, contractSymbol, lastTradeDate, strike, lastPrice, bid, ask,
    # change, percentChange, volume, openInterest, impliedVolatility,
    # inTheMoney, contractSize, currency
    contracts = []

    specific_call_put = -1
    specific_expiry_date = None
    specific_strike = -1
    if specific_contract and specific_contract.count('_') == 2:
        temp = specific_contract.split("_")
        if temp[0] == "call":
            specific_call_put = 0
        elif temp[0] == "put":
            specific_call_put = 1
        specific_expiry_date = temp[1]
        specific_strike = float(temp[2])

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
            if specific_expiry_date and expiry_date != specific_expiry_date:
                continue

            expiry_datetime = date.fromisoformat(expiry_date)
            if expiry_max_datetime >= expiry_datetime >= expiry_min_datetime:
                option_chain = ticker.option_chain(expiry_date, proxy=proxy)
                if len(option_chain) == 0:
                    logging.warning("{symbol}-{expiry_date} option_chain length = 0".format(symbol=symbol,
                                                                                            expiry_date=expiry_date))

                expiry_calls_puts = {"expiryDate": expiry_date, "calls": [], "puts": []}
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

                    if specific_strike != -1:
                        d.drop(d[(specific_strike < d.strike-0.00001) | (specific_strike > d.strike+0.00001)].index,
                               inplace=True)

                    calls_puts[calls_puts_index] = d.to_dict(orient='records')

                if len(calls_puts[0]) > 0 or len(calls_puts[1]) > 0:
                    if specific_call_put == 0:
                        expiry_calls_puts["calls"] = calls_puts[0]
                    elif specific_call_put == 1:
                        expiry_calls_puts["puts"] = calls_puts[1]
                    else:
                        expiry_calls_puts["calls"] = calls_puts[0]
                        expiry_calls_puts["puts"] = calls_puts[1]

                    contracts.append(expiry_calls_puts)

    except Exception:
        logging.error(traceback.format_exc())
        return None

    # logging.info(contracts)
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

    #  logging.info(contracts)


def calc_kelly_criterion(stock_close_data, ewma_his_vol, contracts, calc_kelly_type, iteration):
    now = datetime.now().date()
    key = calc_kelly_type.name

    mu = 0
    if calc_kelly_type is CalcKellyType.KellyCriterion:
        mu = formula.Common.compounded_return(stock_close_data)

    expiry_days_dict = {}
    for contract in contracts:
        expiry_date = contract['expiryDate']
        expiry_datetime = date.fromisoformat(expiry_date)
        expiry_days_dict[expiry_date] = np.busday_count(now, expiry_datetime) + 1

    max_days = max(expiry_days_dict.values())

    if calc_kelly_type is not CalcKellyType.KellyCriterion_IV:
        output = formula.Stock.price_simulation_by_mc(stock_close_data[len(stock_close_data)-1], mu, ewma_his_vol,
                                                      max_days+1, iteration=iteration)
    for contract in contracts:
        expiry_date = contract['expiryDate']
        days = expiry_days_dict[expiry_date]
        expiry_predict_prices_t = None
        if calc_kelly_type is not CalcKellyType.KellyCriterion_IV:
            expiry_predict_prices_t = output[:, days]

        def kelly(call_put, kind, expiry_predict_prices_temp):  # kind: call: 1, put: -1
            expiry_predict_prices = expiry_predict_prices_temp
            if calc_kelly_type is CalcKellyType.KellyCriterion_IV:
                iv = call_put['impliedVolatility']
                output = formula.Stock.price_simulation_by_mc(stock_close_data[len(stock_close_data) - 1], 0,
                                                              iv, days + 1, iteration=50000)
                expiry_predict_prices = output[:, days]

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
            kelly(call, 1, expiry_predict_prices_t)

        for put in contract["puts"]:
            kelly(put, -1, expiry_predict_prices_t)


def filter_out_otm(contracts, stock_price):
    for contract in contracts:
        def filter_contract(call_put, kind):  # kind: call: 1, put: -1
            return call_put["strike"] * kind >= stock_price * kind

        contract["calls"] = [call for call in contract["calls"] if filter_contract(call, 1)]
        contract["puts"] = [put for put in contract["puts"] if filter_contract(put, -1)]


def options_chain_quotes_valuation(symbol, min_next_days, max_next_days, min_volume, min_price, last_trade_days,
                                   ewma_his_vol_period, ewma_his_vol_lambda, only_otm, specific_contract, proxy,
                                   stock_src="yahoo", calc_kelly_iv=False, iteration=100000):
    contracts = get_option_chain(symbol, min_next_days, max_next_days, min_volume, min_price, last_trade_days,
                                 specific_contract, proxy)
    if len(contracts) == 0:
        return None, None, None, None

    stock_data, extra_info = stock.get_stock_history(symbol, "1y", proxy, stock_src)
    ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"], period=ewma_his_vol_period,
                                                                 p_lambda=ewma_his_vol_lambda)
    stock_price = stock_data["Close"][len(stock_data["Close"])-1]

    if only_otm:
        filter_out_otm(contracts, stock_price)

    calc_option_valuation(contracts, stock_price, ewma_his_vol)

    # calc kelly criterion
    calc_kelly_criterion(stock_data["Close"], ewma_his_vol, contracts, CalcKellyType.KellyCriterion, iteration)
    calc_kelly_criterion(stock_data["Close"], ewma_his_vol, contracts, CalcKellyType.KellyCriterion_MU_0, iteration)
    if calc_kelly_iv:
        calc_kelly_criterion(stock_data["Close"], ewma_his_vol, contracts, CalcKellyType.KellyCriterion_IV, iteration)

    return stock_price, extra_info, ewma_his_vol, contracts
