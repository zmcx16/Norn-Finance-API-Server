import traceback
import logging
import requests
import csv
from enum import Enum
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

from models import formula


class PriceSimulationType(Enum):
    AUTO_GEN_MU = 1
    AUTO_GEN_VOL = 2
    AUTO_GEN_MU_VOL = 3
    MANUAL_ALL = 4


def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def send_request(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
    except Exception as ex:
        logging.error(traceback.format_exc())
        return -1, ex

    return 0, res.text


def get_stock_data_from_marketwatch(symbol, days):
    now = datetime.now()
    period_days = now - timedelta(days=days)
    end_date = now.strftime("%m/%d/%Y") + "%20" + now.strftime("%H:%M:%S")
    start_date = period_days.strftime("%m/%d/%Y") + "%20" + period_days.strftime("%H:%M:%S")
    query_url = "https://www.marketwatch.com/investing/stock/" + symbol + "/downloaddatapartial?startdate=" + start_date + "&enddate=" + end_date + "&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false"

    try:
        ret, content = send_request(query_url)
        if ret == 0:
            # logging.info(content)
            lines = content.splitlines()
            read_csv = csv.reader(lines)
            headers = next(read_csv)
            output = []
            for row in read_csv:
                d = {}
                for i in range(len(headers)):
                    v = row[i].replace(',', '')  # 2,134 -> 2134
                    if is_float(v):
                        v = float(v)
                    d[headers[i]] = v
                output.append(d)

            return output
        else:
            logging.info('send_request failed: {ret}'.format(ret=ret))

    except Exception:
        logging.error(traceback.format_exc())

    return None


def get_stock_history(symbol, period, proxy=None, stock_src="yahoo"):
    try:
        if stock_src == "marketwatch":
            if period == "1mo":
                days = 30
            elif period == "3mo":
                days = 91
            elif period == "6mo":
                days = 187
            elif period == "1y":
                days = 365
            else:
                raise ValueError("period is invalid")
            stock_data = get_stock_data_from_marketwatch(symbol, days)
            if stock_data and len(stock_data) > 0:
                # convert to dataframe
                stock_data_df = pd.DataFrame.from_records(stock_data)
                stock_data_df = stock_data_df[::-1]  # reverse order
                stock_data_df = stock_data_df.set_index(pd.DatetimeIndex(stock_data_df['Date']))
                stock_data_df.drop(columns=['Date'], inplace=True)
                return stock_data_df
        else:
            ticker = yf.Ticker(symbol)
            return ticker.history(period=period, proxy=proxy)

    except Exception:
        logging.error(traceback.format_exc())

    return None


def price_simulation_mean_by_mc(symbol, days, ewma_his_vol_lambda, ewma_his_vol_period, iteration, proxy=None, stock_src="yahoo"):
    stock_data = get_stock_history(symbol, "1y", proxy, stock_src)
    ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"], period=ewma_his_vol_period,
                                                                 p_lambda=ewma_his_vol_lambda)

    mu = formula.Common.compounded_return(stock_data["Close"])
    output = formula.Stock.price_simulation_by_mc(stock_data["Close"][-1], mu, ewma_his_vol, days, iteration=iteration)
    final_price = output[:, -1]
    return final_price.mean()


def price_simulation_all_by_mc(symbol, days, ewma_his_vol_lambda, ewma_his_vol_period, iteration,
                               mu_vol_type=PriceSimulationType.AUTO_GEN_MU_VOL, mu=0, ewma_his_vol=0, proxy=None,
                               stock_src="yahoo"):
    stock_data = get_stock_history(symbol, "1y", proxy, stock_src)

    if mu_vol_type is PriceSimulationType.AUTO_GEN_VOL or mu_vol_type is PriceSimulationType.AUTO_GEN_MU_VOL:
        ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"],
                                                                     period=ewma_his_vol_period,
                                                                     p_lambda=ewma_his_vol_lambda)

    if mu_vol_type is PriceSimulationType.AUTO_GEN_MU or mu_vol_type is PriceSimulationType.AUTO_GEN_MU_VOL:
        mu = formula.Common.compounded_return(stock_data["Close"])

    output = formula.Stock.price_simulation_by_mc(stock_data["Close"][-1], mu, ewma_his_vol, days, iteration=iteration)
    return output
