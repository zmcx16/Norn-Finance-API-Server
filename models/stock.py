import traceback
import logging
import requests
import csv
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd


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
            # print(content)
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
            print('send_request failed: {ret}'.format(ret=ret))

    except Exception:
        logging.error(traceback.format_exc())

    return None


def get_stock_history(symbol: str, period: str, proxy=None, stock_src="yahoo"):
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
