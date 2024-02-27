import traceback
import logging
import time
import json
import csv
from enum import Enum
from datetime import date, datetime, timedelta

import numpy as np
import yfinance as yf
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree

from models import formula
from utils import web


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


def get_stock(symbol):
    return yf.Ticker(symbol)


def get_stock_data_from_marketwatch(symbol, days):
    now = datetime.now()
    period_days = now - timedelta(days=days)
    end_date = now.strftime("%m/%d/%Y") + "%20" + now.strftime("%H:%M:%S")
    start_date = period_days.strftime("%m/%d/%Y") + "%20" + period_days.strftime("%H:%M:%S")
    query_url = "https://www.marketwatch.com/investing/stock/" + symbol + "/downloaddatapartial?startdate=" + start_date + "&enddate=" + end_date + "&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false"

    try:
        ret, content = web.send_request(query_url)
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
            logging.error('send_request failed: {ret}'.format(ret=ret))

    except Exception:
        logging.error(traceback.format_exc())

    return None


def get_stock_history(symbol, period, proxy=None, stock_src="yahoo"):
    try:
        extra_info = {"earningsDate": ""}
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
                return stock_data_df, extra_info
        else:
            ticker = yf.Ticker(symbol)
            extra_info["earningsDate"] = ""
            try:
                if ticker.calendar is not None:
                    extra_info["earningsDate"] = ' - '.join(ticker.calendar.iloc[0].astype(str).array)
            except Exception:
                logging.warning("get ticker.calendar failed")
                logging.error(traceback.format_exc())

            return ticker.history(period=period, proxy=proxy), extra_info
    except Exception:
        logging.error(traceback.format_exc())

    return None, None


def price_simulation_mean_by_mc(symbol, days, ewma_his_vol_lambda, ewma_his_vol_period, iteration, proxy=None, stock_src="yahoo"):
    stock_data, extra_info = get_stock_history(symbol, "1y", proxy, stock_src)
    ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"], period=ewma_his_vol_period,
                                                                 p_lambda=ewma_his_vol_lambda)

    mu = formula.Common.compounded_return(stock_data["Close"])
    output = formula.Stock.price_simulation_by_mc(stock_data["Close"][-1], mu, ewma_his_vol, days, iteration=iteration)
    final_price = output[:, -1]
    return final_price.mean()


def price_simulation_all_by_mc(symbol, days, ewma_his_vol_lambda, ewma_his_vol_period, iteration,
                               mu_vol_type=PriceSimulationType.AUTO_GEN_MU_VOL, mu=0, ewma_his_vol=0, proxy=None,
                               stock_src="yahoo"):
    stock_data, extra_info = get_stock_history(symbol, "1y", proxy, stock_src)

    if mu_vol_type is PriceSimulationType.AUTO_GEN_VOL or mu_vol_type is PriceSimulationType.AUTO_GEN_MU_VOL:
        ewma_his_vol = formula.Volatility.ewma_historical_volatility(data=stock_data["Close"],
                                                                     period=ewma_his_vol_period,
                                                                     p_lambda=ewma_his_vol_lambda)

    if mu_vol_type is PriceSimulationType.AUTO_GEN_MU or mu_vol_type is PriceSimulationType.AUTO_GEN_MU_VOL:
        mu = formula.Common.compounded_return(stock_data["Close"])

    output = formula.Stock.price_simulation_by_mc(stock_data["Close"][-1], mu, ewma_his_vol, days, iteration=iteration)
    return output


def get_ex_dividend_list():
    logging.info('get_ex_dividend_list start')
    url = 'https://www.dividend.com/api/t2/body.html/'
    headers = {
        'sec-ch-ua': 'Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105',
        'Accept': 'application/json, text/plain, */*',
        'DNT': '1',
        'Content-Type': 'application/json;charset=UTF-8',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'Windows',
        'Origin': 'https://www.dividend.com',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.dividend.com/ex-dividend-dates/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7'
    }

    output = {'data': []}
    max_page = 100

    for p in range(1, max_page):
        payload = '{"uuid":"Merged-SEOTable","default_filters":[{"filterKey":"ShareClass","value":["Commons"],"filterType":"FilterShareClass","filterCollection":["CollectionMergedStocks"],"esType":"keyword"}],"tab":"TblTabDivMergedExDiv",' \
                  '"page":' + str(p) +\
            ',"collection":"CollectionMergedStocks","sort_by":{"PayoutNextExDate":"asc"},"theme":"FIN::L1(Dividend Income)","modal_key":null,"modal_keyword":null,"special_theme":"EX_DATE_YEAR_FROM_NOW","ad_unit_full_path":"/2143012/Div/Theme/ExDate","no_content_tray_ads_in_table":false}'
        ret, content = web.send_post(url, headers, payload)
        if ret == 0:
            # logging.info(content)
            # mp-table-body
            soup = BeautifulSoup(content, "html.parser")
            dom = etree.HTML(str(soup))
            rows = dom.xpath('//div[@class="mp-table-body-row"]')
            if len(rows) == 0:
                break

            for row in rows:
                # r = etree.tostring(row, pretty_print=True)
                cells = [elem for elem in row.iter() if elem is not row]
                div_i = 0
                # NAME | YIELD | DIV | FREQ | DEC-DATE | EX-DATE | PAY-DATE | AMOUNT | LAST_AMOUNT
                row_output = {"symbol": "", "link": "", "ex_dividend_date": ""}
                for i, _ in enumerate(cells):
                    if cells[i].tag == 'div':
                        div_i += 1

                    if div_i == 2 and row_output['symbol'] == "":
                        row_output['link'] = "https://www.dividend.com" + cells[i+1].attrib['href']
                        row_output['symbol'] = cells[i+4].text
                    if div_i == 6 and row_output['ex_dividend_date'] == "":  # EX-DATE
                        month_day = cells[i+1].text.split('/')
                        year = cells[i+2].text
                        ymd = date(int(year), int(month_day[0]), int(month_day[1]))
                        row_output["ex_dividend_date"] = ymd.strftime('%Y-%m-%d')

                        logging.info('get ' + row_output['symbol'] + ' ex-dividend done')
                        break

                output['data'].append(row_output)
        else:
            logging.error('send_post failed or done: {ret}'.format(ret=ret))
            break

        time.sleep(1)

    logging.info('get_ex_dividend_list end')
    return output


def get_all_dividend_list():
    logging.info('get_all_dividend_list start')
    url = 'https://www.dividend.com/api/t2/body.html/'
    headers = {
        'sec-ch-ua': 'Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105',
        'Accept': 'application/json, text/plain, */*',
        'DNT': '1',
        'Content-Type': 'application/json;charset=UTF-8',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'Windows',
        'Origin': 'https://www.dividend.com',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.dividend.com/dividend-stock-screener/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7'
    }

    output = {'data': []}
    max_page = 100

    for p in range(1, max_page):
        payload = '{"uuid":"Merged-SEOTable","default_filters":[{"filterKey":"ShareClass","value":["Commons"],"filterType":"FilterShareClass","filterCollection":["CollectionMergedStocks"],"esType":"keyword"}],"tab":"TblTabDivMergedOverviewSEO",' \
                  '"page":' + str(p) +\
                  ',"collection":"CollectionMergedStocks","sort_by":{"MarketCap":"desc"},"theme":"FIN::L1(Dividend Income)_&_STRUC::L1(Stock)","modal_key":null,"modal_keyword":null,"special_theme":"","ad_unit_full_path":"/2143012/Div/Theme/Screener","no_content_tray_ads_in_table":false}'
        ret, content = web.send_post(url, headers, payload)
        if ret == 0:
            # logging.info(content)
            # mp-table-body
            soup = BeautifulSoup(content, "html.parser")
            dom = etree.HTML(str(soup))
            rows = dom.xpath('//div[@class="mp-table-body-row"]')
            if len(rows) == 0:
                break

            for row in rows:
                # r = etree.tostring(row, pretty_print=True)
                cells = [elem for elem in row.iter() if elem is not row]
                div_i = 0
                row_output = {"symbol": "", "link": "", "ex_dividend_date": ""}
                for i, _ in enumerate(cells):
                    if cells[i].tag == 'div':
                        div_i += 1
                    if div_i == 2 and row_output['symbol'] == "":
                        row_output['link'] = "https://www.dividend.com" + cells[i+1].attrib['href']
                        row_output['symbol'] = cells[i+4].text
                    if div_i == 6 and row_output['ex_dividend_date'] == "":  # EX-DATE
                        month_day = cells[i+1].text.split('/')
                        year = cells[i+2].text
                        if year.isdigit() is False or month_day[0].isdigit() is False or month_day[1].isdigit() is False:
                            row_output["ex_dividend_date"] = "-"
                        else:
                            ymd = date(int(year), int(month_day[0]), int(month_day[1]))
                            row_output["ex_dividend_date"] = ymd.strftime('%Y-%m-%d')
                        logging.info('get ' + row_output['symbol'] + ' ex-dividend done')
                        break

                output['data'].append(row_output)
        else:
            logging.error('send_post failed or done: {ret}'.format(ret=ret))
            break

        time.sleep(1)

    logging.info('get_all_dividend_list end')
    return output


def get_dividend_history_by_dividend_com(referer):
    logging.info('get_dividend_history start')
    url = ' https://www.dividend.com/api/data_set/'
    headers = {
        'sec-ch-ua': 'Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105',
        'Accept': 'application/json',
        'DNT': '1',
        'Content-Type': 'application/json',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'Windows',
        'Origin': 'https://www.dividend.com',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': referer,
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7'
    }

    if referer[-1] == '/':
        url_slug = referer.split('/')[-2]
    else:
        url_slug = referer.split('/')[-1]

    output = {'data': []}
    payload = '{"tm":"3-ticker-payout-history-full-screen","r":"ES::DividendStock::Stock#HPQ--NYSE",' \
              '"slug":"' + url_slug + \
              '","default_tab":"overview","only":["meta","data","thead"]}'
    ret, content = web.send_post(url, headers, payload)
    if ret == 0:
        # logging.info(content)
        resp = json.loads(content)
        if 'data' not in resp:
            logging.error('no data in resp: {resp}'.format(resp=resp))
        else:
            for dividend in resp['data']:
                """
"year": "2025e",
"calendar_year_payout": "-",
"calendar_year_payout_growth": "-",
"payable_date": "<div class='flex-wrap'><div class='t-ml-1 n-table-status-dot estimated'></div>2025-07-01</div>",
"declared_date": "2025-05-26",
"ex_date": "2025-06-16",
"adjusted_amount": "$0.2500",
"payment_types": "Income, Qualified",
"type": "Regular",
"payment_frequency": "Quarterly",
"days_to_recovery": "-",
"close_on_ex_date": "0.35%"
                """
                row_output = {"adjusted_amount": float(dividend["adjusted_amount"].replace('$', '')),
                              "declared_date": dividend["declared_date"], "ex_date": dividend["ex_date"], 
                              "payment_types": dividend["payment_types"], "type": dividend["type"], 
                              "payment_frequency": dividend["payment_frequency"]}
                output['data'].append(row_output)
    else:
        logging.info('send_post failed or done: {ret}'.format(ret=ret))

    logging.info('get_dividend_history end')
    return output


def get_dividend_history_by_yahoo(symbol):
    logging.info('get_dividend_history_by_yahoo start')
    data_dict = {}
    output_dict = {}
    data = get_stock(symbol)
    dividends = data.dividends.to_dict()
    for key, value in dividends.items():
        data_dict[key.strftime('%Y-%m-%d')] = {}

    history = data.history(period="max", interval="1d").to_dict()
    for ohlcv_key, ohlcv_val in history.items():
        for key, value in ohlcv_val.items():
            d = key.strftime('%Y-%m-%d')
            if ohlcv_key != 'Dividends' or d in data_dict:
                if d not in output_dict:
                    output_dict[d] = {}
                if ohlcv_key == 'Dividends' or ohlcv_key == 'Volume' or ohlcv_key == 'Close':
                    output_dict[d][ohlcv_key] = value

    output = {"data": []}
    for key, value in output_dict.items():
        row_output = {"date": key}
        row_output.update(value)
        output["data"].append(row_output)

    logging.info('get_dividend_history_by_yahoo end')
    return output


def calc_reports_benford_probs(reports, skip_keys, only_calc_latest_report=False):
    numbers = []
    for r in reports:
        i = 0
        for date in r.columns:
            key_i = 0
            values = r[date].values
            for key in r[date].keys():
                key_i += 1
                if key in skip_keys:
                    continue
                if np.isnan(values[key_i-1]):
                    continue

                numbers.append(values[key_i-1])
            if only_calc_latest_report:
                break
            i += 1

    if len(numbers) == 0:
        return {}

    leading_digit_prob = formula.Common.leading_digit_count(numbers)
    leading_digit_prob["benfordSSE"] = np.sum((leading_digit_prob['prob'] - formula.Common.benford_digit_probs()) ** 2)
    leading_digit_prob["prob"] = leading_digit_prob["prob"].tolist()
    leading_digit_prob["count"] = leading_digit_prob["count"].tolist()
    return leading_digit_prob


def calc_stock_benford_probs(stock):
    output = {
        "benfordDigitProbs": formula.Common.benford_digit_probs().tolist(),
        "stockDigitProbsSSE": {
            "lastQuarter": {},
            "lastYear": {},
            "allQuarters": {},
            "allYears": {},
            "allQuartersYears": {}
        }
    }

    ticker = get_stock(stock)
    income_stmt = ticker.income_stmt
    quarter_income_stmt = ticker.quarterly_income_stmt
    balance_sheet = ticker.balance_sheet
    quarter_balance_sheet = ticker.quarterly_balance_sheet
    cashflow = ticker.cashflow
    quarter_cashflow = ticker.quarterly_cashflow
    if income_stmt.empty and quarter_income_stmt.empty and balance_sheet.empty and quarter_balance_sheet.empty and cashflow.empty and quarter_cashflow.empty:
        return None

    skip_keys = ["Diluted EPS", "Basic EPS", "Tax Rate For Calcs"]
    output["stockDigitProbsSSE"]["lastQuarter"] = calc_reports_benford_probs(
        [quarter_income_stmt, quarter_balance_sheet, quarter_cashflow],
        skip_keys, True)
    output["stockDigitProbsSSE"]["lastYear"] = calc_reports_benford_probs(
        [income_stmt, balance_sheet, cashflow],
        skip_keys, True)
    output["stockDigitProbsSSE"]["allQuarters"] = calc_reports_benford_probs(
        [quarter_income_stmt, quarter_balance_sheet, quarter_cashflow],
        skip_keys, False)
    output["stockDigitProbsSSE"]["allYears"] = calc_reports_benford_probs(
        [income_stmt, balance_sheet, cashflow],
        skip_keys, False)
    output["stockDigitProbsSSE"]["allQuartersYears"] = calc_reports_benford_probs(
        [quarter_income_stmt, quarter_balance_sheet, quarter_cashflow, income_stmt, balance_sheet, cashflow],
        skip_keys, False)
    return output
