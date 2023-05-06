import os
import sys
import pathlib
import argparse
import json
import logging
import requests
import traceback
import time
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime, timedelta

afscreener_url = os.environ.get(
    "AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")
DELAY_TIME_SEC = 1
RETRY_FAILED_DELAY = 20
RETRY_CNT = 5

FINRA_COOKIES = {}


def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def send_request(url, for_cookie=False):
    for r in range(RETRY_CNT):
        try:
            res = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            })
            res.raise_for_status()
        except Exception as ex:
            logging.error('Generated an exception: {ex}'.format(ex=ex))

        if res.status_code == 200:
            if for_cookie:
                return 0, res.cookies.get_dict()
            return 0, res.text

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def send_post_json(url, req_data, headers={'content-type': 'application/json'}, cookies=None):
    for r in range(RETRY_CNT):
        try:
            res = requests.post(url, req_data, headers=headers, cookies=cookies)
            res.raise_for_status()
        except Exception as ex:
            logging.error('Generated an exception: {ex}, {res}'.format(ex=ex, res=res.text))

        if res.status_code == 200:
            return 0, res.json()

        time.sleep(RETRY_FAILED_DELAY)

    return -2, {}


def get_finra_cookie_by_browser(driver, retry):
    logging.info("call get_finra_cookie_by_browser")
    for r in range(retry):
        try:
            driver.get("https://www.finra.org/finra-data/browse-catalog/equity-short-interest/data")
            for i in range(60):
                time.sleep(DELAY_TIME_SEC)
                logging.info(driver.get_cookies())
                for cookie in driver.get_cookies():
                    FINRA_COOKIES[cookie['name']] = cookie['value']
                if "XSRF-TOKEN" in FINRA_COOKIES:
                    logging.info("get xsrf success")
                    return 0
                else:
                    logging.info("wait...{i}".format(i=i))
        except Exception:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)


def get_af_common_data(api, retry):
    for r in range(retry):
        try:
            param = {
                'code': afscreener_token,
                'api': api
            }
            encoded_args = urlencode(param)
            query_url = afscreener_url + '?' + encoded_args

            ret, resp = send_request(query_url, 0)
            if ret == 0:
                try:
                    resp_json = json.loads(resp)
                    if resp_json["ret"] != 0:
                        logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                    else:
                        return resp_json["data"]
                except Exception as ex:
                    logging.error(traceback.format_exc())
            else:
                logging.error('send_request failed: {ret}'.format(ret=ret))

        except Exception as ex:
            logging.error(traceback.format_exc())

        logging.info(f'retry = {r}')
        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)


def get_stock_base_info():
    logging.info("call get_stock_base_info")
    try:
        param = {
            'code': afscreener_token,
            'api': 'get-stock-report'
        }
        encoded_args = urlencode(param)
        query_url = afscreener_url + '?' + encoded_args
        ret, resp = send_post_json(query_url, str(
            {"baseinfo_v": ["Price", "Shs Float", "Short Float", "Short Ratio", "Short Interest"]}))
        if ret == 0:
            if resp["ret"] == 0:
                return resp["data"]
            else:
                logging.error('server err = {err}, msg = {msg}'.format(err=resp["ret"], msg=resp["err_msg"]))
        else:
            logging.error('send_post_json failed: {ret}'.format(ret=ret))

        sys.exit(1)

    except Exception as ex:
        logging.error('Generated an exception: {ex}'.format(ex=ex))


def get_short_data(symbol):
    logging.info("call get_short_data")
    xrf_token = FINRA_COOKIES["XSRF-TOKEN"]
    payload = json.dumps({
            "fields": [
                "settlementDate",
                "issueName",
                "symbolCode",
                "marketClassCode",
                "currentShortPositionQuantity",
                "previousShortPositionQuantity",
                "changePreviousNumber",
                "changePercent",
                "averageDailyVolumeQuantity",
                "daysToCoverQuantity",
                "revisionFlag"
            ],
            "dateRangeFilters": [

            ],
            "domainFilters": [

            ],
            "compareFilters": [
                {
                    "fieldName": "symbolCode",
                    "fieldValue": symbol,
                    "compareType": "EQUAL"
                }
            ],
            "orFilters": [

            ],
            "aggregationFilter": None,
            "sortFields": [
                "-settlementDate",
                "+issueName"
            ],
            "limit": 50,
            "offset": 0,
            "delimiter": None,
            "quoteValues": False
        })

    ret, resp = send_post_json(
        "https://services-dynarep.ddwa.finra.org/public/reporting/v2/data/group/OTCMarket/name/ConsolidatedShortInterest",
        payload,
        {
            'content-type': 'application/json',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
            'X-XSRF-TOKEN': str(xrf_token)
        }, FINRA_COOKIES)

    if ret == 0:
        if resp["status"] == "success" and resp["returnBody"]["statusCode"] == 200:
            return json.loads(resp["returnBody"]["data"])
        else:
            logging.error('server err = {err}, msg = {msg}'.format(err=resp["ret"], msg=resp["err_msg"]))
    else:
        logging.error('send_post_json failed: {ret}'.format(ret=ret))

    return None


def main():

    logging.basicConfig(level=logging.INFO)

    options = webdriver.FirefoxOptions()
    options.headless = True
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "-input-symbol-list", dest="input", default="")
    args = parser.parse_args()

    root = pathlib.Path(__file__).parent.resolve()
    stock_short_folder_path = root / "stock-short"
    stock_short_historical_folder_path = stock_short_folder_path / "historical-quotes"
    if not os.path.exists(stock_short_historical_folder_path):
        os.makedirs(stock_short_historical_folder_path)

    # get stock base info
    base_info_dict = {}
    base_info = get_stock_base_info()
    for info in base_info:
        base_info_dict[info["symbol"]] = info

    # get stock info
    if args.input == "":
        symbol_list = get_af_common_data('query-stock-list', RETRY_CNT)
    else:
        symbol_list = args.input.split(",")

    logging.info(symbol_list)

    # get xsrf-token
    # ret, cookies = send_request("https://services-dynarep.ddwa.finra.org/public/reporting/v2/template/template-392fd399-3a27-480d-a500-f0e5ee205396/composite", True)
    if get_finra_cookie_by_browser(driver, RETRY_CNT) != 0:
        logging.error("get_finra_cookie_by_browser failed")
        sys.exit(1)
    logging.info(FINRA_COOKIES)

    stock_short_stat = {'update_time': str(datetime.now()), "data": {}}
    for s_i in range(len(symbol_list)):
        symbol = symbol_list[s_i]
        logging.info(f'[{s_i + 1} / {len(symbol_list)}] get {symbol} data')
        stock_short_history = get_short_data(symbol)
        if stock_short_history is None:
            if get_finra_cookie_by_browser(driver, RETRY_CNT) != 0:
                logging.error("get_finra_cookie_by_browser failed")
                sys.exit(1)
            stock_short_history = get_short_data(symbol)

        if stock_short_history and len(stock_short_history) > 0 and base_info_dict[symbol]["Shs Float"] != "-":
            stock_short_stat["data"][symbol] = \
                {"Shs Float": base_info_dict[symbol]["Shs Float"],
                 "Short Float": stock_short_history[0]["currentShortPositionQuantity"] / base_info_dict[symbol]["Shs Float"],
                 "Short Ratio": stock_short_history[0]["currentShortPositionQuantity"] / stock_short_history[0]["averageDailyVolumeQuantity"],
                 "Short Interest": stock_short_history[0]["currentShortPositionQuantity"],
                 "Avg Daily Volume": stock_short_history[0]["averageDailyVolumeQuantity"]}
            append_data = [
                ["0.5m", 1],
                ["1m", 2],
                ["1.5m", 3],
                ["3m", 6],
                ["0.5y", 12],
                ["1y", 24],
            ]
            for extra_i in range(len(append_data)):
                stock_short_stat["data"][symbol]["SF-" + append_data[extra_i][0]] = "-"
                stock_short_stat["data"][symbol]["SR-" + append_data[extra_i][0]] = "-"
                offset = append_data[extra_i][0]
                pos = append_data[extra_i][1]
                if pos < len(stock_short_history):
                    sf_0 = stock_short_stat["data"][symbol]["Short Float"]
                    sr_0 = stock_short_stat["data"][symbol]["Short Ratio"]
                    sf_pos = stock_short_history[pos]["currentShortPositionQuantity"] / base_info_dict[symbol]["Shs Float"]
                    sr_pos = stock_short_history[pos]["currentShortPositionQuantity"] / stock_short_history[pos]["averageDailyVolumeQuantity"]
                    if sf_0 != 0:
                        stock_short_stat["data"][symbol]["SF-" + offset] = (sf_0 - sf_pos) / sf_pos
                    if sr_0 != 0:
                        stock_short_stat["data"][symbol]["SR-" + offset] = (sr_0 - sr_pos) / sr_pos

            with open(stock_short_historical_folder_path / (symbol + '.json'), 'w', encoding='utf-8') as f:
                f.write(json.dumps(stock_short_history, separators=(',', ':')))

            logging.info('download stock ' + symbol + ' done')
        else:
            logging.info('stock ' + symbol + ' is null')

        time.sleep(DELAY_TIME_SEC)
        
    with open(stock_short_folder_path / 'stat.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(stock_short_stat, separators=(',', ':')))

    logging.info(stock_short_stat)
    logging.info('all task done')


if __name__ == "__main__":
    main()
