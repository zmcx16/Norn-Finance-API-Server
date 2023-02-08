import sys
import os
import json
import time
import logging
import requests
import traceback
from datetime import datetime
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager


afscreener_url = os.environ.get("AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")


DELAY_TIME_SEC = 10
RETRY_SEND_REQUEST = 5
RETRY_FAILED_DELAY = 60
UPDATE_INTERVAL = 60 * 60 * 24 * 7  # 1 week
BATCH_UPDATE = 10

def send_request(url, retry):
    for r in range(retry):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
            res = requests.get(url, headers=headers)
            res.raise_for_status()
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')
        
        if res.status_code == 200:
            return 0, res.text
        
        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def send_post_json(url, retry, req_data):
    for r in range(retry):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
            res = requests.post(url, req_data, headers=headers)
            res.raise_for_status()
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        if res.status_code == 200:
            return 0, res.text

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def get_stock_data(symbol):
    ret, resp = send_request("https://hk.finance.yahoo.com/quote/" + symbol + "?p=" + symbol, RETRY_SEND_REQUEST)
    if ret != 0:
        logging.error('get yahoo data failed')
        exit(-1)

    # logging.info(resp)
    root_app_main = None
    for line in resp.splitlines():
        if "root.App.main" in line:
            temp = line.replace("root.App.main = ", "")
            temp = temp[:len(temp) - 1]
            root_app_main = json.loads(temp)
            break
    if root_app_main is None:
        logging.error('parse yahoo data failed')
        exit(-1)

    return root_app_main


def get_stock_data_by_browser(symbol, retry):
    for r in range(retry):
        try:
            driver.get("https://hk.finance.yahoo.com/quote/" + symbol + "?p=" + symbol)
            time.sleep(DELAY_TIME_SEC)
            if "err=404" in driver.current_url:
                logging.warning('auto redirect to ' + driver.current_url + ', skip it')
                return None
            root_app_main = driver.execute_script("return App.main")
            stores = root_app_main['context']['dispatcher']['stores']
            if "QuoteSummaryStore" not in stores:
                if "PageStore" in stores: # yahoo can't find the symbol
                    logging.warning("yahoo can't find the symbol")
                    return None
                else:
                    logging.warning('may occur encrypted data, retry it')
            else:
                return root_app_main
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)


def update_db(output, api):
    logging.info(f'update to db, output = {output}')
    if len(output["data"]) > 0:
        # update data to server
        param = {
            'code': afscreener_token,
            'api': api
        }
        encoded_args = urlencode(param)
        query_url = afscreener_url + '?' + encoded_args

        ret, resp = send_post_json(query_url, RETRY_SEND_REQUEST, json.dumps(output))
        if ret == 0:
            try:
                resp_json = json.loads(resp)
                if resp_json["ret"] != 0:
                    logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                    sys.exit(1)
            except Exception as ex:
                logging.error(traceback.format_exc())
        else:
            logging.error('send_post_json failed: {ret}'.format(ret=ret))
            sys.exit(1)

    output["data"] = {}


def get_af_common_data(api, retry):
    for r in range(retry):
        try:
            param = {
                'code': afscreener_token,
                'api': api
            }
            encoded_args = urlencode(param)
            query_url = afscreener_url + '?' + encoded_args

            ret, resp = send_request(query_url, RETRY_SEND_REQUEST)
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

if __name__ == "__main__":

    options = webdriver.FirefoxOptions()
    options.headless = True
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)

    logging.basicConfig(level=logging.INFO)

    now = datetime.now().timestamp()

    # get stock list
    symbol_list = get_af_common_data('query-stock-list', RETRY_SEND_REQUEST)
    # get seg data
    current_esg_data = get_af_common_data('get-esg-data', RETRY_SEND_REQUEST)
    # get recommendation data
    current_recommendation_data = get_af_common_data('get-recommendation-data', RETRY_SEND_REQUEST)

    output_esg = {"data": {}}
    output_recommendation = {"data": {}}
    for s_i in range(len(symbol_list)):
        symbol = symbol_list[s_i]
        logging.info(f'[{s_i+1} / {len(symbol_list)}] get {symbol} data [ESG:({len(output_esg["data"])}) | Recomm:({len(output_recommendation["data"])})]')

        esg_latest = symbol in current_esg_data and now - UPDATE_INTERVAL < current_esg_data[symbol]["last_update_time"]
        recommendation_latest = symbol in current_recommendation_data and now - UPDATE_INTERVAL < current_recommendation_data[symbol]["last_update_time"]
        if esg_latest and recommendation_latest:
            logging.info(f'no need update {symbol}')
            continue

        data = get_stock_data_by_browser(symbol, RETRY_SEND_REQUEST)
        if data is None:
            continue

        stores = data['context']['dispatcher']['stores']
        if "QuoteSummaryStore" not in stores:
            logging.warning('may occur encrypted data, skip going')
            break

        # parse esg data
        if not esg_latest:
            d = stores["QuoteSummaryStore"]["esgScores"]
            output_esg["data"][symbol] = {
                "socialScore": "-",
                "governanceScore": "-",
                "environmentScore": "-",
                "percentile": "-",
                "totalEsg": "-",
                "last_update_time": int(datetime.now().timestamp()),
            }

            if len(d) > 0:
                if "socialScore" in d and d["socialScore"] and "raw" in d["socialScore"]:
                    output_esg["data"][symbol]["socialScore"] = d["socialScore"]["raw"]
                if "governanceScore" in d and d["governanceScore"] and "raw" in d["governanceScore"]:
                    output_esg["data"][symbol]["governanceScore"] = d["governanceScore"]["raw"]
                if "environmentScore" in d and d["environmentScore"] and "raw" in d["environmentScore"]:
                    output_esg["data"][symbol]["environmentScore"] = d["environmentScore"]["raw"]
                if "percentile" in d and d["percentile"] and "raw" in d["percentile"]:
                    output_esg["data"][symbol]["percentile"] = d["percentile"]["raw"]
                if "totalEsg" in d and d["totalEsg"] and "raw" in d["totalEsg"]:
                    output_esg["data"][symbol]["totalEsg"] = d["totalEsg"]["raw"]
            else:
                logging.info(f'{symbol} no ESG update')

            if len(output_esg["data"]) >= BATCH_UPDATE:
                update_db(output_esg, 'update-esg-data')

        # parse recommendation data
        if not recommendation_latest:
            d = stores["QuoteSummaryStore"]["financialData"]
            output_recommendation["data"][symbol] = {
                "recommendationKey": "-",
                "recommendationMean": "-",
                "last_update_time": int(datetime.now().timestamp()),
            }

            if len(d) > 0:
                if "recommendationMean" in d and d["recommendationMean"] and "raw" in d["recommendationMean"]:
                    output_recommendation["data"][symbol]["recommendationMean"] = d["recommendationMean"]["raw"]
                if "recommendationKey" in d and d["recommendationKey"]:
                    output_recommendation["data"][symbol]["recommendationKey"] = d["recommendationKey"]
            else:
                logging.info(f'{symbol} no recommendation update')

            if len(output_recommendation["data"]) >= BATCH_UPDATE:
                update_db(output_recommendation, 'update-recommendation-data')

    # final update
    update_db(output_esg, 'update-esg-data')
    update_db(output_recommendation, 'update-recommendation-data')

    logging.info('all task done')
