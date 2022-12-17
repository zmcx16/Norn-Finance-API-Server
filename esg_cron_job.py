import sys
import os
import json
import time
import logging
import requests
import traceback
from datetime import datetime
from urllib.parse import urlencode


afscreener_url = os.environ.get(
    "AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")


DELAY_TIME_SEC = 30
RETRY_SEND_REQUEST = 3
RETRY_FAILED_DELAY = 60
UPDATE_INTERVAL = 60 * 60 * 24 * 7  # 1 week


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


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    now = datetime.now().timestamp()

    # get stock list
    symbol_list = []
    param = {
        'code': afscreener_token,
        'api': 'query-stock-list'
    }
    encoded_args = urlencode(param)
    query_url = afscreener_url + '?' + encoded_args

    ret, resp = send_request(query_url, RETRY_SEND_REQUEST)
    if ret == 0:
        try:
            resp_json = json.loads(resp)
            if resp_json["ret"] != 0:
                logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                sys.exit(1)
            else:
                symbol_list = resp_json["data"]

        except Exception as ex:
            logging.error(traceback.format_exc())
    else:
        logging.error('send_request failed: {ret}'.format(ret=ret))
        sys.exit(1)

    # get seg data
    current_esg_data = {}
    param = {
        'code': afscreener_token,
        'api': 'get-esg-data'
    }
    encoded_args = urlencode(param)
    query_url = afscreener_url + '?' + encoded_args

    ret, resp = send_request(query_url, RETRY_SEND_REQUEST)
    if ret == 0:
        try:
            resp_json = json.loads(resp)
            if resp_json["ret"] != 0:
                logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                sys.exit(1)
            else:
                current_esg_data = resp_json["data"]

        except Exception as ex:
            logging.error(traceback.format_exc())
    else:
        logging.error('send_request failed: {ret}'.format(ret=ret))
        sys.exit(1)

    output = {"data": {}}
    for s_i in range(len(symbol_list)):
        symbol = symbol_list[s_i]
        logging.info(f'[{s_i+1} / {len(symbol_list)}] get {symbol} data')

        if symbol in current_esg_data and now - UPDATE_INTERVAL < current_esg_data[symbol]["last_update_time"]:
            logging.info(f'no need update {symbol}')
            continue

        data = get_stock_data(symbol)

        stores = data['context']['dispatcher']['stores']
        if "QuoteSummaryStore" not in stores:
            logging.warning('may occur encrypted data, skip going')
            break

        d = stores["QuoteSummaryStore"]["esgScores"]
        if len(d) > 0:
            output["data"][symbol] = {
                "socialScore": d["socialScore"]["raw"],
                "governanceScore": d["governanceScore"]["raw"],
                "environmentScore": d["environmentScore"]["raw"],
                "percentile": d["percentile"]["raw"],
                "totalEsg": d["totalEsg"]["raw"],
                "last_update_time": int(datetime.now().timestamp()),
            }
        else:
            logging.info(f'no ESG update {symbol}')

        time.sleep(DELAY_TIME_SEC)

    logging.info(f'output = {output}')

    if len(output["data"]) > 0:
        # update data to server
        param = {
            'code': afscreener_token,
            'api': 'update-esg-data'
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

    logging.info('all task done')
