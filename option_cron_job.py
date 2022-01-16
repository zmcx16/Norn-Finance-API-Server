import os
import sys
import time
import pathlib
import json
import logging
import traceback
import requests
import threading
import queue
from fastapi.testclient import TestClient
from urllib.parse import urlencode

from main import app


afscreener_url = os.environ.get(
    "AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")

DELAY_TIME_SEC = 3
THREAD_CNT = 3

nf_client = TestClient(app)
api_thread_lock = threading.Lock()


def send_request(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
    except Exception as ex:
        logging.error(traceback.format_exc())
        return -1, ex

    return 0, res.text


def get_stock_info():
    try:
        param = {
            'code': afscreener_token,
            'api': 'get-stock-info-from-db'
        }
        encoded_args = urlencode(param)
        query_url = afscreener_url + '?' + encoded_args
        ret, content = send_request(query_url)
        if ret == 0:
            resp = json.loads(content)
            if resp["ret"] == 0:
                return resp["data"]
            else:
                logging.error('server err = {err}, msg = {msg}'.format(err=resp["ret"], msg=resp["err_msg"]))
        else:
            logging.error('send_request failed: {ret}'.format(ret=ret))

    except Exception:
        logging.error(traceback.format_exc())

    sys.exit(1)


class FinanceAPIThread(threading.Thread):

    current_index = 0
    total_cnt = 0

    def __init__(self, id, task_queue):
        threading.Thread.__init__(self)
        self.id = id
        self.task_queue = task_queue
        self.output = []

    def run(self):
        logging.info("Thread " + str(self.id) + "start")
        while self.task_queue.qsize() > 0:
            try:
                data = self.task_queue.get()
                symbol = data["symbol"]
                logging.info("({current_index}/{total_cnt}) get {symbol}".format(
                    current_index=FinanceAPIThread.current_index,
                    total_cnt=FinanceAPIThread.total_cnt, symbol=symbol))

                resp = FinanceAPIThread.__get_option_valuation(symbol)
                if resp is None:
                    logging.error("get {symbol} failed".format(symbol=symbol))
                elif len(resp['contracts']) == 0:
                    logging.info("{symbol} has no any contract with query conditions, skip it".format(symbol=symbol))
                else:
                    self.output.append(resp)

                FinanceAPIThread.__add_index()
                time.sleep(DELAY_TIME_SEC)

            except Exception:
                logging.error(traceback.format_exc())

        logging.info("Thread " + str(self.id) + " end")

    @staticmethod
    def reset_index(total_cnt):
        api_thread_lock.acquire()
        FinanceAPIThread.current_index = 0
        FinanceAPIThread.total_cnt = total_cnt
        api_thread_lock.release()

    @staticmethod
    def __add_index():
        api_thread_lock.acquire()
        FinanceAPIThread.current_index = FinanceAPIThread.current_index + 1
        api_thread_lock.release()

    @staticmethod
    def __get_option_valuation(symbol):
        try:
            response = nf_client.get("/option/quote-valuation?symbol=" + symbol + "&ewma_his_vol_lambda=0.94")
            if response.status_code != 200:
                logging.error("get " + symbol + " option valuation failed")
                return None

            option_data = response.json()
            print(option_data)
            return option_data
        except Exception:
            logging.error(traceback.format_exc())

        return None


def get_diff(a, b):
    if a == b:
        return 0
    try:
        return (b-a) / a
    except ZeroDivisionError:
        return float('inf')


def check_over_diff(last_price, estimated_price, premium_threshold, discount_threshold, price_threshold):
    return last_price > 0 and estimated_price > 0 and last_price >= price_threshold and \
           ((premium_threshold != "NaN" and get_diff(last_price, estimated_price) >= premium_threshold) or
            (discount_threshold != "NaN" and get_diff(last_price, estimated_price) <= discount_threshold))


if __name__ == "__main__":

    start_time = time.perf_counter()
    logging.basicConfig(level=logging.DEBUG)
    root = pathlib.Path(__file__).parent.resolve()
    output_folder = root / "output"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    stock_info = get_stock_info()
    logging.info(stock_info)
    FinanceAPIThread.reset_index(len(stock_info))

    task_queue = queue.Queue()
    for symbol in stock_info:
        data = {"symbol": symbol}
        task_queue.put(data)

    work_list = []
    for index in range(THREAD_CNT):
        work_list.append(FinanceAPIThread(index, task_queue))
        work_list[index].start()

    for worker in work_list:
        worker.join()

    # save output
    output_data = []
    for worker in work_list:
        for d in worker.output:
            output_data.append(d)

    with open(output_folder / 'output.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(output_data, separators=(',', ':')))

    # with open('output.json', 'r', encoding='utf-8') as f:
    #     output_data = json.load(f)

    output_args_list = [
        {
            "price_threshold": 0.1,
            "premium_threshold": 1.00,
            "discount_threshold": "NaN",
        },
        {
            "price_threshold": 0.5,
            "premium_threshold": "NaN",
            "discount_threshold": -0.5,
        },
    ]

    for output_arg in output_args_list:
        # save bias >= threshold output
        bias_output = []
        price_threshold = output_arg["price_threshold"]
        premium_threshold = output_arg["premium_threshold"]
        discount_threshold = output_arg["discount_threshold"]

        for d in output_data:
            t = {"symbol": d["symbol"], "stockPrice": d["stockPrice"],
                 "EWMA_historicalVolatility": d["EWMA_historicalVolatility"], "contracts": []}
            for contract in d["contracts"]:
                c = {"expiryDate": "2022-01-21", "calls": [], "puts": []}
                for call in contract["calls"]:
                    if check_over_diff(call["lastPrice"], call["valuationData"]["BSM_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold) or \
                            check_over_diff(call["lastPrice"], call["valuationData"]["MC_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold) or \
                            check_over_diff(call["lastPrice"], call["valuationData"]["BT_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold):
                        c["calls"].append(call)
                for put in contract["puts"]:
                    if check_over_diff(put["lastPrice"], put["valuationData"]["BSM_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold) or \
                            check_over_diff(put["lastPrice"], put["valuationData"]["MC_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold) or \
                            check_over_diff(put["lastPrice"], put["valuationData"]["BT_EWMAHisVol"], premium_threshold, discount_threshold, price_threshold):
                        c["puts"].append(put)

                if len(c["calls"]) > 0 or len(c["puts"]) > 0:
                    t["contracts"].append(c)

            if len(t["contracts"]) > 0:
                bias_output.append(t)

        # print(bias_output)
        print(len(bias_output))
        with open(output_folder / 'output_bias_{t1}_{t2}_{t3}.json'.format(
                t1=price_threshold, t2=premium_threshold, t3=discount_threshold), 'w', encoding='utf-8') as f:
            f.write(json.dumps(bias_output, separators=(',', ':')))

    logging.info('Time Taken: ' + time.strftime("%H:%M:%S", time.gmtime(time.perf_counter() - start_time)))
    logging.info('all task done')
