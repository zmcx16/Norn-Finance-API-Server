import os
import sys
import time
import pathlib
import json
import logging
import traceback
import argparse
import threading
import queue
from datetime import datetime
from urllib.parse import urlencode
from fastapi.testclient import TestClient

from main import app
from utils import web


afscreener_url = os.environ.get(
    "AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")

OPTIONS_RANGE_DAYS = 365
RETRY_CNT = 5
DELAY_TIME_SEC = 0.8
THREAD_CNT = 1

nf_client = TestClient(app)
api_thread_lock = threading.Lock()


def get_stock_info():

    param = {
        'code': afscreener_token,
        'api': 'get-stock-info-from-db'
    }
    encoded_args = urlencode(param)
    query_url = afscreener_url + '?' + encoded_args

    for retry_i in range(RETRY_CNT):
        try:
            ret, content = web.send_request(query_url)
            if ret == 0:
                resp = json.loads(content)
                if resp["ret"] == 0:
                    return resp["data"]
                else:
                    logging.error('server err = {err}, msg = {msg}'.format(err=resp["ret"], msg=resp["err_msg"]))
            else:
                logging.error('send_request failed: {ret}'.format(ret=ret))

        except Exception:
            logging.error('Generated an exception: {ex}, try next target.'.format(ex=traceback.format_exc()))

        time.sleep(DELAY_TIME_SEC)

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
                range_days = data["range_days"]
                logging.info("({current_index}/{total_cnt}) get {symbol}".format(
                    current_index=FinanceAPIThread.current_index,
                    total_cnt=FinanceAPIThread.total_cnt, symbol=symbol))
                resp = FinanceAPIThread.__get_option_pcr_data(symbol, range_days)
                if resp is None:
                    logging.error("get {symbol} failed".format(symbol=symbol))
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
    def __get_option_pcr_data(symbol, range_days):
        try:
            api = "/option/get-option-pcr?symbol=" + symbol + "&range_days=" + str(range_days)

            response = nf_client.get(api)
            if response.status_code != 200:
                logging.error("get " + symbol + " option pcr data failed")
                return None

            option_data = response.json()
            logging.info(option_data)
            return option_data
        except Exception:
            logging.error(traceback.format_exc())

        return None


if __name__ == "__main__":

    start_time = time.perf_counter()
    root = pathlib.Path(__file__).parent.resolve()
    stock_options_pcr_folder_path = root / "output"
    if not os.path.exists(stock_options_pcr_folder_path):
        os.makedirs(stock_options_pcr_folder_path)

    task_queue = queue.Queue()

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "-log-level", dest="log_level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    output = {'update_time': str(datetime.now()), 'data': {}}

    # stock_info = get_stock_info()
    stock_info = ["AAPL"]
    logging.info(stock_info)
    FinanceAPIThread.reset_index(len(stock_info))
    for symbol in stock_info:
        data = {"symbol": symbol, "range_days": OPTIONS_RANGE_DAYS}
        task_queue.put(data)

    work_list = []
    for index in range(THREAD_CNT):
        work_list.append(FinanceAPIThread(index, task_queue))
        work_list[index].start()

    for worker in work_list:
        worker.join()

    # save output
    for worker in work_list:
        for d in worker.output:
            output['data'][d['symbol']] = {
                'PCR_OpenInterest': d['PCR_OpenInterest'],
                'PCR_Volume': d['PCR_Volume'],
                'calls': {
                    'totalVolume': d['calls']['totalVolume'],
                    'totalOpenInterest': d['calls']['totalOpenInterest'],
                },
                'puts': {
                    'totalVolume': d['puts']['totalVolume'],
                    'totalOpenInterest': d['puts']['totalOpenInterest'],
                },
            }

    logging.info(output)
    with open(stock_options_pcr_folder_path / 'put-call-ratio.json', 'w',
              encoding='utf-8') as f_it:
        f_it.write(json.dumps(output, separators=(',', ':')))

    logging.info('Time Taken: ' + time.strftime("%H:%M:%S", time.gmtime(time.perf_counter() - start_time)))
    logging.info('all task done')
