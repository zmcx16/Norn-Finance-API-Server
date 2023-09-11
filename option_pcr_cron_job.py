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
from urllib.parse import urlencode

from models.option import get_option_chain
from utils import web


afscreener_url = os.environ.get(
    "AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")

OPTIONS_RANGE_DAYS = 365
RETRY_CNT = 5
DELAY_TIME_SEC = 0.8
THREAD_CNT = 1

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
                elif len(resp) == 0:
                    logging.info("{symbol} has no any contract with query conditions, skip it".format(symbol=symbol))
                else:
                    self.output.append({"symbol": symbol, "data": resp})

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
            option_data = get_option_chain(symbol, 0, range_days, 0, 0, range_days)
            logging.info(option_data)
            output = {
                "symbol": symbol,
                "calls": [],
                "puts": []
            }
            for contract in option_data:
                expiry_date = contract["expiryDate"]

                def push_contract(op_type):
                    c = {
                        "total_volume": 0,
                        "total_openInterest": 0,
                        "expiry": expiry_date
                    }
                    for op in contract[op_type]:
                        c["total_volume"] += op["volume"]
                        c["total_openInterest"] += op["openInterest"]
                    output[op_type].append(c)

                push_contract("calls")
                push_contract("puts")
            return option_data

        except Exception:
            logging.error(traceback.format_exc())

        return None


if __name__ == "__main__":

    start_time = time.perf_counter()
    root = pathlib.Path(__file__).parent.resolve()
    stock_options_pcr_folder_path = root / "options" / "put-call-ratio"
    stock_options_pcr_folder_historical_path = stock_options_pcr_folder_path / "historical"
    if not os.path.exists(stock_options_pcr_folder_historical_path):
        os.makedirs(stock_options_pcr_folder_historical_path)

    task_queue = queue.Queue()

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "-log-level", dest="log_level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

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
    output_data = []
    for worker in work_list:
        for d in worker.output:
            output_data.append(d)

    logging.info(output_data)

    logging.info('Time Taken: ' + time.strftime("%H:%M:%S", time.gmtime(time.perf_counter() - start_time)))
    logging.info('all task done')
