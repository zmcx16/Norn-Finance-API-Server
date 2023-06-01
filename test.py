import sys
import os
import json
import time
import gzip
from io import BytesIO
import logging
import requests
import traceback
from datetime import datetime
from urllib import parse
from selenium.webdriver import FirefoxOptions
from seleniumwire import webdriver

fireFoxOptions = FirefoxOptions()
fireFoxOptions.headless = True

afscreener_url = os.environ.get("AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")

DELAY_TIME_SEC = 10
RETRY_SEND_REQUEST = 10
RETRY_FAILED_DELAY = 80
UPDATE_INTERVAL = 60 * 60 * 24 * 7  # 1 week
BATCH_UPDATE = 10


def send_request(url, retry):
    for r in range(retry):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
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
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
            res = requests.post(url, req_data, headers=headers)
            res.raise_for_status()
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        if res.status_code == 200:
            return 0, res.text

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def get_stock_data_by_browser(symbol, retry):
    for r in range(retry):
        try:
            driver.get("https://www.morningstar.com/stocks/xnys/"+symbol+"/valuation")
            time.sleep(DELAY_TIME_SEC)

            for request in driver.requests:
                if request.response:
                    s = request.response.body
                    if 'component=sal-valuation' in request.url:
                        buff = BytesIO(request.response.body)
                        f = gzip.GzipFile(fileobj=buff)
                        s = f.read().decode("utf-8")
                    print(
                        request.url,
                        request.response.status_code,
                        request.response.headers['Content-Type'],
                        s
                    )

            return ""

        except Exception:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)


if __name__ == "__main__":
    seleniumwire_options = {
        'proxy': {
            'no_proxy': 'localhost,127.0.0.1,dev_server:8080'
        }
    }

    driver = webdriver.Firefox(
        options=fireFoxOptions,
        seleniumwire_options=seleniumwire_options)

    logging.basicConfig(level=logging.INFO)
    selenium_logger = logging.getLogger('seleniumwire')
    selenium_logger.setLevel(logging.ERROR)

    now = datetime.now().timestamp()

    get_stock_data_by_browser("T", RETRY_SEND_REQUEST)

    logging.info('all task done')
