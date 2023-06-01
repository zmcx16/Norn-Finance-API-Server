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


def get_morningstar_valuation_data(symbol):
    output = {"data": {}}
    driver.get("https://www.morningstar.com/stocks/xnys/"+symbol+"/valuation")
    wait_cnt = 30
    req_done = False
    for i in range(wait_cnt):
        for request in driver.requests:
            #  {"Collapsed":{"rows":[{"label":"Price/Sales","salDataId":"price.sales.label","isQuantitative":false,"datum":["1.5043","1.3415","1.3357","1.6051","1.496","1.1312","1.5742","1.2019","1.0191","0.8634","0.9823","1.1786","2.2548"],"subLevel":""},{"label":"Price/Earnings","salDataId":"price.earnings.label","isQuantitative":false,"datum":["24.4167","10.3354","38.2333","18.0212","18.6029","5.4259","17.5247","19.0464","189.2308","7.6074",null,"23.019","19.8491"],"subLevel":""},{"label":"Price/Cash Flow","salDataId":"price.cash.flow.label","isQuantitative":false,"datum":["5.1552","5.2513","5.7282","6.8603","6.0997","4.4078","5.8822","4.6185","4.3375","4.0683","3.6101","4.6783","14.8633"],"subLevel":""},{"label":"Price/Book","salDataId":"price.book.label","isQuantitative":false,"datum":["2.1438","1.8813","1.7357","2.1122","1.9102","1.1304","1.5488","1.1674","1.0727","1.072","1.1314","1.2235","3.5813"],"subLevel":""}],"columnDefs":["Calendar","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","Current","5-Yr","Index"],"columnDefs_labels":["tabular.data.label.column.year","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","valuation.headers.current","valuation.headers.fiveyear","valuation.headers.index"],"userType":null,"footer":{"asOfLabel":"As of","asOfDate":"2023-05-31T00:00:00.000","indexLabel":"Index:","indexName":"Morningstar US Market TR USD","enterpriseValueCurrency":"USD"}},"Expanded":{"rows":[{"label":"Price/Forward Earnings","salDataId":"price.forward.earnings.label","isQuantitative":false,"datum":["13.1062","13.4409","11.9617","14.245","12.9199","8.0645","10.846","9.0662","7.57","7.0373","6.5274","8.6425",null],"subLevel":""},{"label":"PEG Ratio","salDataId":"peg.ratio.label","isQuantitative":false,"datum":["1.2017","2.4948","2.8757","3.0315","2.1193","1.7338","4.613",null,"9.8302","4.4541","3.1839","5.0876",null],"subLevel":""},{"label":"Earnings Yield %","salDataId":"earnings.yield.label","isQuantitative":false,"datum":["4.1","9.68","2.62","5.55","5.38","18.43","5.71","5.25","0.53","13.15","-7.5","6.31",null],"subLevel":""},{"label":"Enterprise Value (Bil)","salDataId":"enterprise.value.label","isQuantitative":false,"datum":["258.61","247.40","332.18","380.50","353.46","382.58","464.38","376.12","356.51","281.28","265.53","372.19",null],"subLevel":"","orderOfMagnitude":"Bil"},{"label":"Enterprise Value/EBIT","salDataId":"enterprise.value.ebit.label","isQuantitative":false,"datum":["19.3367","7.8409","28.6111","13.7069","14.0219","14.9351","15.7099","15.7273","29.5539","9.2641","116.7677","28.2391",null],"subLevel":""},{"label":"Enterprise Value/EBITDA","salDataId":"enterprise.value.ebitda.label","isQuantitative":false,"datum":["8.1681","4.9541","10.4734","7.0522","7.1187","7.3256","7.9101","7.176","9.8399","5.2774","12.9755","8.2523",null],"subLevel":""}],"columnDefs":["Calendar","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","Current","5-Yr","Index"],"columnDefs_labels":["tabular.data.label.column.year","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","valuation.headers.current","valuation.headers.fiveyear","valuation.headers.index"]}}
            if 'component=sal-valuation' in request.url and request.response and request.response.status_code == 200:
                buff = BytesIO(request.response.body)
                f = gzip.GzipFile(fileobj=buff)
                s = json.loads(f.read().decode("utf-8"))
                logging.info(s)
                output["data"][symbol] = {
                    "valuation": []
                }

                for date_i in range(1, len(s["Collapsed"]["columnDefs"])):  # skip first item: Calendar
                    output["data"][symbol]["valuation"].append({
                        "date": s["Collapsed"]["columnDefs"][date_i]
                    })
                for key_i in range(len(s["Collapsed"]["rows"])):
                    for date_i in range(len(s["Collapsed"]["rows"][key_i]["datum"])):
                        o = "-"
                        if s["Collapsed"]["rows"][key_i]["datum"][date_i]:
                            o = float(s["Collapsed"]["rows"][key_i]["datum"][date_i])
                        output["data"][symbol]["valuation"][date_i][s["Collapsed"]["rows"][key_i]["label"]] = o

                for key_i in range(len(s["Expanded"]["rows"])):
                    for date_i in range(len(s["Expanded"]["rows"][key_i]["datum"])):
                        o = "-"
                        if s["Expanded"]["rows"][key_i]["datum"][date_i]:
                            o = float(s["Expanded"]["rows"][key_i]["datum"][date_i])
                        output["data"][symbol]["valuation"][date_i][s["Expanded"]["rows"][key_i]["label"]] = o

                req_done = True
                break
        if req_done:
            break
        time.sleep(1)

    logging.info(output)
    return ""


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

    get_morningstar_valuation_data("T")

    logging.info('all task done')
