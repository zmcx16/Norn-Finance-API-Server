import sys
import os
import json
import pathlib
import time
import logging
import requests
import traceback
from datetime import datetime
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from lxml import etree


DELAY_TIME_SEC = 1
RETRY_FAILED_DELAY = 20
RETRY_CNT = 10
afscreener_url = os.environ.get("AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")


def send_request(url, for_cookie=False):
    for r in range(RETRY_CNT):
        res = None
        try:
            res = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            })
            res.raise_for_status()
        except Exception as ex:
            logging.error('Generated an exception: {ex}'.format(ex=ex))

        if res is None:
            logging.error('send_request failed and not to retry')
            return -3, "res is None", ""

        if res.status_code == 200:
            if for_cookie:
                return 0, res.cookies.get_dict(), res.url
            return 0, res.text, res.url

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt", ""


def get_stock_info():
    try:
        # get stock info
        param = {
            'code': afscreener_token,
            'api': 'get-stock-info-from-db'
        }
        encoded_args = urlencode(param)
        query_url = afscreener_url + '?' + encoded_args
        ret, content, _ = send_request(query_url, False)
        if ret == 0:
            resp = json.loads(content)
            if resp["ret"] == 0:
                return resp["data"]
            else:
                print('server err = {err}, msg = {msg}'.format(err=resp["ret"], msg=resp["err_msg"]))
                sys.exit(1)
        else:
            print('send_request failed: {ret}'.format(ret=ret))
            sys.exit(1)

    except Exception as ex:
        print('Generated an exception: {ex}'.format(ex=ex))
        sys.exit(1)


def main():

    logging.basicConfig(level="INFO")

    root = pathlib.Path(__file__).parent.resolve()
    employees_path = root / 'employees.json'

    # get stock list
    stock_info = get_stock_info()
    logging.info(stock_info)

    # get employees
    employees_stat = {'update_time': str(datetime.now()), "data": []}
    dt = []
    s_i = 0
    for symbol in stock_info:
        s_i += 1
        logging.info(f'[{s_i} / {len(stock_info)}] get {symbol} data')
        employees_url = f'https://www.macrotrends.net/stocks/charts/{symbol}//number-of-employees'
        ret, resp, url = send_request(employees_url, False)
        if ret == 0:
            try:
                soup = BeautifulSoup(resp, "html.parser")
                dom = etree.HTML(str(soup))
                tables = dom.xpath('//table[@class="historical_data_table table"]')
                if len(tables) == 0:
                    break
                employees_table = tables[0]
                rows = employees_table.xpath('.//tr')
                if len(rows) == 0:
                    logging.error('no employees data')
                    break
                employees_list = []
                for r_i in range(len(rows)):
                    row = rows[r_i]
                    if r_i == 0:
                        continue
                    tds = row.xpath('.//td')
                    if len(tds) == 0:
                        continue
                    date = tds[0].text
                    employees_cnt = tds[1].text
                    try:
                        employees_cnt = int(employees_cnt.replace(',', ''))
                        employees_list.append({'date': date, 'employees_cnt': employees_cnt})
                    except Exception as ex:
                        logging.error('Generated an exception: {ex}, {data}'.format(ex=ex, data=[tds[0].text, tds[1].text]))
                        continue

                # check all empty data
                if len(employees_list) == 0:
                    logging.error('no employees data, skip it')
                    continue

                o = {"symbol": symbol, "history": employees_list, "url": url, 'neg_count': 0, 'latest_growth': 0,
                     'avg_growth': 0, 'tags': []}
                neg_count = 0
                keep_growth = True
                keep_growth_cnt = 0
                for i in range(len(employees_list)):
                    if i >= 1:
                        if employees_list[i - 1]['employees_cnt'] <= employees_list[i]['employees_cnt']:
                            keep_growth = False
                            neg_count += 1
                        if keep_growth:
                            keep_growth_cnt += 1

                    if i == 0 and len(employees_list) > 1:
                        if employees_list[i + 1]['employees_cnt'] != 0:
                            o["latest_growth"] = \
                                (employees_list[i]['employees_cnt'] -
                                 employees_list[i + 1]['employees_cnt']) / abs(employees_list[i + 1]['employees_cnt'])
                        avg = 0
                        for j in range(len(employees_list) - 1):
                            if employees_list[j + 1]['employees_cnt'] == 0:
                                avg = 0
                                break
                            avg += \
                                (employees_list[j]['employees_cnt'] -
                                 employees_list[j + 1]['employees_cnt']) / abs(employees_list[j + 1]['employees_cnt'])
                        avg /= len(employees_list) - 1

                # tag
                if keep_growth and len(employees_list) >= 2:
                    o["tags"].append("keep_growth")
                if keep_growth_cnt >= 3:
                    o["tags"].append("keep_growth_3")
                if keep_growth_cnt >= 5:
                    o["tags"].append("keep_growth_5")
                if keep_growth_cnt >= 10:
                    o["tags"].append("keep_growth_10")

                o["neg_count"] = neg_count
                o["avg_growth"] = avg
                dt.append(o)

            except Exception as ex:
                logging.error(traceback.format_exc())
        else:
            logging.error('send_request failed: {ret}, skip it'.format(ret=ret))
        time.sleep(DELAY_TIME_SEC)

    dt = sorted(dt, key=lambda d: d['latest_growth'], reverse=True)
    for i in range(len(dt)):
        symbol = dt[i]["symbol"]
        employees_data = {
            "name": symbol,
            "symbol": symbol,
            "rank": i + 1,
            "rank_color": '',
            "tags": dt[i]["tags"],
            "extra_info": "",
            "link": dt[i]["url"],
        }
        if symbol in stock_info:
            employees_data["name"] = stock_info[symbol][0]

        if dt[i]['neg_count'] == 0:
            employees_data['rank_color'] = "#00e676"
        elif dt[i]['neg_count'] == 1:
            employees_data['rank_color'] = "#29b6f6"
        elif dt[i]['neg_count'] == 2:
            employees_data['rank_color'] = "#ffca28"
        else:
            employees_data['rank_color'] = "#f44336"

        employees_data["extra_info"] = \
            f"Latest Growth: {dt[i]['latest_growth']:.2%}\n" \
            f"Avg Growth: {dt[i]['avg_growth']:.2%}\n\n "

        # pretty history
        for j in range(len(dt[i]['history'])):
            employees_data["extra_info"] += \
                f"{dt[i]['history'][j]['date']}: {dt[i]['history'][j]['employees_cnt']}\n"

        employees_stat["data"].append(employees_data)

    with open(employees_path, 'w',
              encoding='utf-8') as f_it:
        f_it.write(json.dumps(employees_stat, separators=(',', ':')))

    logging.info(employees_stat)
    logging.info('all task done')


if __name__ == "__main__":
    main()
