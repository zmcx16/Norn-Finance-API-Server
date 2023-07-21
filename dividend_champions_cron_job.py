import os
import sys
import pathlib
import logging
import json
import time
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from models.stock import get_all_dividend_list, get_dividend_history_by_yahoo


DELAY_TIME_SEC = 1
RETRY_FAILED_DELAY = 20
RETRY_CNT = 5


def send_request(url):
    for r in range(RETRY_CNT):
        try:
            res = requests.get(url)
            res.raise_for_status()
        except Exception as ex:
            print('Generated an exception: {ex}'.format(ex=ex))

        if res.status_code == 200:
            return 0, res.text

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def main():

    logging.basicConfig(level="INFO")

    root = pathlib.Path(__file__).parent.resolve()
    dividend_path = root / 'dividend'
    dividend_historical_path = dividend_path / "historical-quotes"
    dividend_champions_path = dividend_path / 'champions.xlsx'

    if not os.path.exists(dividend_path):
        os.makedirs(dividend_path)
    if not os.path.exists(dividend_historical_path):
        os.makedirs(dividend_historical_path)

    # get dividend champions
    dividend_champions_url = 'https://drive.google.com/uc?id=1D4H2OoHOFVPmCoyKBVCjxIl0Bt3RLYSz&export=download'
    res = requests.get(dividend_champions_url, allow_redirects=True)
    if res.status_code == 200:
        with open(dividend_champions_path, 'wb')as f:
            f.write(res.content)
    else:
        logging.error('download dividend champions failed')
        sys.exit(1)

    df = pd.read_excel(dividend_champions_path, sheet_name='All')
    headers = df.iloc[[1]]
    fields = ['No Years', 'Div Yield', '5Y Avg Yield', 'DGR 1Y', 'DGR 3Y', 'DGR 5Y', 'DGR 10Y', 'Payouts/ Year']
    field_map = {}
    symbol_index = -1

    champions_map = {}
    output = {'update_time': str(datetime.now()), 'data': {}}
    for i in range(len(headers.columns)):
        if headers.iloc[0, i] in fields:
            field_map[headers.iloc[0, i]] = i
        elif headers.iloc[0, i] == 'Symbol':
            symbol_index = i

    for i in range(2, len(df)):
        row = df.iloc[[i]]
        symbol = row.iloc[0, symbol_index]
        champions_map[symbol] = ""
        output['data'][symbol] = {}
        for field in fields:
            v = row.iloc[0, field_map[field]]
            if np.isnan(v):
                output['data'][symbol][field] = "-"
            else:
                output['data'][symbol][field] = v

    with open(dividend_path / 'champions.json', 'w') as f:
        f.write(json.dumps(output, separators=(',', ':')))

    # get all dividend list
    all_dividend_date_list = get_all_dividend_list()
    logging.info(all_dividend_date_list)
    with open(dividend_path / 'all_dividend_date_list.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(all_dividend_date_list, separators=(',', ':')))

    # get dividend champions history
    for d in all_dividend_date_list["data"]:
        if d["symbol"] in champions_map:
            champions_map[d["symbol"]] = d["link"]

    for symbol in champions_map:
        logging.info('fetching {symbol} dividend history'.format(symbol=symbol))
        if champions_map[symbol] == "":
            logging.info('no dividend history for {symbol}'.format(symbol=symbol))
            continue
        dividend_history = get_dividend_history_by_yahoo(symbol)
        logging.info(dividend_history)
        if len(dividend_history["data"]) == 0:
            logging.info('no dividend history for {symbol}'.format(symbol=symbol))
            continue
        with open(dividend_historical_path / (symbol + '.json'), 'w', encoding='utf-8') as f:
            f.write(json.dumps(dividend_history, separators=(',', ':')))
        time.sleep(DELAY_TIME_SEC)

    logging.info('all task done')


if __name__ == "__main__":
    main()
