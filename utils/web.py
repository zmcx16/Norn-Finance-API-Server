import logging
import traceback

import requests
from requests.exceptions import HTTPError


def send_request(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
        return res.status_code, http_err
    except Exception as ex:
        logging.error(traceback.format_exc())
        return -1, ex

    return 0, res.text


def send_post(url, headers, req_data):
    try:
        res = requests.post(url, req_data, headers=headers)
        res.raise_for_status()
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
        return -2, http_err
    except Exception as ex:
        logging.error(traceback.format_exc())
        return -1, ex

    return 0, res.text
