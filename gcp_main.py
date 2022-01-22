import logging
import traceback

from fastapi.testclient import TestClient
from main import app


def gcp_api_main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    logging.basicConfig(level=logging.INFO)
    try:
        request_json = request.get_json()
        if request.args and 'message' in request.args:
            return request.args.get('message')
        elif request_json and 'message' in request_json:
            return request_json['message']
        elif request.args and 'api' in request.args and "quote-valuation" == request.args.get('api') and \
                "symbol" in request.args:
            logging.info('run quote-valuation')
            nf_client = TestClient(app)
            symbol = request.args.get('symbol')
            url = "/option/quote-valuation?symbol=" + symbol
            other_args = ["min_next_days", "max_next_days", "min_volume", "last_trade_days", "ewma_his_vol_period",
                          "ewma_his_vol_lambda", "proxy"]
            for arg in other_args:
                if arg in request.args:
                    url = url + "&" + arg + "=" + request.args.get(arg)
            return nf_client.get(url)
        else:
            return f'Hello World!'

    except Exception:
        logging.error(traceback.format_exc())
