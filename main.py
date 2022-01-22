import pandas as pd
import logging
import traceback

from fastapi import FastAPI, Request, Response
from fastapi.websockets import WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from rate_limiter import limiter
from routers import option, stock

# init
logging.basicConfig(level=logging.DEBUG)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# rate limiter
app = FastAPI()
app.state.limiter = limiter.app_limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# routers
app.include_router(option.router)
app.include_router(stock.router)

# gzip
app.add_middleware(GZipMiddleware, minimum_size=1000)

# cors
origins = ["http://localhost:5000", "https://project.zmcx16.moe", "https://norn-stockscreener.zmcx16.moe"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/ws", option.ws)


@app.get("/")
@limiter.app_limiter.limit("2/minute")
async def hello_norn(request: Request, response: Response):
    return {"msg": "Hello Norn"}


@app.websocket_route("/ws")
async def ws_hello_norn(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_json({"msg": "Hello Norn"})
    await websocket.close()


def gcp_api_main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
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
