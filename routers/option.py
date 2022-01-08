import time
import threading

from typing import List, Optional
from fastapi import FastAPI, APIRouter, Query, HTTPException, Request, Response
from fastapi.websockets import WebSocket
from pydantic import BaseModel

from rate_limiter import limiter
from models import option, stock, formula


class ValuationData(BaseModel):
    BSM_EWMAHisVol: float
    MC_EWMAHisVol: float
    BT_EWMAHisVol: float


class OptionsChainBaseData(BaseModel):
    lastTradeDate: str
    strike: float
    lastPrice: float
    bid: float
    ask: float
    change: float
    percentChange: float
    volume: int
    openInterest: int
    impliedVolatility: float
    valuationData: Optional[ValuationData]


class OptionsChainQuotesData(BaseModel):
    expiryDate: str
    calls: List[OptionsChainBaseData]
    puts: List[OptionsChainBaseData]


class OptionsChainQuotesResponse(BaseModel):
    symbol: str
    stockPrice: Optional[float]
    contracts: List[OptionsChainQuotesData]


class OptionsChainQuotesValuationResponse(BaseModel):
    symbol: str
    stockPrice: Optional[float]
    EWMA_historicalVolatility: Optional[float]
    contracts: List[OptionsChainQuotesData]


router = APIRouter(
    prefix="/option",
    tags=["option"]
)

ws = FastAPI()


@router.get("/quote", tags=["quote"], response_model=OptionsChainQuotesResponse)
@limiter.app_limiter.limit("100/minute")
async def options_chain_quotes(request: Request, response: Response, symbol: str, min_next_days: Optional[int] = 0,
                               max_next_days: Optional[int] = 45,
                               min_volume: Optional[int] = 5,
                               last_trade_days: Optional[int] = 3,
                               proxy: Optional[str] = None):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    contracts = option.get_option_chain(symbol, min_next_days, max_next_days, min_volume, last_trade_days, proxy)
    if len(contracts) == 0:
        return {"symbol": symbol, "contracts": []}

    stock_data = stock.get_stock_history(symbol, "1d")
    return {"symbol": symbol, "stockPrice": stock_data["Close"][len(stock_data["Close"])-1], "contracts": contracts}


@router.get("/quote-valuation", tags=["quote"], response_model=OptionsChainQuotesValuationResponse)
@limiter.app_limiter.limit("100/minute")
async def options_chain_quotes_valuation(request: Request, response: Response, symbol: str,
                                         min_next_days: Optional[int] = 0, max_next_days: Optional[int] = 45,
                                         min_volume: Optional[int] = 5,
                                         last_trade_days: Optional[int] = 3,
                                         ewma_his_vol_period: Optional[int] = 21,
                                         ewma_his_vol_lambda: Optional[float] = 0.94,
                                         proxy: Optional[str] = None):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    stock_price, ewma_his_vol, contracts = \
        option.options_chain_quotes_valuation(symbol, min_next_days, max_next_days, min_volume, last_trade_days,
                                              ewma_his_vol_period, ewma_his_vol_lambda, proxy)
    if contracts is None or len(contracts) == 0:
        return {"symbol": symbol, "contracts": []}

    return {"symbol": symbol, "stockPrice": stock_price, "EWMA_historicalVolatility": ewma_his_vol,
            "contracts": contracts}


@ws.websocket("/option/quote-valuation")
async def ws_options_chain_quotes_valuation(websocket: WebSocket, symbol: str,
                                         min_next_days: Optional[int] = 0, max_next_days: Optional[int] = 45,
                                         min_volume: Optional[int] = 5,
                                         last_trade_days: Optional[int] = 3,
                                         ewma_his_vol_period: Optional[int] = 21,
                                         ewma_his_vol_lambda: Optional[float] = 0.94,
                                         proxy: Optional[str] = None):

    start_time = time.perf_counter()
    class RunThread(threading.Thread):
        output = None

        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            stock_price, ewma_his_vol, contracts = \
                option.options_chain_quotes_valuation(symbol, min_next_days, max_next_days, min_volume, last_trade_days,
                                                      ewma_his_vol_period, ewma_his_vol_lambda, proxy)
            if contracts is None or len(contracts) == 0:
                self.output = {"symbol": symbol, "contracts": []}
            else:
                self.output = {"symbol": symbol, "stockPrice": stock_price, "EWMA_historicalVolatility": ewma_his_vol,
                               "contracts": contracts}

    await websocket.accept()
    t = RunThread()
    t.start()
    while True:
        if not t.is_alive():
            # don't pass t.output directly, may occur "Object of type longdouble is not JSON serializable"
            await websocket.send_json(OptionsChainQuotesValuationResponse(**t.output).dict())
            break
        time.sleep(0.5)

    print('Time Taken: ' + time.strftime("%H:%M:%S", time.gmtime(time.perf_counter() - start_time)))
    await websocket.close()
