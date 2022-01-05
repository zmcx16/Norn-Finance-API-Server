from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request, Response
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


@router.get("/quote", tags=["quote"], response_model=OptionsChainQuotesResponse)
@limiter.app_limiter.limit("100/minute")
async def options_chain_quotes(request: Request, response: Response, symbol: str, min_next_days: Optional[int] = 0,
                               max_next_days: Optional[int] = 60,
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
                                         min_next_days: Optional[int] = 0, max_next_days: Optional[int] = 60,
                                         min_volume: Optional[int] = 5,
                                         last_trade_days: Optional[int] = 3,
                                         ewma_his_vol_period: Optional[int] = 21,
                                         ewma_his_vol_lambda: Optional[float] = 0.94,
                                         proxy: Optional[str] = None):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    stock_price, ewma_his_vol, contracts = option.options_chain_quotes_valuation(symbol, min_next_days, max_next_days,
                                                                          min_volume, last_trade_days, proxy,
                                                                          ewma_his_vol_period, ewma_his_vol_lambda)
    return {"symbol": symbol, "stockPrice": stock_price, "EWMA_historicalVolatility": ewma_his_vol,
            "contracts": contracts}
