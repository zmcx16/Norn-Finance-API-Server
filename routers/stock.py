import sys
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request, Response, Query
from pydantic import BaseModel

from rate_limiter import limiter
from models import stock


class StockHistoryData(BaseModel):
    Date: str
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int


class StockHistoryResponse(BaseModel):
    symbol: str
    data: List[StockHistoryData]


class StockPriceSimulationByMCResponse(BaseModel):
    data: List[List[float]]
    mean: List[float]


router = APIRouter(
    prefix="/stock",
    tags=["stock"]
)


@router.get("/history", tags=["stock"], response_model=StockHistoryResponse)
@limiter.app_limiter.limit("100/minute")
async def stock_history(request: Request, response: Response, symbol: str, period: Optional[str] = "1y",
                        proxy: Optional[str] = None, stock_src: Optional[str] = "yahoo"):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    output = stock.get_stock_history(symbol, period, proxy, stock_src)
    output['Date'] = output.index
    output['Date'] = output['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    return {"symbol": symbol, "data": output.to_dict(orient='records')}


@router.get("/price-simulation-by-mc", tags=["stock"], response_model=StockPriceSimulationByMCResponse)
@limiter.app_limiter.limit("100/minute")
async def price_simulation_by_mc(request: Request, response: Response, symbol: str,
                                 days: Optional[int] = Query(252, ge=1, le=252),
                                 ewma_his_vol_period: Optional[int] = 21,
                                 ewma_his_vol_lambda: Optional[float] = 0.94,
                                 iteration: Optional[int] = Query(100, ge=1, le=100),
                                 mu: Optional[float] = sys.float_info.max,
                                 vol: Optional[float] = sys.float_info.max,
                                 proxy: Optional[str] = None, stock_src: Optional[str] = "yahoo"):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    mu_vol_type = stock.PriceSimulationType.MANUAL_ALL
    if mu == sys.float_info.max and vol == sys.float_info.max:
        mu_vol_type = stock.PriceSimulationType.AUTO_GEN_MU_VOL
    elif mu == sys.float_info.max:
        mu_vol_type = stock.PriceSimulationType.AUTO_GEN_MU
    elif vol == sys.float_info.max:
        mu_vol_type = stock.PriceSimulationType.AUTO_GEN_VOL

    o = stock.price_simulation_all_by_mc(symbol, days, ewma_his_vol_lambda, ewma_his_vol_period, iteration,
                                         mu_vol_type, mu, vol, proxy=proxy, stock_src=stock_src)

    return {'data': o.tolist(), 'mean': o.mean(axis=0).tolist()}
