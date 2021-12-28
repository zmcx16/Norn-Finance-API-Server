from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from rate_limiter import limiter
from models import option


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


class OptionsChainQuotesData(BaseModel):
    expiryDate: str
    calls: List[OptionsChainBaseData]
    puts: List[OptionsChainBaseData]


class OptionsChainQuotesResponse(BaseModel):
    symbol: str
    contracts: List[OptionsChainQuotesData]


router = APIRouter(
    prefix="/option",
    tags=["option"]
)


@router.get("/quote", tags=["quote"], response_model=OptionsChainQuotesResponse)
@limiter.app_limiter.limit("3/minute")
async def options_chain_quotes(request: Request, response: Response, symbol: str, min_next_days: Optional[int] = 0,
                               max_next_days: Optional[int] = 60,
                               min_volume: Optional[int] = 5,
                               last_trade_days: Optional[int] = 3,
                               proxy: Optional[str] = None):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    contracts = option.get_option_chain(symbol, min_next_days, max_next_days, min_volume, last_trade_days, proxy)
    return {"symbol": symbol, "contracts": contracts}
