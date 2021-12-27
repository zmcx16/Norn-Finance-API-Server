from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request, Response
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


router = APIRouter(
    prefix="/stock",
    tags=["stock"]
)


@router.get("/history", tags=["stock"], response_model=StockHistoryResponse)
@limiter.app_limiter.limit("3/minute")
async def stock_history(request: Request, response: Response, symbol: str, period: Optional[str] = "1y"):
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    output = stock.get_stock_history(symbol, period)
    output['Date'] = output.index
    output['Date'] = output['Date'].apply(lambda x: x.strftime('%m/%d/%Y'))
    return {"symbol": symbol, "data": output.to_dict(orient='records')}
