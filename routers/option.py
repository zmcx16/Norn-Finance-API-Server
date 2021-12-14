from typing import Optional
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from rate_limiter import limiter
from models import option


class OptionsChainQuotesResponse(BaseModel):
    symbols: str


router = APIRouter(
    prefix="/option",
    tags=["option"]
)


@router.get("/quote", tags=["quote"], response_model=OptionsChainQuotesResponse)
@limiter.app_limiter.limit("3/minute")
async def options_chain_quotes(request: Request, response: Response, symbols: str, min_next_days: Optional[int] = 0, max_next_days: Optional[int] = 30):
    if not symbols:
        raise HTTPException(status_code=400, detail="Invalid request parameter")

    option.get_option_chain('T')
    return {"symbols": symbols}
