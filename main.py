import pandas as pd
import logging
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
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


@app.get("/")
@limiter.app_limiter.limit("2/minute")
def hello_norn(request: Request, response: Response):
    return {"msg": "Hello Norn"}
