from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


# rate limiter
limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
@limiter.limit("10/minute")
def hello(request: Request, response: Response):
    return {"msg": "Hello Norn"}
