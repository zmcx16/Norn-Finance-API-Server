from slowapi import Limiter
from slowapi.util import get_remote_address

app_limiter = Limiter(key_func=get_remote_address)
