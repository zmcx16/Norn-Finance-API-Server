import traceback
import yfinance as yf
import logging


def get_stock_history(symbol: str, period: str, proxy=None):
    try:
        ticker = yf.Ticker(symbol)
        return ticker.history(period=period, proxy=proxy)

    except Exception:
        logging.error(traceback.format_exc())

    return None
