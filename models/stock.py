import yfinance as yf
import logging


def get_stock_history(symbol: str, period: str):
    try:
        ticker = yf.Ticker(symbol)
        return ticker.history(period=period)

    except Exception as ex:
        logging.error('Generated an exception: {ex}'.format(ex=ex))

    return None
