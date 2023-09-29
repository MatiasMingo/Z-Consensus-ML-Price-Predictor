import yfinance as yf
from datetime import datetime, timedelta


def get_symbol_info(symbol):
    ticker = yf.Ticker(symbol)
    market_cap = ticker.info["marketCap"]
    industry = ticker.info["industry"]
    return market_cap, industry

def get_price_change(symbol, datetime_obj):
    first_date_str = datetime_obj.strftime("%Y-%m-%d")
    second_day = datetime_obj + timedelta(days=1)
    second_day_str = second_day.strftime("%Y-%m-%d")
    ticker = yf.Ticker(symbol)

    # Get historical data for the specified date
    historical_data = ticker.history(period="1h", start=first_date_str, end=second_day_str)
    closing_price = historical_data["Close"][0]
    lowest_price = historical_data["Low"][0]
    opening_price = historical_data["Open"][0]
    highest_price = historical_data["High"][0]
    percentage_change = ((closing_price/opening_price) - 1)*100
    return percentage_change
