import datetime as dt
import yfinance as yf
import pandas as pd
from config import NIFTY_INDEX_TICKER, NIFTY_STOCKS

def get_intraday(symbol: str, trade_date: dt.date, interval: str = "5m") -> pd.DataFrame:
    start = trade_date
    end = trade_date + dt.timedelta(days=1)
    # auto_adjust=True fixes some yahoo data issues
    df = yf.download(symbol, start=start, end=end, interval=interval, progress=False, auto_adjust=True)
    if df.empty:
        return df
    
    # Handle MultiIndex columns if they exist (common yfinance update)
    if isinstance(df.columns, pd.MultiIndex):
        try:
            # Try to drop the Ticker level if it exists
            df.columns = df.columns.droplevel(1)
        except:
            pass

    if df.index.tz is not None:
        df = df.tz_convert("Asia/Kolkata").tz_localize(None)
    
    # Filter for market hours only
    return df.between_time("09:15", "15:30")

def get_prev_close(symbol: str, trade_date: dt.date) -> float:
    start = trade_date - dt.timedelta(days=5)
    end = trade_date
    df = yf.download(symbol, start=start, end=end, interval="1d", progress=False, auto_adjust=True)
    if df.empty:
        return None
        
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df.columns = df.columns.droplevel(1)
        except:
            pass

    return float(df.iloc[-1]["Close"])

def get_top_movers(trade_date: dt.date, top_n: int = 5):
    movers = []
    for symbol in NIFTY_STOCKS:
        try:
            intraday = get_intraday(symbol, trade_date, interval="15m")
            if intraday.empty:
                continue
            day_open = float(intraday.iloc[0]["Open"])
            day_close = float(intraday.iloc[-1]["Close"])
            pct_change = (day_close - day_open) / day_open * 100
            avg_vol = float(intraday["Volume"].mean())
            movers.append({"symbol": symbol, "pct_change": pct_change, "avg_vol": avg_vol})
        except Exception:
            continue

    if not movers:
        return [], []

    df = pd.DataFrame(movers)
    df = df[df["avg_vol"] > 0]
    df_sorted = df.sort_values("pct_change", ascending=False)
    top_gainers = df_sorted.head(top_n)["symbol"].tolist()
    top_losers = df_sorted.tail(top_n)["symbol"].tolist()
    return top_gainers, top_losers

def get_nifty_intraday(trade_date: dt.date) -> pd.DataFrame:
    return get_intraday(NIFTY_INDEX_TICKER, trade_date, interval="5m")
