import datetime as dt
import pandas as pd
from config import CAPITAL, RISK_PER_TRADE, ENTRY_WINDOW_START, ENTRY_WINDOW_END
from data_fetch import get_intraday
from db import insert_prediction, get_symbol_stats

def compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    df["vwap"] = (tp * df["Volume"]).cumsum() / df["Volume"].cumsum()
    return df

def get_market_bias(nifty_df: pd.DataFrame, prev_close: float) -> str:
    if nifty_df.empty or prev_close is None:
        return "Sideways"
    opening_range = nifty_df.between_time("09:15", "09:30")
    if opening_range.empty:
        return "Sideways"
    close_0930 = float(opening_range.iloc[-1]["Close"])
    if close_0930 > prev_close * 1.001:
        return "Bullish"
    elif close_0930 < prev_close * 0.999:
        return "Bearish"
    else:
        return "Sideways"

def get_opening_range_levels(df: pd.DataFrame):
    or_df = df.between_time("09:15", "09:30")
    if or_df.empty:
        return {"ORH": None, "ORL": None}
    return {
        "ORH": float(or_df["High"].max()),
        "ORL": float(or_df["Low"].min()),
    }

def within_entry_window(ts: pd.Timestamp) -> bool:
    t = ts.time()
    return ENTRY_WINDOW_START <= t <= ENTRY_WINDOW_END

def suggest_qty(entry: float, sl: float) -> int:
    risk_per_share = abs(entry - sl)
    if risk_per_share <= 0:
        return 0
    max_risk_value = CAPITAL * RISK_PER_TRADE
    qty = int(max_risk_value // risk_per_share)
    return max(qty, 0)

def generate_predictions_for_date(trade_date: dt.date, market_bias: str, top_gainers, top_losers):
    predictions = []
    symbol_stats = get_symbol_stats()

    if market_bias == "Bullish":
        candidate_symbols = top_gainers
        action = "BUY"
    elif market_bias == "Bearish":
        candidate_symbols = top_losers
        action = "SELL"
    else:
        return []

    filtered_symbols = []
    for sym in candidate_symbols:
        stats = symbol_stats.get(sym, None)
        if not stats or stats["trades"] < 10:
            filtered_symbols.append(sym)
        else:
            if stats["win_rate"] >= 0.4:
                filtered_symbols.append(sym)

    for symbol in filtered_symbols:
        intraday = get_intraday(symbol, trade_date, interval="5m")
        if intraday.empty:
            continue

        intraday = compute_vwap(intraday)
        levels = get_opening_range_levels(intraday)
        ORH, ORL = levels["ORH"], levels["ORL"]
        if ORH is None or ORL is None:
            continue

        for ts, row in intraday.iterrows():
            if not within_entry_window(ts):
                continue
            close = float(row["Close"])
            vwap = float(row["vwap"])

            if market_bias == "Bullish" and action == "BUY":
                if close > ORH and close > vwap:
                    entry_price = close
                    stop_loss = ORL
                    target_price = entry_price + 2 * (entry_price - stop_loss)
                    qty = suggest_qty(entry_price, stop_loss)
                    pred = {
                        "trade_date": trade_date.isoformat(),
                        "symbol": symbol,
                        "action": "BUY",
                        "entry_price": entry_price,
                        "target_price": target_price,
                        "stop_loss": stop_loss,
                        "signal_time": ts.strftime("%H:%M"),
                        "nifty_bias": market_bias,
                        "reason": "Bullish breakout above ORH & VWAP",
                        "risk_per_share": abs(entry_price - stop_loss),
                        "suggested_qty": qty,
                    }
                    pred_id = insert_prediction(pred)
                    pred["id"] = pred_id
                    predictions.append(pred)
                    break 

            elif market_bias == "Bearish" and action == "SELL":
                if close < ORL and close < vwap:
                    entry_price = close
                    stop_loss = ORH
                    target_price = entry_price - 2 * (stop_loss - entry_price)
                    qty = suggest_qty(entry_price, stop_loss)
                    pred = {
                        "trade_date": trade_date.isoformat(),
                        "symbol": symbol,
                        "action": "SELL",
                        "entry_price": entry_price,
                        "target_price": target_price,
                        "stop_loss": stop_loss,
                        "signal_time": ts.strftime("%H:%M"),
                        "nifty_bias": market_bias,
                        "reason": "Bearish breakdown below ORL & VWAP",
                        "risk_per_share": abs(entry_price - stop_loss),
                        "suggested_qty": qty,
                    }
                    pred_id = insert_prediction(pred)
                    pred["id"] = pred_id
                    predictions.append(pred)
                    break
    return predictions

def evaluate_outcome_for_prediction(pred, trade_date: dt.date):
    symbol = pred["symbol"]
    action = pred["action"]
    entry_price = pred["entry_price"]
    target = pred["target_price"]
    sl = pred["stop_loss"]
    signal_time_str = pred["signal_time"]

    intraday = get_intraday(symbol, trade_date, interval="5m")
    if intraday.empty:
        return None 

    signal_time_dt = dt.datetime.combine(trade_date, dt.datetime.strptime(signal_time_str, "%H:%M").time())
    candle_after = intraday[intraday.index >= signal_time_dt]
    
    if candle_after.empty:
        return {
             "prediction_id": pred["id"], "entry_price_actual": None, "entry_time_actual": None,
             "exit_price": None, "exit_time": None, "outcome": "NO_TRADE", "pnl": 0.0, "r_multiple": 0.0
        }

    entry_price_actual = float(candle_after.iloc[0]["Open"])
    entry_time_actual = candle_after.index[0]
    
    outcome = "EOD_EXIT"
    exit_price = float(intraday.iloc[-1]["Close"])
    exit_time = intraday.index[-1]

    for ts, row in intraday[intraday.index >= entry_time_actual].iterrows():
        high = float(row["High"])
        low = float(row["Low"])
        if action == "BUY":
            if low <= sl:
                outcome = "SL_HIT"; exit_price = sl; exit_time = ts; break
            elif high >= target:
                outcome = "TARGET_HIT"; exit_price = target; exit_time = ts; break
        else:
            if high >= sl:
                outcome = "SL_HIT"; exit_price = sl; exit_time = ts; break
            elif low <= target:
                outcome = "TARGET_HIT"; exit_price = target; exit_time = ts; break

    pnl_share = (exit_price - entry_price_actual) if action == "BUY" else (entry_price_actual - exit_price)
    qty = pred.get("suggested_qty", 0)
    pnl = pnl_share * qty
    risk = abs(entry_price - sl) if abs(entry_price - sl) > 0 else 0.001
    r_mult = pnl_share / risk

    return {
        "prediction_id": pred["id"],
        "entry_price_actual": entry_price_actual,
        "entry_time_actual": entry_time_actual.strftime("%H:%M"),
        "exit_price": exit_price,
        "exit_time": exit_time.strftime("%H:%M"),
        "outcome": outcome,
        "pnl": pnl,
        "r_multiple": r_mult,
    }
