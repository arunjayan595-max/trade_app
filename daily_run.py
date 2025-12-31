import datetime as dt
from db import init_db, insert_outcome, update_symbol_stats
from data_fetch import get_nifty_intraday, get_prev_close, get_top_movers
from signals import get_market_bias, generate_predictions_for_date, evaluate_outcome_for_prediction

def run_for_date(date_str: str):
    trade_date = dt.date.fromisoformat(date_str)
    
    # 1. Market bias
    nifty_df = get_nifty_intraday(trade_date)
    prev_close = get_prev_close("^NSEI", trade_date)
    bias = get_market_bias(nifty_df, prev_close)
    
    # 2. Top movers
    gainers, losers = get_top_movers(trade_date, top_n=5)
    
    # 3. Generate predictions
    preds = generate_predictions_for_date(trade_date, bias, gainers, losers)
    
    # 4. Evaluate outcomes
    for pred in preds:
        outcome = evaluate_outcome_for_prediction(pred, trade_date)
        if outcome:
            insert_outcome(outcome)

    # 5. Update learning
    update_symbol_stats()
    
    return len(preds), bias
