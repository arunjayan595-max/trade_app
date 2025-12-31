import datetime as dt
import sqlite3
import pandas as pd
import streamlit as st

# Initialize DB immediately so it exists on cloud
from db import init_db, get_conn, fetch_predictions_with_outcomes
init_db()

from daily_run import run_for_date

st.set_page_config(page_title="Intraday Helper", layout="wide")

def fetch_history_df():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT p.trade_date, p.symbol, p.action, p.entry_price, p.target_price, 
               p.stop_loss, p.signal_time, p.nifty_bias, o.outcome, o.pnl, o.r_multiple
        FROM predictions p
        LEFT JOIN outcomes o ON p.id = o.prediction_id
        ORDER BY p.trade_date DESC, p.symbol
    """, conn)
    conn.close()
    return df

def main():
    st.title("Intraday Research Tool")

    # Important Note for Cloud
    st.info("Note: On free cloud hosting, data may reset if the app goes to sleep. Download your CSV from the history tab to save data.")

    # Sidebar
    today = dt.date.today()
    date_input = st.sidebar.date_input("Select date", value=today)
    trade_date_str = date_input.isoformat()
    
    # === NEW: Button to run analysis ===
    if st.sidebar.button("Run Pipeline for Selected Date"):
        with st.spinner(f"Fetching data and calculating for {trade_date_str}..."):
            try:
                count, bias = run_for_date(trade_date_str)
                st.success(f"Done! Nifty Bias: {bias}. Predictions: {count}")
                st.rerun() # Refresh page to show new data
            except Exception as e:
                st.error(f"Error running pipeline: {e}")

    # Load Data
    data = fetch_predictions_with_outcomes(trade_date_str)

    st.header(f"Results for {trade_date_str}")
    if not data:
        st.warning("No data found for this date. Click 'Run Pipeline' in the sidebar.")
    else:
        for rec in data:
            with st.container():
                st.markdown(f"### {rec['symbol']} ({rec['action']})")
                c1, c2, c3 = st.columns(3)
                
                with c1:
                    st.markdown("**Plan**")
                    st.write(f"Entry: {rec['entry_price']:.2f}")
                    st.write(f"Target: {rec['target_price']:.2f}")
                    st.write(f"Stop: {rec['stop_loss']:.2f}")
                    st.write(f"Qty: {rec['suggested_qty']}")
                    
                with c2:
                    st.markdown("**Outcome**")
                    outcome = rec['outcome'] if rec['outcome'] else "Pending"
                    st.write(f"Status: **{outcome}**")
                    if rec['entry_price_actual']:
                        st.write(f"Filled: {rec['entry_price_actual']:.2f} @ {rec['entry_time_actual']}")
                    if rec['exit_price']:
                        st.write(f"Exit: {rec['exit_price']:.2f} @ {rec['exit_time']}")
                        
                with c3:
                    st.markdown("**PnL**")
                    pnl = rec['pnl'] if rec['pnl'] else 0.0
                    color = "green" if pnl > 0 else "red"
                    st.markdown(f":{color}[{pnl:.2f}]")
                    if rec['r_multiple']:
                        st.write(f"R-Mult: {rec['r_multiple']:.2f}")

                st.markdown("---")

    st.header("History & Analytics")
    hist_df = fetch_history_df()
    if not hist_df.empty:
        st.dataframe(hist_df)
        
        # Download button
        csv = hist_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "trading_history.csv", "text/csv")

if __name__ == "__main__":
    main()
