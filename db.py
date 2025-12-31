import sqlite3
from config import DB_PATH

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        entry_price REAL NOT NULL,
        target_price REAL NOT NULL,
        stop_loss REAL NOT NULL,
        signal_time TEXT NOT NULL,
        nifty_bias TEXT NOT NULL,
        reason TEXT,
        risk_per_share REAL,
        suggested_qty INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_id INTEGER NOT NULL,
        entry_price_actual REAL,
        entry_time_actual TEXT,
        exit_price REAL,
        exit_time TEXT,
        outcome TEXT,
        pnl REAL,
        r_multiple REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(prediction_id) REFERENCES predictions(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS symbol_stats (
        symbol TEXT PRIMARY KEY,
        trades INTEGER NOT NULL,
        wins INTEGER NOT NULL,
        losses INTEGER NOT NULL
    );
    """)

    conn.commit()
    conn.close()

def insert_prediction(pred):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO predictions
        (trade_date, symbol, action, entry_price, target_price, stop_loss,
         signal_time, nifty_bias, reason, risk_per_share, suggested_qty)
        VALUES (:trade_date, :symbol, :action, :entry_price, :target_price,
                :stop_loss, :signal_time, :nifty_bias, :reason,
                :risk_per_share, :suggested_qty)
    """, pred)
    prediction_id = cur.lastrowid
    conn.commit()
    conn.close()
    return prediction_id

def insert_outcome(out):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO outcomes
        (prediction_id, entry_price_actual, entry_time_actual, exit_price,
         exit_time, outcome, pnl, r_multiple)
        VALUES (:prediction_id, :entry_price_actual, :entry_time_actual,
                :exit_price, :exit_time, :outcome, :pnl, :r_multiple)
    """, out)
    outcome_id = cur.lastrowid
    conn.commit()
    conn.close()
    return outcome_id

def fetch_predictions_with_outcomes(trade_date: str):
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*, o.entry_price_actual, o.entry_time_actual,
               o.exit_price, o.exit_time, o.outcome, o.pnl, o.r_multiple
        FROM predictions p
        LEFT JOIN outcomes o ON p.id = o.prediction_id
        WHERE p.trade_date = ?
        ORDER BY p.symbol, p.signal_time
    """, (trade_date,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def update_symbol_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.symbol,
               COUNT(o.id) as trades,
               SUM(CASE WHEN o.pnl > 0 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN o.pnl <= 0 THEN 1 ELSE 0 END) as losses
        FROM predictions p
        JOIN outcomes o ON p.id = o.prediction_id
        GROUP BY p.symbol
    """)
    data = cur.fetchall()
    for symbol, trades, wins, losses in data:
        cur.execute("""
            INSERT INTO symbol_stats (symbol, trades, wins, losses)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                trades = excluded.trades,
                wins = excluded.wins,
                losses = excluded.losses
        """, (symbol, trades, wins, losses))
    conn.commit()
    conn.close()

def get_symbol_stats():
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM symbol_stats")
    rows = cur.fetchall()
    conn.close()
    stats = {}
    for r in rows:
        trades = r["trades"]
        wins = r["wins"]
        win_rate = wins / trades if trades > 0 else 0.0
        stats[r["symbol"]] = {"trades": trades, "win_rate": win_rate}
    return stats
