"""
BTC Paper Trading 50x — simulador de futuros, SEM ordens reais.
Dados públicos via ccxt/MEXC; candles BTC/USDT em 5m.
"""
import os, time, sqlite3, logging, math
from datetime import datetime, timezone

import ccxt
import pandas as pd
import requests

APP_NAME = "BTC Paper Trading 50x"
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "5m")
START_BALANCE = float(os.getenv("START_BALANCE", "100"))
MARGIN_USDT = float(os.getenv("MARGIN_USDT", "1"))
LEVERAGE = float(os.getenv("LEVERAGE", "50"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))       # 0,05% por execução (hipótese configurável)
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0002")) # 0,02% por execução (hipótese configurável)
VOLUME_MULTIPLIER = float(os.getenv("VOLUME_MULTIPLIER", "1.5"))
BREAKOUT_LOOKBACK = int(os.getenv("BREAKOUT_LOOKBACK", "20"))
STOP_LOOKBACK = int(os.getenv("STOP_LOOKBACK", "5"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
DB_PATH = os.getenv("DB_PATH", "paper_trading.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.001"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.02"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

exchange = ccxt.mexc({"enableRateLimit": True, "options": {"defaultType": "swap"}})

def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS state (
            id INTEGER PRIMARY KEY CHECK(id=1), balance REAL NOT NULL, last_candle TEXT)""")
        con.execute("INSERT OR IGNORE INTO state(id,balance,last_candle) VALUES(1,?,NULL)", (START_BALANCE,))
        con.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, opened_at TEXT, closed_at TEXT,
            side TEXT, entry REAL, stop REAL, tp1 REAL, qty REAL, remaining_qty REAL,
            partial_done INTEGER DEFAULT 0, realized_pnl REAL DEFAULT 0,
            fees REAL DEFAULT 0, status TEXT, exit_price REAL, mfe_pct REAL DEFAULT 0,
            mae_pct REAL DEFAULT 0, note TEXT)""")

def send_telegram(text, image_path=None):
    if not BOT_TOKEN or not CHAT_ID:
        logging.info("Telegram não configurado; alerta apenas no log.")
        return
    try:
        if image_path and os.path.exists(image_path):
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
            with open(image_path, "rb") as f:
                requests.post(url, data={"chat_id": CHAT_ID, "caption": text[:1024]}, files={"photo": f}, timeout=20).raise_for_status()
        else:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=20).raise_for_status()
    except Exception as e:
        logging.warning("Falha no Telegram: %s", e)

def fetch_data():
    # O mercado de futuros pode usar símbolos distintos conforme a versão da API.
    # Tentamos candles públicos do mercado swap e recorremos ao spot público se necessário.
    try:
        rows = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=max(100, BREAKOUT_LOOKBACK + 50))
    except Exception as e:
        logging.warning("Falha ao buscar swap (%s); tentando spot público: %s", SYMBOL, e)
        spot = ccxt.mexc({"enableRateLimit": True})
        rows = spot.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=max(100, BREAKOUT_LOOKBACK + 50))
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df

def indicators(df):
    df = df.copy()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
    df["vol_avg"] = df["volume"].rolling(20).mean()
    df["prior_high"] = df["high"].shift(1).rolling(BREAKOUT_LOOKBACK).max()
    df["prior_low"] = df["low"].shift(1).rolling(BREAKOUT_LOOKBACK).min()
    # Só candle fechado: último candle pode ainda estar em formação.
    return df

def state():
    with db() as con:
        row = con.execute("SELECT * FROM state WHERE id=1").fetchone()
        return dict(row)

def update_state(balance=None, last_candle=None):
    with db() as con:
        if balance is not None:
            con.execute("UPDATE state SET balance=? WHERE id=1", (balance,))
        if last_candle is not None:
            con.execute("UPDATE state SET last_candle=? WHERE id=1", (last_candle,))

def active_trade():
    with db() as con:
        row = con.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None

def open_trade(side, price, stop, candle_time, reason):
    balance = state()["balance"]
    if balance < MARGIN_USDT:
        send_telegram(f"⛔ {APP_NAME}\nSaldo virtual insuficiente: {balance:.2f} USDT.")
        return
    # Notional aproximado = margem x alavancagem; quantidade em BTC.
    notional = MARGIN_USDT * LEVERAGE
    entry = price * (1 + SLIPPAGE_RATE if side == "LONG" else 1 - SLIPPAGE_RATE)
    qty = notional / entry
    stop_pct = abs(entry - stop) / entry
    if stop_pct < MIN_STOP_PCT or stop_pct > MAX_STOP_PCT:
        logging.info("Sinal ignorado: stop muito próximo/distante (%.3f%%)", stop_pct*100)
        return
    fee = notional * FEE_RATE
    update_state(balance=balance-fee)
    tp1 = entry + (entry-stop) if side == "LONG" else entry - (stop-entry)
    with db() as con:
        con.execute("""INSERT INTO trades(opened_at,side,entry,stop,tp1,qty,remaining_qty,
                     partial_done,realized_pnl,fees,status,mfe_pct,mae_pct,note)
                     VALUES(?,?,?,?,?,?,?,0,0,?,'OPEN',0,0,?)""",
                    (utcnow(), side, entry, stop, tp1, qty, qty, fee, reason))
        tid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    msg = (f"🧪 {APP_NAME} — {side}\n"
           f"Ativo: {SYMBOL} | Período: {TIMEFRAME}\n"
           f"Entrada simulada: {entry:.2f}\nStop estrutural: {stop:.2f}\nTP1 (1R): {tp1:.2f}\n"
           f"Margem: {MARGIN_USDT:.2f} USDT | Alavancagem simulada: {LEVERAGE:.0f}x\n"
           f"Notional: {notional:.2f} USDT | Quantidade: {qty:.8f} BTC\n"
           f"Taxa estimada de entrada: {fee:.4f} USDT\nMotivo: {reason}\n"
           f"⚠️ SIMULAÇÃO: nenhuma ordem real foi enviada.")
    logging.info(msg.replace("\n", " | "))
    send_telegram(msg)

def close_qty(trade, qty, price, label, final=False):
    side = trade["side"]
    exit_price = price * (1 - SLIPPAGE_RATE if side == "LONG" else 1 + SLIPPAGE_RATE)
    gross = (exit_price-trade["entry"])*qty if side == "LONG" else (trade["entry"]-exit_price)*qty
    fee = abs(exit_price*qty)*FEE_RATE
    pnl = gross-fee
    bal = state()["balance"] + pnl
    update_state(balance=bal)
    rem = max(0.0, trade["remaining_qty"]-qty)
    total_realized = trade["realized_pnl"]+pnl
    total_fees = trade["fees"]+fee
    status = "CLOSED" if final or rem <= 1e-12 else "OPEN"
    with db() as con:
        con.execute("""UPDATE trades SET remaining_qty=?, realized_pnl=?, fees=?, status=?,
                       closed_at=CASE WHEN ?='CLOSED' THEN ? ELSE closed_at END,
                       exit_price=CASE WHEN ?='CLOSED' THEN ? ELSE exit_price END
                       WHERE id=?""",
                    (rem,total_realized,total_fees,status,status,utcnow(),status,exit_price,trade["id"]))
    send_telegram(f"📊 {APP_NAME}\n{label}\n{trade['side']} | saída: {exit_price:.2f}\n"
                  f"Resultado desta execução após custo: {pnl:+.4f} USDT\nSaldo virtual: {bal:.4f} USDT\n"
                  f"Custos acumulados da operação: {total_fees:.4f} USDT\nSem ordens reais.")
    return bal

def manage_trade(trade, row):
    high, low, close = float(row.high), float(row.low), float(row.close)
    side = trade["side"]
    entry = trade["entry"]
    favorable = (high-entry)/entry*100 if side=="LONG" else (entry-low)/entry*100
    adverse = (entry-low)/entry*100 if side=="LONG" else (high-entry)/entry*100
    mfe = max(trade["mfe_pct"], favorable)
    mae = max(trade["mae_pct"], adverse)
    with db() as con:
        con.execute("UPDATE trades SET mfe_pct=?, mae_pct=? WHERE id=?", (mfe,mae,trade["id"]))
    # Recarrega campos após atualização de MFE/MAE.
    trade = active_trade()
    stop_hit = low <= trade["stop"] if side=="LONG" else high >= trade["stop"]
    tp_hit = high >= trade["tp1"] if side=="LONG" else low <= trade["tp1"]
    # Se stop e TP ocorrerem no mesmo candle, adota cenário conservador: stop primeiro.
    if stop_hit:
        close_qty(trade, trade["remaining_qty"], trade["stop"], "STOP LOSS acionado (estimativa conservadora)", final=True)
        return
    if tp_hit and not trade["partial_done"]:
        half = trade["remaining_qty"] * 0.5
        close_qty(trade, half, trade["tp1"], "TP1 atingido: fechamento de 50% em 1R")
        with db() as con:
            con.execute("UPDATE trades SET partial_done=1 WHERE id=?", (trade["id"],))
        trade = active_trade()
        if not trade:
            return
    # Trailing de 0,5% após parcial, seguindo extremos dos últimos candles fechados.
    if trade and trade["partial_done"]:
        df = latest_df
        recent = df.iloc[-(STOP_LOOKBACK+1):-1]
        if len(recent):
            new_stop = float(recent["low"].min()) if side=="LONG" else float(recent["high"].max())
            # Limita a distância inicial de trailing a 0,5%; stop só aperta, nunca afrouxa.
            if side=="LONG":
                trail_candidate = max(new_stop, close*(1-0.005))
                if trail_candidate > trade["stop"]:
                    with db() as con: con.execute("UPDATE trades SET stop=? WHERE id=?", (trail_candidate,trade["id"]))
            else:
                trail_candidate = min(new_stop, close*(1+0.005))
                if trail_candidate < trade["stop"]:
                    with db() as con: con.execute("UPDATE trades SET stop=? WHERE id=?", (trail_candidate,trade["id"]))

def report_if_needed():
    with db() as con:
        count = con.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'").fetchone()[0]
        if count and count % 100 == 0:
            r = con.execute("""SELECT COUNT(*) n, SUM(realized_pnl) pnl,
                SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) wins,
                AVG(CASE WHEN realized_pnl>0 THEN realized_pnl END) avg_win,
                AVG(CASE WHEN realized_pnl<=0 THEN realized_pnl END) avg_loss,
                MAX(mfe_pct) max_mfe, MAX(mae_pct) max_mae FROM trades WHERE status='CLOSED'""").fetchone()
            # Evita repetir o relatório para o mesmo total.
            marker = f"report_{count}"
            sent = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reports_sent'").fetchone()
            con.execute("CREATE TABLE IF NOT EXISTS reports_sent (n INTEGER PRIMARY KEY)")
            already = con.execute("SELECT 1 FROM reports_sent WHERE n=?", (count,)).fetchone()
            if not already:
                con.execute("INSERT INTO reports_sent(n) VALUES(?)", (count,))
                send_telegram(f"📈 Relatório de {count} operações fechadas\nPnL líquido acumulado: {r['pnl'] or 0:.4f} USDT\n"
                              f"Vitórias: {r['wins'] or 0} | Taxa de acerto: {(r['wins'] or 0)/count*100:.1f}%\n"
                              f"MFE máximo registrado: {r['max_mfe'] or 0:.3f}% | MAE máximo: {r['max_mae'] or 0:.3f}%\n"
                              f"Saldo virtual: {state()['balance']:.4f} USDT")

def main():
    global latest_df
    init_db()
    logging.info("%s iniciado. PAPER TRADING, sem ordens reais.", APP_NAME)
    send_telegram(f"✅ {APP_NAME} iniciado.\nMercado: {SYMBOL} | candles: {TIMEFRAME}\n"
                  f"Saldo: {state()['balance']:.2f} USDT | margem: {MARGIN_USDT:.2f} | alavancagem simulada: {LEVERAGE:.0f}x\n"
                  "Nenhuma ordem real será enviada.")
    while True:
        try:
            df = indicators(fetch_data())
            if len(df) < 210:
                logging.warning("Ainda sem candles suficientes para EMA200.")
                time.sleep(POLL_SECONDS); continue
            latest_df = df
            # Usamos penúltimo candle porque o último pode estar aberto.
            row = df.iloc[-2]
            candle_key = str(row["time"])
            st = state()
            trade = active_trade()
            if trade:
                manage_trade(trade, row)
                report_if_needed()
            elif st["last_candle"] != candle_key:
                # Confirma tendência em 5m usando EMA50/200; sinal no candle fechado.
                vol_ok = pd.notna(row["vol_avg"]) and row["volume"] >= row["vol_avg"] * VOLUME_MULTIPLIER
                long_ok = row["close"] > row["prior_high"] and row["ema50"] > row["ema200"] and vol_ok
                short_ok = row["close"] < row["prior_low"] and row["ema50"] < row["ema200"] and vol_ok
                if long_ok:
                    stop = float(df.iloc[-(STOP_LOOKBACK+2):-2]["low"].min())
                    open_trade("LONG", float(row["close"]), stop, candle_key,
                               f"Rompimento de máxima de {BREAKOUT_LOOKBACK} candles + volume {row['volume']/row['vol_avg']:.2f}x média + EMA50>EMA200")
                elif short_ok:
                    stop = float(df.iloc[-(STOP_LOOKBACK+2):-2]["high"].max())
                    open_trade("SHORT", float(row["close"]), stop, candle_key,
                               f"Rompimento de mínima de {BREAKOUT_LOOKBACK} candles + volume {row['volume']/row['vol_avg']:.2f}x média + EMA50<EMA200")
                update_state(last_candle=candle_key)
            report_if_needed()
        except Exception:
            logging.exception("Erro no ciclo")
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
