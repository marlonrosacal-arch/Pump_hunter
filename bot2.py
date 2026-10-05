import os
import time
import threading
import sqlite3
import json
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify


# ============================================================
# V5.2 — PUMP HUNTER / FUTURES RADAR
# ============================================================

VERSION = "V5.3"

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MEXC_BASE = "https://contract.mexc.com"

# ------------------------------------------------------------
# CONFIGURAÇÃO PRINCIPAL
# ------------------------------------------------------------

SCAN_INTERVAL = 5

MAX_CONTRACTS = 50

ALERT_INTERVAL = 60

COOLDOWN_SYMBOL = 8 * 60

CONTRACT_REFRESH = 15 * 60

# Score mínimo para permitir alerta
MIN_SCORE = 76

# Score considerado excepcional
STRONG_SCORE = 86

# RR mínimo
MIN_RR = 1.50

# Resultado do laboratório
GAIN_TARGET = 3.0
LOSS_TARGET = -5.0

# Janela completa de avaliação
LAB_WINDOW = 24 * 60 * 60

# V5.3: avaliação sem limite artificial de 15 minutos.
LAB_EXPIRY = 24 * 60 * 60
HORIZONS = {
    "30": 30,
    "60": 60,
    "180": 180,
    "300": 300,
    "600": 600,
    "900": 900,
    "1800": 1800,
    "3600": 3600,
}
DIVERGENCE_LOOKBACK = 90

# Históricos
HISTORY_SECONDS = 70 * 60

MAX_HISTORY = int(HISTORY_SECONDS / SCAN_INTERVAL) + 100

# Relatório por blocos
REPORT_BLOCK_SIZE = 100

# Relatório semanal
WEEKLY_SECONDS = 7 * 24 * 60 * 60

# Quantidade mínima para estatística de padrão
MIN_PATTERN_SAMPLE = 15


# ============================================================
# APP / ESTADO
# ============================================================

app = Flask(__name__)

session = requests.Session()

histories = {}
btc_history = deque(maxlen=MAX_HISTORY)
eth_history = deque(maxlen=MAX_HISTORY)

contracts = []

last_alert_global = 0
last_alert_symbol = {}

last_scan_at = 0
last_successful_scan = 0
last_api_error = ""

scan_count = 0

running = True

active_observations = {}

db_lock = threading.Lock()


# ============================================================
# BANCO
# ============================================================

DB_PATH = os.getenv("ANALYTICS_DB", "pump_hunter_v53.db")


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock:
        conn = db_connect()

        conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            detected_at REAL NOT NULL,
            detected_at_str TEXT NOT NULL,

            symbol TEXT NOT NULL,

            side TEXT DEFAULT 'LONG',

            alert_sent INTEGER DEFAULT 1,
            alert_sent_at REAL,

            entry_price REAL,

            stop_price REAL,
            tp1_price REAL,
            tp2_price REAL,

            score REAL,
            raw_score REAL,

            structure_score REAL,
            trend_score REAL,
            volume_score REAL,
            momentum_score REAL,
            oi_score REAL,
            volatility_score REAL,
            market_score REAL,
            entry_score REAL,

            r5 REAL,
            r10 REAL,
            r15 REAL,
            r30 REAL,
            r60 REAL,

            accel_15 REAL,
            accel_5 REAL,

            rsi REAL,
            rsi_delta REAL,

            ema9 REAL,
            ema21 REAL,
            ema50 REAL,

            oi_change REAL,

            btc15 REAL,
            btc30 REAL,
            btc60 REAL,

            eth15 REAL,
            eth30 REAL,
            eth60 REAL,

            volume_ratio REAL,

            from_high30 REAL,
            from_high60 REAL,

            breakout INTEGER DEFAULT 0,
            false_breakout INTEGER DEFAULT 0,

            setup_quality TEXT,

            reasons TEXT,

            ret_30 REAL,
            ret_60 REAL,
            ret_180 REAL,
            ret_300 REAL,
            ret_600 REAL,
            ret_900 REAL,

            mfe_900 REAL,
            mae_900 REAL,

            max_fav REAL,
            max_adv REAL,

            mfe_at REAL,
            mfe_elapsed REAL,

            mae_at REAL,
            mae_elapsed REAL,

            outcome TEXT,
            outcome_at REAL,
            outcome_elapsed REAL,

            complete INTEGER DEFAULT 0,
            completed_at REAL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        # ----------------------------------------------------
        # MIGRAÇÃO AUTOMÁTICA
        # ----------------------------------------------------

        existing = set()

        rows = conn.execute(
            "PRAGMA table_info(signals)"
        ).fetchall()

        for row in rows:
            existing.add(row["name"])

        columns = {
            "side": "TEXT DEFAULT 'LONG'",
            "alert_sent": "INTEGER DEFAULT 1",
            "alert_sent_at": "REAL",

            "stop_price": "REAL",
            "tp1_price": "REAL",
            "tp2_price": "REAL",

            "structure_score": "REAL",
            "trend_score": "REAL",
            "volume_score": "REAL",
            "momentum_score": "REAL",
            "oi_score": "REAL",
            "volatility_score": "REAL",
            "market_score": "REAL",
            "entry_score": "REAL",

            "eth15": "REAL",
            "eth30": "REAL",
            "eth60": "REAL",

            "volume_ratio": "REAL",

            "breakout": "INTEGER DEFAULT 0",
            "false_breakout": "INTEGER DEFAULT 0",

            "setup_quality": "TEXT",

            "ret_30": "REAL",
            "ret_60": "REAL",
            "ret_180": "REAL",
            "ret_300": "REAL",
            "ret_600": "REAL",
            "ret_900": "REAL",

            "mfe_900": "REAL",
            "mae_900": "REAL",

            "mfe_at": "REAL",
            "mfe_elapsed": "REAL",

            "mae_at": "REAL",
            "mae_elapsed": "REAL",

            "outcome": "TEXT",
            "outcome_at": "REAL",
            "outcome_elapsed": "REAL"
        }

        columns.update({
            "strategy": "TEXT",
            "movement_stage": "TEXT",
            "from_high30": "REAL",
            "from_high60": "REAL",
            "rsi_divergence": "TEXT",
            "rsi_divergence_strength": "REAL",
            "rsi_divergence_price_delta": "REAL",
            "rsi_divergence_rsi_delta": "REAL",
            "entry_context_json": "TEXT",
            "ret_1800": "REAL",
            "ret_3600": "REAL",
            "mfe_86400": "REAL",
            "mae_86400": "REAL",
            "hit_05": "INTEGER DEFAULT 0",
            "hit_10": "INTEGER DEFAULT 0",
            "hit_15": "INTEGER DEFAULT 0",
            "hit_20": "INTEGER DEFAULT 0",
            "hit_30": "INTEGER DEFAULT 0",
            "hit_05_elapsed": "REAL",
            "hit_10_elapsed": "REAL",
            "hit_15_elapsed": "REAL",
            "hit_20_elapsed": "REAL",
            "hit_30_elapsed": "REAL",
            "signal_number": "TEXT"
        })

        for name, definition in columns.items():
            if name not in existing:
                conn.execute(
                    f"ALTER TABLE signals ADD COLUMN {name} {definition}"
                )

        conn.commit()
        conn.close()


# ============================================================
# META
# ============================================================

def get_meta(key, default=None):
    with db_lock:
        conn = db_connect()
        row = conn.execute(
            "SELECT value FROM analytics_meta WHERE key=?",
            (key,)
        ).fetchone()
        conn.close()

    if not row:
        return default

    return row["value"]


def set_meta(key, value):
    with db_lock:
        conn = db_connect()

        conn.execute("""
        INSERT INTO analytics_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
        """, (key, str(value)))

        conn.commit()
        conn.close()


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("ERRO: BOT_TOKEN ou CHAT_ID não configurado.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = session.post(
            url,
            json=payload,
            timeout=10
        )

        if response.ok:
            return True

        print(
            "Telegram erro:",
            response.status_code,
            response.text[:300]
        )

    except Exception as e:
        print("Telegram exception:", e)

    return False


# ============================================================
# MEXC
# ============================================================

def mexc_get(path, params=None):
    global last_api_error

    try:
        response = session.get(
            MEXC_BASE + path,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        data = response.json()

        last_api_error = ""

        return data

    except Exception as e:
        last_api_error = str(e)
        return None


def refresh_contracts():
    global contracts

    data = mexc_get("/api/v1/contract/detail")

    if not data:
        return False

    items = data.get("data", [])

    new_contracts = []

    for item in items:

        symbol = item.get("symbol")

        if not symbol:
            continue

        if not symbol.endswith("_USDT"):
            continue

        new_contracts.append(symbol)

    if new_contracts:

        contracts = new_contracts

        print(
            f"[CONTRACTS] {len(contracts)} contratos USDT encontrados"
        )

        return True

    return False


def get_all_tickers():

    data = mexc_get("/api/v1/contract/ticker")

    if not data:
        return []

    raw = data.get("data", data)

    if isinstance(raw, dict):
        raw = raw.get("data", [])

    if not isinstance(raw, list):
        return []

    result = []

    for item in raw:

        symbol = item.get("symbol")

        if not symbol:
            continue

        try:
            price = float(
                item.get("lastPrice")
                or item.get("last")
                or item.get("fairPrice")
                or 0
            )

            if price <= 0:
                continue

            volume24 = float(
                item.get("volume24")
                or item.get("volume")
                or 0
            )

            amount24 = float(
                item.get("amount24")
                or item.get("amount")
                or 0
            )

            oi = float(
                item.get("holdVol")
                or item.get("openInterest")
                or 0
            )

            rise = float(
                item.get("riseFallRate")
                or 0
            ) * 100

            result.append({
                "symbol": symbol,
                "price": price,
                "volume24": volume24,
                "amount24": amount24,
                "oi": oi,
                "rise": rise,
                "timestamp": time.time()
            })

        except Exception:
            continue

    return result


# ============================================================
# INDICADORES
# ============================================================

def pct_change(old, new):

    if old is None or old == 0 or new is None:
        return 0.0

    return (new / old - 1) * 100


def value_at(history, seconds):

    if not history:
        return None

    target = time.time() - seconds

    chosen = None

    for ts, price, *rest in reversed(history):

        if ts <= target:
            chosen = price
            break

    if chosen is None:
        return None

    return chosen


def ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(values[:period]) / period

    for price in values[period:]:
        result = (
            price - result
        ) * multiplier + result

    return result


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) +
            gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) +
            losses[i]
        ) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def rolling_std(values):

    if len(values) < 2:
        return 0

    mean = sum(values) / len(values)

    variance = sum(
        (x - mean) ** 2
        for x in values
    ) / (len(values) - 1)

    return variance ** 0.5


# ============================================================
# HISTÓRICO MULTI-TIMEFRAME
# ============================================================

def get_returns(history):

    prices = [x[1] for x in history]

    current = prices[-1]

    def ret(seconds):
        old = value_at(history, seconds)

        if old is None:
            return 0

        return pct_change(old, current)

    return {
        "r5": ret(5),
        "r10": ret(10),
        "r15": ret(15),
        "r30": ret(30),
        "r60": ret(60),
        "r300": ret(300),
        "r900": ret(900)
    }


def get_candle_proxy(history, seconds):

    """
    Como estamos usando ticker em tempo real,
    fazemos uma aproximação dos timeframes utilizando
    o histórico de preços.

    1m  = 60s
    5m  = 300s
    15m = 900s
    1h  = 3600s
    """

    if not history:
        return None

    target = time.time() - seconds

    for ts, price, *rest in reversed(history):

        if ts <= target:
            return price

    return None


def timeframe_return(history, seconds):

    old = get_candle_proxy(history, seconds)

    if old is None or not history:
        return 0

    return pct_change(old, history[-1][1])


# ============================================================
# BTC / ETH
# ============================================================

def market_context():

    btc15 = timeframe_return(btc_history, 15)
    btc30 = timeframe_return(btc_history, 30)
    btc60 = timeframe_return(btc_history, 60)

    eth15 = timeframe_return(eth_history, 15)
    eth30 = timeframe_return(eth_history, 30)
    eth60 = timeframe_return(eth_history, 60)

    btc1h = timeframe_return(btc_history, 3600)
    eth1h = timeframe_return(eth_history, 3600)

    # --------------------------------------------------------
    # REGIME
    # --------------------------------------------------------

    if btc1h > 1.0 and btc60 > 0.15:
        regime = "BULLISH"

    elif btc1h < -1.0 and btc60 < -0.15:
        regime = "BEARISH"

    elif abs(btc1h) < 0.8 and abs(btc60) < 0.20:
        regime = "SIDEWAYS"

    else:
        regime = "EXPANSION"

    return {
        "btc15": btc15,
        "btc30": btc30,
        "btc60": btc60,

        "eth15": eth15,
        "eth30": eth30,
        "eth60": eth60,

        "btc1h": btc1h,
        "eth1h": eth1h,

        "regime": regime
    }


# ============================================================
# OI / VOLUME
# ============================================================

def history_field_at(history, seconds, field_index):

    if not history:
        return None

    target = time.time() - seconds

    for item in reversed(history):

        if item[0] <= target:

            if len(item) > field_index:
                return item[field_index]

    return None


def oi_change(history, seconds=60):

    if not history:
        return 0

    old = history_field_at(
        history,
        seconds,
        2
    )

    current = history[-1][2]

    if old is None or old == 0:
        return 0

    return pct_change(old, current)


def volume_ratio(history, seconds=60):

    """
    Estimativa de aceleração do volume.

    O ticker da MEXC fornece volume acumulado/24h,
    portanto usamos a variação observada no histórico.
    """

    if len(history) < 20:
        return 1.0

    old = history_field_at(
        history,
        seconds,
        3
    )

    current = history[-1][3]

    if old is None:
        return 1.0

    delta = current - old

    older = history_field_at(
        history,
        seconds * 2,
        3
    )

    if older is None:
        return 1.0

    previous_delta = old - older

    if previous_delta <= 0:
        return 1.0

    return max(
        0.0,
        delta / previous_delta
    )


# ============================================================
# ESTRUTURA / ROMPIMENTO
# ============================================================

def _pivot_indices(values, left=3, right=3):
    highs=[]
    lows=[]
    n=len(values)
    for i in range(left, n-right):
        window=values[i-left:i+right+1]
        if values[i] == max(window):
            highs.append(i)
        if values[i] == min(window):
            lows.append(i)
    return highs, lows


def rsi_divergence(prices, period=14, lookback=DIVERGENCE_LOOKBACK):
    """Detecta divergência RSI regular e devolve força normalizada.
    Bullish: preço faz fundo mais baixo e RSI fundo mais alto.
    Bearish: preço faz topo mais alto e RSI topo mais baixo.
    A divergência sozinha nunca dispara sinal.
    """
    if len(prices) < period * 3 + 10:
        return {"type": "NONE", "strength": 0.0, "price_delta": 0.0, "rsi_delta": 0.0}
    vals=prices[-lookback:]
    if len(vals) < period*2+10:
        vals=prices
    rsis=[]
    for i in range(period, len(vals)+1):
        v=rsi(vals[:i], period)
        if v is not None:
            rsis.append(v)
    if len(rsis) < 20:
        return {"type":"NONE","strength":0.0,"price_delta":0.0,"rsi_delta":0.0}
    offset=len(vals)-len(rsis)
    highs,lows=_pivot_indices(vals,3,3)
    # só pivôs que possuem RSI correspondente
    lows=[i for i in lows if i>=offset and i-offset < len(rsis)]
    highs=[i for i in highs if i>=offset and i-offset < len(rsis)]
    result={"type":"NONE","strength":0.0,"price_delta":0.0,"rsi_delta":0.0}
    if len(lows)>=2:
        a,b=lows[-2],lows[-1]
        r1,r2=rsis[a-offset],rsis[b-offset]
        pd=(vals[b]/vals[a]-1)*100 if vals[a] else 0
        rd=r2-r1
        if vals[b] < vals[a] and r2 > r1 + 1.0:
            strength=min(100.0, max(0.0, abs(pd)*8 + rd*4))
            result={"type":"BULLISH","strength":strength,"price_delta":pd,"rsi_delta":rd}
    if len(highs)>=2:
        a,b=highs[-2],highs[-1]
        r1,r2=rsis[a-offset],rsis[b-offset]
        pd=(vals[b]/vals[a]-1)*100 if vals[a] else 0
        rd=r2-r1
        if vals[b] > vals[a] and r2 < r1 - 1.0:
            strength=min(100.0, max(0.0, abs(pd)*8 + abs(rd)*4))
            result={"type":"BEARISH","strength":strength,"price_delta":pd,"rsi_delta":rd}
    return result


def structure_analysis(history):
    prices=[x[1] for x in history]
    if len(prices)<30:
        return {"bull":False,"bear":False,"breakout":False,"breakdown":False,"false_breakout":False,"false_breakdown":False,"resistance":None,"support":None,"distance_resistance":0,"distance_support":0}
    current=prices[-1]
    recent_1m=prices[-12:]
    base=prices[-72:-12] if len(prices)>=84 else prices[:-12]
    if not base:
        base=prices[:-1]
    resistance=max(base)
    support=min(base)
    old_current=prices[-13] if len(prices)>=13 else prices[0]
    breakout=current>resistance and old_current<=resistance
    breakdown=current<support and old_current>=support
    max_recent=max(recent_1m)
    min_recent=min(recent_1m)
    false_breakout=max_recent>resistance and current<resistance*0.997
    false_breakdown=min_recent<support and current>support*1.003
    ema21=ema(prices[-100:],21) if len(prices)>=21 else None
    bull=bool(ema21 is not None and current>ema21)
    bear=bool(ema21 is not None and current<ema21)
    return {
        "bull":bull,"bear":bear,
        "breakout":bool(breakout or breakdown),
        "breakout_up":bool(breakout),"breakdown_down":bool(breakdown),
        "false_breakout":bool(false_breakout),"false_breakdown":bool(false_breakdown),
        "resistance":resistance,"support":support,
        "distance_resistance":pct_change(resistance,current),
        "distance_support":pct_change(support,current)
    }


# ============================================================
# ANÁLISE PRINCIPAL
# ============================================================

def analyze(symbol, history, ticker, market):
    if len(history) < 180:
        return None
    prices=[x[1] for x in history]
    current=prices[-1]
    returns=get_returns(history)
    r5,r10,r15,r30,r60=(returns[k] for k in ("r5","r10","r15","r30","r60"))
    accel15=r15-(r30/2)
    accel5=r5-(r10-r5)
    rsi_current=rsi(prices[-120:],14)
    rsi_previous=rsi(prices[-132:-12],14) or rsi_current
    if rsi_current is None: return None
    rsi_delta=rsi_current-rsi_previous
    ema9,ema21,ema50=ema(prices[-100:],9),ema(prices[-100:],21),ema(prices[-100:],50)
    if None in (ema9,ema21,ema50): return None
    structure=structure_analysis(history)
    div=rsi_divergence(prices)
    oi=oi_change(history,60); vol_ratio=volume_ratio(history,60)
    btc15,btc30,btc60=market["btc15"],market["btc30"],market["btc60"]
    eth15,eth30,eth60=market["eth15"],market["eth30"],market["eth60"]
    # Distância do topo recente: identifica entrada tardia/exaustão.
    high_30=max(prices[-360:]) if len(prices)>=360 else max(prices)
    high_60=max(prices[-720:]) if len(prices)>=720 else max(prices)
    from_high30=(current/high_30-1)*100 if high_30 else 0
    from_high60=(current/high_60-1)*100 if high_60 else 0
    total_move_60=(current/prices[-720]-1)*100 if len(prices)>=720 else r60
    recovery=(current/min(prices[-180:])-1)*100 if prices[-180:] else 0
    stage="EARLY"
    if abs(total_move_60)>=80: stage="EXHAUSTED"
    elif abs(total_move_60)>=35: stage="MATURE"
    elif abs(total_move_60)>=15: stage="DEVELOPING"
    if recovery>8 and r30<0: stage="RECOVERY"
    # Base scores: iguais em pesos para permitir comparação LONG/SHORT.
    trend_long=(4 if current>ema9 else 0)+(5 if ema9>ema21 else 0)+(6 if ema21>ema50 else 0)
    trend_short=(4 if current<ema9 else 0)+(5 if ema9<ema21 else 0)+(6 if ema21<ema50 else 0)
    structure_long=sum([5 if structure["bull"] else 0,4 if current>ema21 else 0,3 if current>ema50 else 0,3 if r30>0 else 0,2 if r60>0 else 0,3 if structure["breakout_up"] else 0])
    structure_short=sum([5 if structure["bear"] else 0,4 if current<ema21 else 0,3 if current<ema50 else 0,3 if r30<0 else 0,2 if r60<0 else 0,3 if structure["breakdown_down"] else 0])
    volume_score=min(15,(5 if vol_ratio>=1.2 else 0)+(5 if vol_ratio>=1.5 else 0)+(5 if vol_ratio>=2 else 0))
    oi_score=min(15,(5 if oi>0.2 else 0)+(5 if oi>0.5 else 0)+(5 if oi>1 else 0))
    recent_returns=[pct_change(prices[i-1],prices[i]) for i in range(max(1,len(prices)-60),len(prices)) if prices[i-1]!=0]
    vol=rolling_std(recent_returns)
    volatility_score=min(10,(3 if vol>0.03 else 0)+(3 if vol>0.06 else 0)+(4 if 0.01<abs(r15)<2.5 else 0))
    market_long=(2 if btc15>=0 else 0)+(2 if btc60>=0 else 0)+(2 if eth15>=0 else 0)+(2 if eth60>=0 else 0)+(2 if market["regime"] in ("BULLISH","EXPANSION") else 0)
    market_short=(2 if btc15<=0 else 0)+(2 if btc60<=0 else 0)+(2 if eth15<=0 else 0)+(2 if eth60<=0 else 0)+(2 if market["regime"] in ("BEARISH","EXPANSION") else 0)
    momentum_long=min(10,(3 if r15>0.30 else 0)+(2 if r30>0.60 else 0)+(3 if accel15>0.20 else 0)+(2 if accel5>0.08 else 0))
    momentum_short=min(10,(3 if r15<-0.30 else 0)+(2 if r30<-0.60 else 0)+(3 if accel15<-0.20 else 0)+(2 if accel5<-0.08 else 0))
    # Entrada: separada da força do movimento.
    entry_long=min(5,(3 if structure["breakout_up"] else 0)+(2 if abs(structure["distance_resistance"])<0.8 else 0))
    entry_short=min(5,(3 if structure["breakdown_down"] else 0)+(2 if abs(structure["distance_support"])<0.8 else 0))
    common={"symbol":symbol,"r5":r5,"r10":r10,"r15":r15,"r30":r30,"r60":r60,"accel15":accel15,"accel5":accel5,"rsi":rsi_current,"rsi_delta":rsi_delta,"ema9":ema9,"ema21":ema21,"ema50":ema50,"oi_change":oi,"btc15":btc15,"btc30":btc30,"btc60":btc60,"eth15":eth15,"eth30":eth30,"eth60":eth60,"volume_ratio":vol_ratio,"regime":market["regime"],"breakout":structure["breakout"],"false_breakout":structure["false_breakout"],"from_high30":from_high30,"from_high60":from_high60,"rsi_divergence":div["type"],"rsi_divergence_strength":div["strength"],"rsi_divergence_price_delta":div["price_delta"],"rsi_divergence_rsi_delta":div["rsi_delta"],"movement_stage":stage}
    candidates=[]
    # LONG
    long_score=structure_long+min(trend_long,15)+volume_score+momentum_long+oi_score+volatility_score+market_long+entry_long
    lp=[]; lr=[]
    if btc15<-0.60: long_score-=8; lp.append("BTC contra")
    if btc60<-1.50: long_score-=7; lp.append("BTC forte queda")
    if eth15<-0.60: long_score-=4; lp.append("ETH contra")
    if rsi_current>78: long_score-=10; lp.append("RSI esticado")
    if oi<-0.50: long_score-=6; lp.append("OI negativo")
    if current<ema21: long_score-=8; lp.append("abaixo EMA21")
    if ema21<ema50: long_score-=6; lp.append("EMA21 < EMA50")
    if structure["false_breakout"]: long_score-=15; lp.append("falso rompimento")
    if r30>2.5: long_score-=8; lp.append("entrada esticada")
    if stage in ("MATURE","EXHAUSTED") and r30>0: long_score-=10; lp.append("movimento avançado")
    if stage=="RECOVERY" and not (div["type"]=="BULLISH" and rsi_delta>0): long_score-=8; lp.append("pullback sem confirmação")
    if div["type"]=="BULLISH": long_score+=4; lr.append(f"Divergência RSI bullish ({div['strength']:.0f})")
    if div["type"]=="BEARISH": long_score-=7; lp.append("divergência RSI bearish")
    # reversal ganha pontos somente com confirmação estrutural/momentum
    if div["type"]=="BULLISH" and rsi_delta>0 and r5>0 and current>ema9: long_score+=4; lr.append("reversão confirmada")
    long_score=max(0,min(100,long_score))
    if long_score>=MIN_SCORE and r5>0 and r15>0 and rsi_current>=50:
        strategy="LONG_BREAKOUT" if structure["breakout_up"] else ("LONG_REVERSAL" if div["type"]=="BULLISH" and stage in ("RECOVERY","MATURE") else ("LONG_ACCEL" if accel15>0.20 and r15>0 else "LONG_TREND"))
        entry=current; stop=structure["support"]*0.998 if structure["support"] else current*0.985; risk=entry-stop
        if risk>0:
            reasons=lr[:]
            if structure["breakout_up"]: reasons.append("Rompimento bullish")
            if vol_ratio>=1.2: reasons.append(f"Volume x{vol_ratio:.1f}")
            if oi>0.2: reasons.append(f"OI +{oi:.2f}%")
            if ema9>ema21: reasons.append("EMA9 > EMA21")
            if accel15>0.20: reasons.append("Aceleração")
            if btc15>=0: reasons.append("BTC alinhado")
            candidates.append({**common,"side":"LONG","score":long_score,"raw_score":long_score,"structure_score":structure_long,"trend_score":trend_long,"volume_score":volume_score,"momentum_score":momentum_long,"oi_score":oi_score,"volatility_score":volatility_score,"market_score":market_long,"entry_score":entry_long,"entry":entry,"stop":stop,"tp1":entry+risk*1.8,"tp2":entry+risk*2.7,"rr1":1.8,"rr2":2.7,"quality":"STRONG" if long_score>=STRONG_SCORE else ("GOOD" if long_score>=82 else "OPPORTUNITY"),"strategy":strategy,"reasons":reasons,"penalties":lp})
    # SHORT
    short_score=structure_short+min(trend_short,15)+volume_score+momentum_short+oi_score+volatility_score+market_short+entry_short
    sp=[]; sr=[]
    if btc15>0.60: short_score-=8; sp.append("BTC contra SHORT")
    if btc60>1.50: short_score-=7; sp.append("BTC forte alta")
    if eth15>0.60: short_score-=4; sp.append("ETH contra SHORT")
    if rsi_current<22: short_score-=4; sp.append("RSI muito baixo")
    if oi<-0.50: short_score-=6; sp.append("OI negativo")
    if current>ema21: short_score-=8; sp.append("acima EMA21")
    if ema21>ema50: short_score-=6; sp.append("EMA21 > EMA50")
    if structure["false_breakdown"]: short_score-=15; sp.append("falso breakdown")
    if r30<-2.5: short_score-=8; sp.append("SHORT esticado")
    if div["type"]=="BEARISH": short_score+=5; sr.append(f"Divergência RSI bearish ({div['strength']:.0f})")
    if div["type"]=="BULLISH": short_score-=7; sp.append("divergência RSI bullish")
    if stage=="EXHAUSTED" and div["type"]!="BEARISH": short_score-=5; sp.append("alta exausta sem divergência")
    if div["type"]=="BEARISH" and rsi_delta<0 and r5<0 and current<ema9: short_score+=4; sr.append("reversão bearish confirmada")
    short_score=max(0,min(100,short_score))
    if short_score>=MIN_SCORE and r5<0 and r15<0 and rsi_current<=50:
        strategy="SHORT_BREAKDOWN" if structure["breakdown_down"] else ("SHORT_REVERSAL" if div["type"]=="BEARISH" and stage in ("RECOVERY","MATURE","EXHAUSTED") else ("SHORT_REJECTION" if div["type"]=="BEARISH" or structure["false_breakout"] else "SHORT_TREND"))
        entry=current; stop=structure["resistance"]*1.002 if structure["resistance"] else current*1.015; risk=stop-entry
        if risk>0:
            reasons=sr[:]
            if structure["breakdown_down"]: reasons.append("Breakdown bearish")
            if structure["false_breakout"]: reasons.append("Rejeição/falso rompimento")
            if vol_ratio>=1.2: reasons.append(f"Volume x{vol_ratio:.1f}")
            if oi>0.2: reasons.append(f"OI +{oi:.2f}%")
            if ema9<ema21: reasons.append("EMA9 < EMA21")
            candidates.append({**common,"side":"SHORT","score":short_score,"raw_score":short_score,"structure_score":structure_short,"trend_score":trend_short,"volume_score":volume_score,"momentum_score":momentum_short,"oi_score":oi_score,"volatility_score":volatility_score,"market_score":market_short,"entry_score":entry_short,"entry":entry,"stop":stop,"tp1":entry-risk*1.8,"tp2":entry-risk*2.7,"rr1":1.8,"rr2":2.7,"quality":"STRONG" if short_score>=STRONG_SCORE else ("GOOD" if short_score>=82 else "OPPORTUNITY"),"strategy":strategy,"reasons":reasons,"penalties":sp})
    if not candidates: return None
    return max(candidates,key=lambda c:(c["score"],c["entry_score"],c["volume_ratio"],abs(c["r15"])))


# ============================================================
# COOLDOWN
# ============================================================

def symbol_recently_alerted(symbol):

    last = last_alert_symbol.get(symbol, 0)

    return (
        time.time() - last
        < COOLDOWN_SYMBOL
    )
# ============================================================
# REGISTRO DOS SINAIS
# ============================================================

def register_signal(candidate, detected_at):
    symbol=candidate["symbol"]
    if symbol_recently_alerted(symbol):
        return None
    with db_lock:
        conn=db_connect()
        cur=conn.execute("""
        INSERT INTO signals (detected_at,detected_at_str,symbol,side,alert_sent,alert_sent_at,entry_price,stop_price,tp1_price,tp2_price,score,raw_score,structure_score,trend_score,volume_score,momentum_score,oi_score,volatility_score,market_score,entry_score,r5,r10,r15,r30,r60,accel_15,accel_5,rsi,rsi_delta,ema9,ema21,ema50,oi_change,btc15,btc30,btc60,eth15,eth30,eth60,volume_ratio,from_high30,from_high60,breakout,false_breakout,setup_quality,reasons) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,(detected_at,datetime.fromtimestamp(detected_at,timezone.utc).isoformat(),symbol,candidate["side"],0,None,candidate["entry"],candidate["stop"],candidate["tp1"],candidate["tp2"],candidate["score"],candidate["raw_score"],candidate["structure_score"],candidate["trend_score"],candidate["volume_score"],candidate["momentum_score"],candidate["oi_score"],candidate["volatility_score"],candidate["market_score"],candidate["entry_score"],candidate["r5"],candidate["r10"],candidate["r15"],candidate["r30"],candidate["r60"],candidate["accel15"],candidate["accel5"],candidate["rsi"],candidate["rsi_delta"],candidate["ema9"],candidate["ema21"],candidate["ema50"],candidate["oi_change"],candidate["btc15"],candidate["btc30"],candidate["btc60"],candidate["eth15"],candidate["eth30"],candidate["eth60"],candidate["volume_ratio"],candidate["from_high30"],candidate["from_high60"],int(candidate["breakout"]),int(candidate["false_breakout"]),candidate["quality"],json.dumps({"reasons":candidate["reasons"],"penalties":candidate["penalties"]},ensure_ascii=False)))
        signal_id=cur.lastrowid
        context={k:candidate.get(k) for k in ("strategy","movement_stage","rsi_divergence","rsi_divergence_strength","rsi_divergence_price_delta","rsi_divergence_rsi_delta","regime","btc15","btc30","btc60","eth15","eth30","eth60","ema9","ema21","ema50","rsi","rsi_delta","r5","r10","r15","r30","r60","accel15","accel5","volume_ratio","oi_change","from_high30","from_high60","score","raw_score","structure_score","trend_score","volume_score","momentum_score","oi_score","volatility_score","market_score","entry_score","entry","stop","tp1","tp2","rr1","rr2")}
        conn.execute("UPDATE signals SET signal_number=?,strategy=?,movement_stage=?,rsi_divergence=?,rsi_divergence_strength=?,rsi_divergence_price_delta=?,rsi_divergence_rsi_delta=?,entry_context_json=? WHERE id=?",(f"SINAL #{signal_id:06d}",candidate.get("strategy"),candidate.get("movement_stage"),candidate.get("rsi_divergence"),candidate.get("rsi_divergence_strength"),candidate.get("rsi_divergence_price_delta"),candidate.get("rsi_divergence_rsi_delta"),json.dumps(context,ensure_ascii=False),signal_id))
        conn.commit(); conn.close()
    active_observations[signal_id]={"id":signal_id,"symbol":symbol,"side":candidate["side"],"entry":candidate["entry"],"detected_at":detected_at,"max_fav":0.0,"max_adv":0.0,"mfe_at":detected_at,"mfe_elapsed":0.0,"mae_at":detected_at,"mae_elapsed":0.0,"outcome":None,"outcome_at":None,"outcome_elapsed":None,"hits":{"0.5":None,"1.0":None,"1.5":None,"2.0":None,"3.0":None}}
    last_alert_symbol[symbol]=time.time()
    return signal_id


def mark_alert_sent(signal_id):
    with db_lock:
        conn=db_connect(); conn.execute("UPDATE signals SET alert_sent=1,alert_sent_at=? WHERE id=?",(time.time(),signal_id)); conn.commit(); conn.close()


def discard_signal(signal_id):
    active_observations.pop(signal_id,None)
    with db_lock:
        conn=db_connect(); conn.execute("DELETE FROM signals WHERE id=?",(signal_id,)); conn.commit(); conn.close()


# ============================================================
# RETORNO AJUSTADO PARA LONG / SHORT
# ============================================================

def side_return(side, entry, price):

    if not entry or entry <= 0:
        return 0.0

    raw = pct_change(
        entry,
        price
    )

    if side == "SHORT":
        return -raw

    return raw


# ============================================================
# LABORATÓRIO — ACOMPANHAMENTO
# ============================================================

def update_active_observations(tickers, now):
    prices={x["symbol"]:x["price"] for x in tickers}
    for signal_id,obs in list(active_observations.items()):
        price=prices.get(obs["symbol"])
        if price is None: continue
        elapsed=now-obs["detected_at"]
        ret=side_return(obs["side"],obs["entry"],price)
        if ret>obs["max_fav"]: obs["max_fav"]=ret; obs["mfe_at"]=now; obs["mfe_elapsed"]=elapsed
        if ret<obs["max_adv"]: obs["max_adv"]=ret; obs["mae_at"]=now; obs["mae_elapsed"]=elapsed
        for threshold,key in ((0.5,"05"),(1.0,"10"),(1.5,"15"),(2.0,"20"),(3.0,"30")):
            if obs["hits"].get(key) is None and ret>=threshold:
                obs["hits"][key]=elapsed
        if obs["outcome"] is None:
            if ret>=GAIN_TARGET:
                obs["outcome"]="GAIN"; obs["outcome_at"]=now; obs["outcome_elapsed"]=elapsed
            elif ret<=LOSS_TARGET:
                obs["outcome"]="LOSS"; obs["outcome_at"]=now; obs["outcome_elapsed"]=elapsed
            elif elapsed>=LAB_EXPIRY:
                obs["outcome"]="EXPIRED"; obs["outcome_at"]=now; obs["outcome_elapsed"]=elapsed
        fields={}
        for key,seconds in HORIZONS.items():
            if elapsed>=seconds: fields[f"ret_{key}"]=ret
        if elapsed>=LAB_EXPIRY:
            fields.update({"mfe_86400":obs["max_fav"],"mae_86400":obs["max_adv"],"max_fav":obs["max_fav"],"max_adv":obs["max_adv"],"mfe_at":obs["mfe_at"],"mfe_elapsed":obs["mfe_elapsed"],"mae_at":obs["mae_at"],"mae_elapsed":obs["mae_elapsed"],"outcome":obs["outcome"],"outcome_at":obs["outcome_at"],"outcome_elapsed":obs["outcome_elapsed"],"complete":1,"completed_at":now})
        for key,col in (("05","hit_05"),("10","hit_10"),("15","hit_15"),("20","hit_20"),("30","hit_30")):
            if obs["hits"].get(key) is not None: fields[col]=1; fields[f"{col}_elapsed"]=obs["hits"][key]
        if fields:
            with db_lock:
                conn=db_connect(); assignments=", ".join(f"{k}=?" for k in fields); conn.execute(f"UPDATE signals SET {assignments} WHERE id=?",(*fields.values(),signal_id)); conn.commit(); conn.close()
        if obs["outcome"] is not None and obs["outcome"] in ("GAIN","LOSS"):
            # continua coletando até 24h para MFE/MAE e horizontes; só remove no expiry.
            pass
        if elapsed>=LAB_EXPIRY:
            active_observations.pop(signal_id,None)



# ============================================================
# RECUPERA OBSERVAÇÕES APÓS RESTART
# ============================================================

def load_pending_observations():

    active_observations.clear()

    cutoff = (
        time.time()
        - LAB_EXPIRY
        - 300
    )

    with db_lock:

        conn = db_connect()

        rows = conn.execute("""
        SELECT
            id,
            symbol,
            side,
            entry_price,
            detected_at,

            max_fav,
            max_adv,

            mfe_at,
            mfe_elapsed,

            mae_at,
            mae_elapsed,

            outcome,
            outcome_at,
            outcome_elapsed,
            hit_05_elapsed, hit_10_elapsed, hit_15_elapsed, hit_20_elapsed, hit_30_elapsed

        FROM signals

        WHERE alert_sent=1
        AND complete=0
        AND detected_at >= ?
        """, (
            cutoff,
        )).fetchall()

        conn.close()

    for row in rows:

        active_observations[
            row["id"]
        ] = {

            "id":
                row["id"],

            "symbol":
                row["symbol"],

            "side":
                row["side"] or "LONG",

            "entry":
                row["entry_price"],

            "detected_at":
                row["detected_at"],

            "max_fav":
                row["max_fav"] or 0.0,

            "max_adv":
                row["max_adv"] or 0.0,

            "mfe_at":
                row["mfe_at"]
                or row["detected_at"],

            "mfe_elapsed":
                row["mfe_elapsed"]
                or 0.0,

            "mae_at":
                row["mae_at"]
                or row["detected_at"],

            "mae_elapsed":
                row["mae_elapsed"]
                or 0.0,

            "outcome":
                row["outcome"],

            "outcome_at":
                row["outcome_at"],

            "outcome_elapsed": row["outcome_elapsed"],
            "hits": {
                "05": row["hit_05_elapsed"], "10": row["hit_10_elapsed"], "15": row["hit_15_elapsed"], "20": row["hit_20_elapsed"], "30": row["hit_30_elapsed"]
            }
        }


# ============================================================
# RELATÓRIO — DADOS
# ============================================================

def completed_rows_after(
    cursor_id,
    limit=100
):

    with db_lock:

        conn = db_connect()

        rows = conn.execute("""
        SELECT *
        FROM signals

        WHERE id > ?

        AND alert_sent=1

        AND complete=1

        AND outcome IN (
            'GAIN',
            'LOSS',
            'EXPIRED'
        )

        ORDER BY id ASC

        LIMIT ?
        """, (
            cursor_id,
            limit
        )).fetchall()

        conn.close()

    return rows


def completed_since(seconds):

    cutoff = (
        time.time()
        - seconds
    )

    with db_lock:

        conn = db_connect()

        rows = conn.execute("""
        SELECT *
        FROM signals

        WHERE alert_sent=1

        AND complete=1

        AND outcome IN (
            'GAIN',
            'LOSS',
            'EXPIRED'
        )

        AND completed_at >= ?

        ORDER BY id ASC
        """, (
            cutoff,
        )).fetchall()

        conn.close()

    return rows


# ============================================================
# ESTATÍSTICAS
# ============================================================

def report_metrics(rows):

    total = len(rows)

    if total == 0:
        return None

    gains = [
        r for r in rows
        if r["outcome"] == "GAIN"
    ]

    losses = [
        r for r in rows
        if r["outcome"] == "LOSS"
    ]

    expired = [
        r for r in rows
        if r["outcome"] == "EXPIRED"
    ]

    def average(
        column,
        subset
    ):

        values = [
            float(r[column])
            for r in subset
            if r[column] is not None
        ]

        if not values:
            return 0.0

        return (
            sum(values)
            / len(values)
        )

    return {

        "total":
            total,

        "gain":
            len(gains),

        "loss":
            len(losses),

        "expired":
            len(expired),

        "gain_pct":
            len(gains)
            / total
            * 100,

        "loss_pct":
            len(losses)
            / total
            * 100,

        "expired_pct":
            len(expired)
            / total
            * 100,

        "mfe": average("mfe_900", rows),

        "mae": average("mae_900", rows),

        "ret5":
            average(
                "ret_300",
                rows
            ),

        "ret15":
            average(
                "ret_900",
                rows
            ),

        "gain_time":
            average(
                "outcome_elapsed",
                gains
            ),

        "loss_time": average("outcome_elapsed", losses),
        "expired_time": average("outcome_elapsed", expired),
        "hit_rates": {k: (sum(1 for r in rows if r[f"hit_{k}"]) / total * 100) for k in ("05","10","15","20","30")},
        "ret30m": average("ret_1800", rows),
        "ret60m": average("ret_3600", rows)
    }


# ============================================================
# PADRÕES
# ============================================================

def bucket_lines(rows):

    buckets = [

        (
            "Score 76-81",
            lambda r:
                76 <=
                (r["score"] or 0)
                < 82
        ),

        (
            "Score 82-85",
            lambda r:
                82 <=
                (r["score"] or 0)
                < 86
        ),

        (
            "Score 86+",
            lambda r:
                (r["score"] or 0)
                >= 86
        ),

        (
            "LONG",
            lambda r:
                r["side"] == "LONG"
        ),

        (
            "SHORT",
            lambda r:
                r["side"] == "SHORT"
        ),

        (
            "Rompimento",
            lambda r:
                bool(
                    r["breakout"]
                )
        ),

        (
            "OI positivo",
            lambda r:
                (r["oi_change"] or 0)
                > 0.20
        ),

        (
            "Volume >= 1.5x",
            lambda r: (r["volume_ratio"] or 0) >= 1.5
        ),
        ("Divergência RSI bullish", lambda r: r["rsi_divergence"] == "BULLISH"),
        ("Divergência RSI bearish", lambda r: r["rsi_divergence"] == "BEARISH"),
        ("LONG_REVERSAL", lambda r: r["strategy"] == "LONG_REVERSAL"),
        ("SHORT_REVERSAL", lambda r: r["strategy"] == "SHORT_REVERSAL"),
        ("Entrada avançada", lambda r: r["movement_stage"] in ("MATURE","EXHAUSTED")),
        ("Pullback/Recovery", lambda r: r["movement_stage"] == "RECOVERY")
    ]

    output = []

    for name, condition in buckets:

        subset = [
            r
            for r in rows
            if condition(r)
        ]

        if len(subset) < MIN_PATTERN_SAMPLE:
            continue

        metrics = report_metrics(
            subset
        )

        output.append(
            f"• {name}: "
            f"n={metrics['total']} | "
            f"G {metrics['gain_pct']:.0f}% | "
            f"L {metrics['loss_pct']:.0f}% | "
            f"N {metrics['neutral_pct']:.0f}% | "
            f"MFE {metrics['mfe']:.2f}%"
        )

    return output


# ============================================================
# CONSTRÓI RELATÓRIO
# ============================================================

def build_report(
    title,
    rows
):

    metrics = report_metrics(
        rows
    )

    if not metrics:

        return (
            f"*{title}*\n\n"
            "Nenhum sinal concluído no período."
        )

    text = [

        f"📊 *{title}*",
        "",

        f"Sinais avaliados: "
        f"*{metrics['total']}*",

        "",

        f"🟢 GAIN: "
        f"*{metrics['gain']} "
        f"({metrics['gain_pct']:.1f}%)*",

        f"🔴 LOSS: "
        f"*{metrics['loss']} "
        f"({metrics['loss_pct']:.1f}%)*",

        f"⚪ EXPIRED: *{metrics['expired']} ({metrics['expired_pct']:.1f}%)*",

        "",

        f"MFE médio 15m: *{metrics['mfe']:.2f}%*",

        f"MAE médio 15m: "
        f"*{metrics['mae']:.2f}%*",

        f"Retorno médio 5m: "
        f"*{metrics['ret5']:.2f}%*",

        f"Retorno médio 15m: *{metrics['ret15']:.2f}%*",
        f"Retorno médio 30m: *{metrics['ret30m']:.2f}%*",
        f"Retorno médio 60m: *{metrics['ret60m']:.2f}%*",
        "",
        "🎯 *Hits antes do desfecho:*",
        f"+0,5%: {metrics['hit_rates']['05']:.1f}% | +1%: {metrics['hit_rates']['10']:.1f}%",
        f"+1,5%: {metrics['hit_rates']['15']:.1f}% | +2%: {metrics['hit_rates']['20']:.1f}% | +3%: {metrics['hit_rates']['30']:.1f}%",

        f"Tempo médio GAIN: "
        f"*{metrics['gain_time']/60:.1f} min*",

        f"Tempo médio LOSS: "
        f"*{metrics['loss_time']/60:.1f} min*"
    ]

    patterns = bucket_lines(
        rows
    )

    if patterns:

        text += [
            "",
            "*Padrões com amostra suficiente:*"
        ]

        text += patterns

    text += [
        "",
        "ℹ️ Relatório diagnóstico.",
        "Nenhuma alteração automática "
        "da estratégia."
    ]

    return "\n".join(text)


# ============================================================
# RELATÓRIOS AUTOMÁTICOS
# ============================================================

def maybe_send_reports():

    now = time.time()

    # --------------------------------------------------------
    # BLOCO DE 100
    # --------------------------------------------------------

    cursor = int(
        float(
            get_meta(
                "last_block_cursor_id",
                0
            ) or 0
        )
    )

    rows = completed_rows_after(
        cursor,
        REPORT_BLOCK_SIZE
    )

    if len(rows) >= REPORT_BLOCK_SIZE:

        block = rows[
            :REPORT_BLOCK_SIZE
        ]

        title = (
            "V5.3 — BLOCO "
            f"{block[0]['id']}-"
            f"{block[-1]['id']}"
        )

        if send_telegram(
            build_report(
                title,
                block
            )
        ):

            set_meta(
                "last_block_cursor_id",
                block[-1]["id"]
            )

    # --------------------------------------------------------
    # SEMANAL
    # --------------------------------------------------------

    last_weekly = float(
        get_meta(
            "last_weekly_report_at",
            0
        ) or 0
    )

    if (
        now - last_weekly
        >= WEEKLY_SECONDS
    ):

        weekly_rows = completed_since(
            WEEKLY_SECONDS
        )

        if send_telegram(
            build_report(
                "V5.3 — RELATÓRIO SEMANAL",
                weekly_rows
            )
        ):

            set_meta(
                "last_weekly_report_at",
                now
            )


# ============================================================
# ALERTA TELEGRAM
# ============================================================

def format_alert(candidate, signal_id=None):

    side = candidate["side"]

    icon = (
        "🟢"
        if side == "LONG"
        else "🔴"
    )

    reasons = "\n".join(
        f"✓ {reason}"
        for reason
        in candidate["reasons"][:6]
    )

    if not reasons:
        reasons = "✓ Estrutura compatível"

    penalties = "\n".join(
        f"⚠️ {penalty}"
        for penalty
        in candidate["penalties"][:4]
    )

    message = [

        f"{icon} *{side} — {candidate['symbol']}*",
        "",
        (f"🚨 *SINAL #{signal_id:06d}*" if signal_id else "🚨 *SINAL — PENDENTE*"),
        f"*Estratégia:* {candidate.get('strategy','N/A')}",
        f"*Estágio:* {candidate.get('movement_stage','N/A')}",

        "",

        f"*Score:* "
        f"{candidate['score']:.0f}/100",

        f"*Qualidade:* "
        f"{candidate['quality']}",

        "",

        f"*Entrada:* "
        f"`{candidate['entry']:.8g}`",

        f"*Stop:* "
        f"`{candidate['stop']:.8g}`",

        f"*TP1:* "
        f"`{candidate['tp1']:.8g}`",

        f"*TP2:* "
        f"`{candidate['tp2']:.8g}`",

        "",

        f"*R:R TP1:* "
        f"{candidate['rr1']:.1f}",

        f"*R:R TP2:* "
        f"{candidate['rr2']:.1f}",

        f"*Regime:* "
        f"{candidate['regime']}",

        "",

        f"*Divergência RSI:* {candidate.get('rsi_divergence','NONE')} ({candidate.get('rsi_divergence_strength',0):.0f}/100)",
        "",
        "*Motivos:*",
        reasons
    ]

    if penalties:

        message += [
            "",
            "*Atenções:*",
            penalties
        ]

    message += [
        "",
        "⚠️ *Entrada ainda não executada*"
    ]

    return "\n".join(
        message
    )


# ============================================================
# ESCOLHA DO MELHOR SINAL
# ============================================================

def choose_best(
    candidates,
    now
):

    available = [

        candidate

        for candidate
        in candidates

        if not symbol_recently_alerted(
            candidate["symbol"]
        )
    ]

    if not available:
        return None

    available.sort(
        key=lambda candidate: (

            candidate["score"],

            candidate["quality"]
            == "STRONG",

            candidate["volume_ratio"],

            abs(
                candidate["r15"]
            )
        ),

        reverse=True
    )

    return available[0]


# ============================================================
# MONITOR PRINCIPAL
# ============================================================

def monitor_loop():

    global last_scan_at
    global last_successful_scan
    global scan_count
    global contracts

    last_contract_refresh = 0

    last_alert_global_local = 0

    while running:

        started = time.time()

        last_scan_at = started

        try:

            # ------------------------------------------------
            # CONTRATOS
            # ------------------------------------------------

            if (
                not contracts
                or
                started
                - last_contract_refresh
                >= CONTRACT_REFRESH
            ):

                refresh_contracts()

                last_contract_refresh = started

            # ------------------------------------------------
            # TICKERS
            # ------------------------------------------------

            tickers = get_all_tickers()

            if not tickers:

                time.sleep(
                    SCAN_INTERVAL
                )

                continue

            last_successful_scan = (
                time.time()
            )

            scan_count += 1

            by_symbol = {
                ticker["symbol"]:
                ticker

                for ticker
                in tickers
            }

            # ------------------------------------------------
            # BTC / ETH
            # ------------------------------------------------

            btc = by_symbol.get(
                "BTC_USDT"
            )

            if btc:

                btc_history.append(
                    (
                        btc["timestamp"],
                        btc["price"],
                        btc["oi"],
                        btc["amount24"]
                    )
                )

            eth = by_symbol.get(
                "ETH_USDT"
            )

            if eth:

                eth_history.append(
                    (
                        eth["timestamp"],
                        eth["price"],
                        eth["oi"],
                        eth["amount24"]
                    )
                )

            # ------------------------------------------------
            # RANKING POR LIQUIDEZ
            # ------------------------------------------------

            ranked = sorted(
                tickers,

                key=lambda ticker:
                    ticker["amount24"],

                reverse=True
            )

            selected = [

                ticker

                for ticker
                in ranked

                if ticker["symbol"]
                in contracts

            ][:MAX_CONTRACTS]

            # ------------------------------------------------
            # HISTÓRICO
            # ------------------------------------------------

            for ticker in selected:

                symbol = ticker[
                    "symbol"
                ]

                history = histories.setdefault(
                    symbol,
                    deque(
                        maxlen=MAX_HISTORY
                    )
                )

                history.append(
                    (
                        ticker["timestamp"],
                        ticker["price"],
                        ticker["oi"],
                        ticker["amount24"]
                    )
                )

            # ------------------------------------------------
            # LABORATÓRIO
            # ------------------------------------------------

            update_active_observations(
                tickers,
                time.time()
            )

            # ------------------------------------------------
            # CONTEXTO
            # ------------------------------------------------

            market = market_context()

            candidates = []

            # ------------------------------------------------
            # ANALISA MOEDAS
            # ------------------------------------------------

            for ticker in selected:

                symbol = ticker[
                    "symbol"
                ]

                if symbol in (
                    "BTC_USDT",
                    "ETH_USDT"
                ):
                    continue

                history = histories.get(
                    symbol
                )

                if not history:
                    continue

                candidate = analyze(
                    symbol,
                    history,
                    ticker,
                    market
                )

                if candidate:

                    candidates.append(
                        candidate
                    )

            # ------------------------------------------------
            # MELHOR OPORTUNIDADE
            # ------------------------------------------------

            best = choose_best(
                candidates,
                time.time()
            )

            # ------------------------------------------------
            # ALERTA
            # ------------------------------------------------

            if (
                best

                and

                time.time()
                - last_alert_global_local
                >= ALERT_INTERVAL
            ):

                detected_at=time.time()
                signal_id=register_signal(best, detected_at)
                if signal_id:
                    message=format_alert(best, signal_id)
                    sent=send_telegram(message)
                    if sent:
                        mark_alert_sent(signal_id)
                        last_alert_global_local=time.time()
                        print("[ALERT]", best["side"], best["symbol"], f"#{signal_id:06d}", f"score={best['score']:.0f}")
                    else:
                        discard_signal(signal_id)

            # ------------------------------------------------
            # RELATÓRIOS
            # ------------------------------------------------

            maybe_send_reports()

            # ------------------------------------------------
            # LOG
            # ------------------------------------------------

            if scan_count % 12 == 0:

                print(
                    f"[SCAN] {scan_count} | "
f"Monitoradas: {len(selected)}/{MAX_CONTRACTS} | "
f"Candidatos: {len(candidates)} | "
f"melhor={best['symbol'] if best else 'nenhuma'} | "
f"BTC15={market['btc15']:.2f}% | "
f"BTC60={market['btc60']:.2f}% | "
f"regime={market['regime']}"
                )

        except Exception as error:

            print(
                "[MONITOR ERROR]",
                type(error).__name__,
                ":",
                error
            )

        elapsed = (
            time.time()
            - started
        )

        time.sleep(
            max(
                0.5,
                SCAN_INTERVAL
                - elapsed
            )
        )


# ============================================================
# WATCHDOG
# ============================================================

def watchdog_loop():

    while running:

        time.sleep(30)

        if last_scan_at:

            age = (
                time.time()
                - last_scan_at
            )

            if age > 90:

                print(
                    "[WATCHDOG] "
                    f"último scan há "
                    f"{age:.0f}s"
                )


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return jsonify({

        "bot":
            "Pump Hunter",

        "version":
            VERSION,

        "status":
            "running"
            if running
            else "stopped",

        "scan_count":
            scan_count,

        "last_scan_at":
            last_scan_at,

        "last_successful_scan":
            last_successful_scan,

        "last_api_error":
            last_api_error,

        "active_observations":
            len(
                active_observations
            ),

        "contracts":
            len(contracts)
    })


@app.route("/status")
def status():

    return jsonify({

        "version":
            VERSION,

        "running":
            running,

        "scan_count":
            scan_count,

        "last_scan_at":
            last_scan_at,

        "last_successful_scan":
            last_successful_scan,

        "last_api_error":
            last_api_error,

        "active_observations":
            len(
                active_observations
            ),

        "contracts":
            len(contracts),

        "database":
            DB_PATH
    })


# ============================================================
# START
# ============================================================

def start():

    print("=" * 60)

    print(
        f"🚀 {VERSION} iniciado"
    )

    print("=" * 60)

    # Banco
    init_db()

    # Recupera sinais ainda em avaliação
    load_pending_observations()

    # Monitor
    threading.Thread(
        target=monitor_loop,
        daemon=True
    ).start()

    # Watchdog
    threading.Thread(
        target=watchdog_loop,
        daemon=True
    ).start()

    # Servidor web para o Render
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    start()
