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
# V5.4 — PUMP HUNTER / FUTURES RADAR
# ============================================================

VERSION = "V5.4"

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
# V5.4 — CONTEXTO MULTI-TIMEFRAME OFICIAL
# ============================================================

HTF_REFRESH_SECONDS = 300
HTF_SYMBOL_LIMIT = 30
HTF_INTERVALS = ("Min5", "Min15", "Min60", "Hour4")

htf_cache = {}
htf_symbols = []
htf_lock = threading.Lock()
last_htf_refresh = 0

# Evita inversões SHORT -> LONG -> SHORT por ruído curto.
OPPOSITE_FLIP_MINUTES = 30
OPPOSITE_FLIP_SCORE = 86


# ============================================================
# BANCO
# ============================================================

DB_PATH = os.getenv("ANALYTICS_DB", "pump_hunter_v533.db")


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

            alert_sent INTEGER DEFAULT 0,
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

            htf_score REAL,
            htf_bias TEXT,
            tf5m_bias TEXT,
            tf15m_bias TEXT,
            tf1h_bias TEXT,
            tf4h_bias TEXT,
            htf_rsi15 REAL,
            htf_rsi1h REAL,
            htf_volume15 REAL,
            htf_volume1h REAL,

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

            signal_number TEXT,
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

            "htf_score": "REAL",
            "htf_bias": "TEXT",
            "tf5m_bias": "TEXT",
            "tf15m_bias": "TEXT",
            "tf1h_bias": "TEXT",
            "tf4h_bias": "TEXT",
            "htf_rsi15": "REAL",
            "htf_rsi1h": "REAL",
            "htf_volume15": "REAL",
            "htf_volume1h": "REAL",

            "eth15": "REAL",
            "eth30": "REAL",
            "eth60": "REAL",

            "volume_ratio": "REAL",

            "htf_score": "REAL",
            "htf_bias": "TEXT",
            "tf5m_bias": "TEXT",
            "tf15m_bias": "TEXT",
            "tf1h_bias": "TEXT",
            "tf4h_bias": "TEXT",
            "htf_rsi15": "REAL",
            "htf_rsi1h": "REAL",
            "htf_volume15": "REAL",
            "htf_volume1h": "REAL",

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
            "outcome_elapsed": "REAL",

            "signal_number": "TEXT"
        }

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
# MEXC KLINES — CONTEXTO REAL DE TIMEFRAME
# ============================================================

def _kline_series(data):

    if not isinstance(data, dict):
        return []

    closes = data.get("close") or []
    opens = data.get("open") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    vols = data.get("vol") or []
    times = data.get("time") or []

    n = min(
        len(closes),
        len(times)
    )

    rows = []

    for i in range(n):
        try:
            rows.append({
                "time": float(times[i]),
                "open": float(opens[i]) if i < len(opens) else float(closes[i]),
                "high": float(highs[i]) if i < len(highs) else float(closes[i]),
                "low": float(lows[i]) if i < len(lows) else float(closes[i]),
                "close": float(closes[i]),
                "vol": float(vols[i]) if i < len(vols) else 0.0
            })
        except Exception:
            continue

    return rows


def fetch_kline(symbol, interval):

    data = mexc_get(
        f"/api/v1/contract/kline/{symbol}",
        {
            "interval": interval,
            "limit": 100
        }
    )

    if not data:
        return []

    return _kline_series(
        data.get("data", {})
    )


def analyze_timeframe(rows):

    if len(rows) < 25:
        return None

    closes = [x["close"] for x in rows]
    vols = [x["vol"] for x in rows]

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50) if len(closes) >= 50 else None
    r = rsi(closes, 14)

    if e9 is None or e21 is None or r is None:
        return None

    if e50 is not None:
        bull = closes[-1] > e21 and e9 > e21 and e21 > e50
        bear = closes[-1] < e21 and e9 < e21 and e21 < e50
    else:
        bull = closes[-1] > e21 and e9 > e21
        bear = closes[-1] < e21 and e9 < e21

    bias = "BULLISH" if bull else "BEARISH" if bear else "NEUTRAL"

    # Inclinação da EMA21: compara com uma janela anterior.
    ema21_prev = ema(closes[:-5], 21) if len(closes) >= 30 else e21
    ema_slope = pct_change(ema21_prev, e21) if ema21_prev else 0.0

    # Volume da última vela contra a média das 20 anteriores.
    vol_base = vols[-21:-1]
    avg_vol = sum(vol_base) / len(vol_base) if vol_base else 0.0
    volume_ratio = (vols[-1] / avg_vol) if avg_vol > 0 else 1.0

    # Retornos úteis para diferenciar tendência de ruído.
    r3 = pct_change(closes[-4], closes[-1]) if len(closes) >= 4 else 0.0
    r6 = pct_change(closes[-7], closes[-1]) if len(closes) >= 7 else 0.0

    return {
        "bias": bias,
        "close": closes[-1],
        "ema9": e9,
        "ema21": e21,
        "ema50": e50,
        "rsi": r,
        "ema_slope": ema_slope,
        "volume_ratio": volume_ratio,
        "r3": r3,
        "r6": r6
    }


def fetch_symbol_htf(symbol):

    result = {}

    for interval, key in (
        ("Min5", "5m"),
        ("Min15", "15m"),
        ("Min60", "1h"),
        ("Hour4", "4h")
    ):
        rows = fetch_kline(symbol, interval)
        result[key] = analyze_timeframe(rows)

    if not any(result.values()):
        return None

    return result


def refresh_htf_context(symbols):

    global last_htf_refresh

    refreshed = 0

    for symbol in symbols[:HTF_SYMBOL_LIMIT]:
        try:
            context = fetch_symbol_htf(symbol)
            if context:
                with htf_lock:
                    htf_cache[symbol] = {
                        "updated_at": time.time(),
                        "timeframes": context
                    }
                refreshed += 1
        except Exception as error:
            print(
                "[HTF ERROR]",
                symbol,
                type(error).__name__,
                error
            )

    last_htf_refresh = time.time()

    if refreshed:
        print(
            f"[HTF] Atualizados {refreshed}/{min(len(symbols), HTF_SYMBOL_LIMIT)}"
        )


def get_htf_context(symbol):

    with htf_lock:
        item = htf_cache.get(symbol)

    if not item:
        return None

    return item.get("timeframes")


def htf_summary(context):

    if not context:
        return {
            "score": 0,
            "bias": "NEUTRAL",
            "direction": None,
            "reversal_confirmed": False
        }

    weights = {
        "5m": 10,
        "15m": 25,
        "1h": 30,
        "4h": 20
    }

    bullish = 0
    bearish = 0

    for key, weight in weights.items():
        tf = context.get(key)
        if not tf:
            continue
        if tf["bias"] == "BULLISH":
            bullish += weight
        elif tf["bias"] == "BEARISH":
            bearish += weight

    total = bullish + bearish

    if bullish >= 55 and bullish > bearish + 15:
        bias = "STRONG_LONG"
        direction = "LONG"
    elif bullish >= 40 and bullish > bearish + 10:
        bias = "LONG"
        direction = "LONG"
    elif bearish >= 55 and bearish > bullish + 15:
        bias = "STRONG_SHORT"
        direction = "SHORT"
    elif bearish >= 40 and bearish > bullish + 10:
        bias = "SHORT"
        direction = "SHORT"
    else:
        bias = "NEUTRAL"
        direction = None

    score = max(bullish, bearish)

    # Reversão precisa de 15m + 1h concordando; 4h pode ainda estar atrasado.
    tf15 = context.get("15m")
    tf1h = context.get("1h")
    reversal_confirmed = bool(
        tf15 and tf1h and
        tf15["bias"] == tf1h["bias"] and
        tf15["rsi"] is not None and
        tf1h["rsi"] is not None and
        ((tf15["bias"] == "BULLISH" and tf15["rsi"] >= 52 and tf1h["rsi"] >= 50) or
         (tf15["bias"] == "BEARISH" and tf15["rsi"] <= 48 and tf1h["rsi"] <= 50))
    )

    return {
        "score": score,
        "bias": bias,
        "direction": direction,
        "reversal_confirmed": reversal_confirmed
    }


def htf_market_context():

    btc = get_htf_context("BTC_USDT")
    eth = get_htf_context("ETH_USDT")

    btc_sum = htf_summary(btc)
    eth_sum = htf_summary(eth)

    if btc_sum["direction"] == "LONG" and eth_sum["direction"] == "LONG":
        regime = "BULLISH"
    elif btc_sum["direction"] == "SHORT" and eth_sum["direction"] == "SHORT":
        regime = "BEARISH"
    elif btc_sum["direction"] or eth_sum["direction"]:
        regime = "EXPANSION"
    else:
        regime = "SIDEWAYS"

    return {
        "btc": btc,
        "eth": eth,
        "btc_summary": btc_sum,
        "eth_summary": eth_sum,
        "regime": regime
    }


def htf_refresh_loop():

    time.sleep(5)

    while running:
        try:
            with htf_lock:
                symbols = list(htf_symbols)

            if symbols:
                refresh_htf_context(symbols)

        except Exception as error:
            print(
                "[HTF LOOP ERROR]",
                type(error).__name__,
                error
            )

        time.sleep(HTF_REFRESH_SECONDS)


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

def structure_analysis(history):

    if len(history) < 120:
        return {
            "bull": False,
            "bear": False,
            "breakout": False,
            "breakdown": False,
            "false_breakout": False,
            "false_breakdown": False,
            "resistance": None,
            "support": None,
            "distance_resistance": 0,
            "distance_support": 0
        }

    prices = [x[1] for x in history]

    current = prices[-1]

    # aproximadamente últimos 5 minutos
    recent_5m = prices[-60:]

    # aproximadamente 1 minuto
    recent_1m = prices[-12:]

    # exclui os últimos candles/ticks para definir nível
    base = prices[-72:-12]

    if not base:
        return {
            "bull": False,
            "bear": False,
            "breakout": False,
            "breakdown": False,
            "false_breakout": False,
            "false_breakdown": False,
            "resistance": None,
            "support": None,
            "distance_resistance": 0,
            "distance_support": 0
        }

    resistance = max(base)

    support = min(base)

    old_current = prices[-12]

    breakout = (
        current > resistance
        and
        old_current <= resistance
    )

    breakdown = (
        current < support
        and
        old_current >= support
    )

    # falso rompimento:
    # houve pico acima do nível mas preço voltou
    max_recent = max(recent_1m)

    false_breakout = (
        max_recent > resistance
        and
        current < resistance * 0.997
    )

    false_breakdown = (
        min(recent_1m) < support
        and
        current > support * 1.003
    )

    distance_resistance = pct_change(
        resistance,
        current
    )

    distance_support = pct_change(
        support,
        current
    )

    bull = (
        current > ema(prices[-100:], 21)
        if len(prices) >= 100
        else False
    )

    bear = (
        current <
        ema(prices[-100:], 21)
        if len(prices) >= 100
        else False
    )

    return {
        "bull": bull,
        "bear": bear,

        "breakout": breakout or breakdown,
        "breakdown": breakdown,

        "false_breakout":
            false_breakout or false_breakdown,
        "false_breakdown": false_breakdown,

        "resistance": resistance,
        "support": support,

        "distance_resistance":
            distance_resistance,

        "distance_support":
            distance_support
    }


# ============================================================
# ANÁLISE PRINCIPAL
# ============================================================

def analyze(symbol, history, ticker, market):

    if len(history) < 180:
        return None

    prices = [x[1] for x in history]
    current = prices[-1]
    returns = get_returns(history)

    r5 = returns["r5"]
    r10 = returns["r10"]
    r15 = returns["r15"]
    r30 = returns["r30"]
    r60 = returns["r60"]

    accel15 = r15 - (r30 / 2)
    accel5 = r5 - (r10 - r5)

    rsi_current = rsi(prices[-120:], 14)
    rsi_previous = rsi(prices[-132:-12], 14)

    if rsi_current is None:
        return None

    if rsi_previous is None:
        rsi_previous = rsi_current

    rsi_delta = rsi_current - rsi_previous

    ema9 = ema(prices[-100:], 9)
    ema21 = ema(prices[-100:], 21)
    ema50 = ema(prices[-100:], 50)

    if None in (ema9, ema21, ema50):
        return None

    structure = structure_analysis(history)
    oi = oi_change(history, 60)
    vol_ratio = volume_ratio(history, 60)

    # --------------------------------------------------------
    # CONTEXTO MULTI-TIMEFRAME REAL
    # --------------------------------------------------------

    htf = get_htf_context(symbol)

    if not htf:
        return None

    hs = htf_summary(htf)
    htf_direction = hs["direction"]

    # Não permitimos que um gatilho de poucos segundos escolha
    # sozinho a direção. Primeiro precisa existir um viés de 15m/1h/4h.
    if htf_direction is None:
        return None

    # --------------------------------------------------------
    # GATILHO RÁPIDO — 20 PONTOS
    # --------------------------------------------------------

    trigger_score = 0

    if htf_direction == "LONG":
        if r5 > 0:
            trigger_score += 4
        if r15 > 0:
            trigger_score += 4
        if rsi_current >= 50:
            trigger_score += 3
        if rsi_delta > 0:
            trigger_score += 2
        if accel15 > 0:
            trigger_score += 2
        if structure["breakout"]:
            trigger_score += 3
        if current >= ema9:
            trigger_score += 2
    else:
        if r5 < 0:
            trigger_score += 4
        if r15 < 0:
            trigger_score += 4
        if rsi_current <= 50:
            trigger_score += 3
        if rsi_delta < 0:
            trigger_score += 2
        if accel15 < 0:
            trigger_score += 2
        if structure["breakout"]:
            trigger_score += 3
        if current <= ema9:
            trigger_score += 2

    trigger_score = min(trigger_score, 20)

    # --------------------------------------------------------
    # HTF — 30 PONTOS
    # --------------------------------------------------------

    htf_score = 0
    tf5 = htf.get("5m")
    tf15 = htf.get("15m")
    tf1h = htf.get("1h")
    tf4h = htf.get("4h")

    tf_weights = {"5m": 3, "15m": 9, "1h": 11, "4h": 7}

    for key, weight in tf_weights.items():
        tf = htf.get(key)
        if tf and ((htf_direction == "LONG" and tf["bias"] == "BULLISH") or
                   (htf_direction == "SHORT" and tf["bias"] == "BEARISH")):
            htf_score += weight

    # --------------------------------------------------------
    # ESTRUTURA — 15
    # --------------------------------------------------------

    structure_score = 0

    if htf_direction == "LONG":
        if current > ema21:
            structure_score += 4
        if current > ema50:
            structure_score += 3
        if r30 > 0:
            structure_score += 3
        if r60 > 0:
            structure_score += 2
        if structure["breakout"]:
            structure_score += 3
    else:
        if current < ema21:
            structure_score += 4
        if current < ema50:
            structure_score += 3
        if r30 < 0:
            structure_score += 3
        if r60 < 0:
            structure_score += 2
        if structure["breakout"]:
            structure_score += 3

    structure_score = min(structure_score, 15)

    # --------------------------------------------------------
    # VOLUME — 15
    # --------------------------------------------------------

    volume_score = 0

    tf5_vol = tf5["volume_ratio"] if tf5 else 1.0
    tf15_vol = tf15["volume_ratio"] if tf15 else 1.0
    tf1h_vol = tf1h["volume_ratio"] if tf1h else 1.0

    if vol_ratio >= 1.20:
        volume_score += 3
    if vol_ratio >= 1.50:
        volume_score += 2
    if tf5_vol >= 1.20:
        volume_score += 1
    if tf15_vol >= 1.20:
        volume_score += 2
    if tf1h_vol >= 1.10:
        volume_score += 2

    volume_score = min(volume_score, 10)

    # --------------------------------------------------------
    # RSI / MOMENTUM — 10
    # --------------------------------------------------------

    momentum_score = 0

    if htf_direction == "LONG":
        if rsi_current >= 50:
            momentum_score += 2
        if rsi_delta > 0:
            momentum_score += 2
        if tf15 and tf15["rsi"] >= 50:
            momentum_score += 2
        if tf1h and tf1h["rsi"] >= 50:
            momentum_score += 2
        if accel15 > 0:
            momentum_score += 2
    else:
        if rsi_current <= 50:
            momentum_score += 2
        if rsi_delta < 0:
            momentum_score += 2
        if tf15 and tf15["rsi"] <= 50:
            momentum_score += 2
        if tf1h and tf1h["rsi"] <= 50:
            momentum_score += 2
        if accel15 < 0:
            momentum_score += 2

    momentum_score = min(momentum_score, 10)

    # --------------------------------------------------------
    # OI — 5
    # --------------------------------------------------------

    oi_score = 0
    if htf_direction == "LONG":
        if oi > 0.05:
            oi_score += 2
        if oi > 0.20:
            oi_score += 3
    else:
        if oi < -0.05:
            oi_score += 2
        if oi < -0.20:
            oi_score += 3
    oi_score = min(oi_score, 5)

    # --------------------------------------------------------
    # MERCADO — 5
    # --------------------------------------------------------

    market_score = 0
    market_dir = market["regime"]

    if htf_direction == "LONG":
        if market_dir in ("BULLISH", "EXPANSION"):
            market_score += 3
        if market.get("btc_summary", {}).get("direction") == "LONG":
            market_score += 1
        if market.get("eth_summary", {}).get("direction") == "LONG":
            market_score += 1
    else:
        if market_dir in ("BEARISH", "EXPANSION"):
            market_score += 3
        if market.get("btc_summary", {}).get("direction") == "SHORT":
            market_score += 1
        if market.get("eth_summary", {}).get("direction") == "SHORT":
            market_score += 1

    market_score = min(market_score, 5)

    # --------------------------------------------------------
    # ENTRADA — 5
    # --------------------------------------------------------

    entry_score = 0

    if structure["breakout"]:
        entry_score += 3

    if htf_direction == "LONG" and current >= ema9:
        entry_score += 2
    elif htf_direction == "SHORT" and current <= ema9:
        entry_score += 2

    entry_score = min(entry_score, 5)

    raw_score = (
        htf_score
        + trigger_score
        + structure_score
        + volume_score
        + momentum_score
        + oi_score
        + market_score
        + entry_score
    )

    score = raw_score
    penalties = []

    # --------------------------------------------------------
    # PULLBACK VS REVERSÃO
    # --------------------------------------------------------

    short_term_against = (
        htf_direction == "LONG" and r5 < 0 and r15 < 0
    ) or (
        htf_direction == "SHORT" and r5 > 0 and r15 > 0
    )

    if short_term_against:
        score -= 12
        penalties.append("pullback contra o gatilho")

    if structure["false_breakout"]:
        score -= 10
        penalties.append("falso rompimento")

    if htf_direction == "LONG" and r30 < -0.8:
        score -= 8
        penalties.append("30m contra LONG")

    if htf_direction == "SHORT" and r30 > 0.8:
        score -= 8
        penalties.append("30m contra SHORT")

    if htf_direction == "LONG" and rsi_current > 78:
        score -= 8
        penalties.append("RSI esticado LONG")

    if htf_direction == "SHORT" and rsi_current < 22:
        score -= 8
        penalties.append("RSI esticado SHORT")

    # Reversão contra o último sinal só é aceita com confirmação real.
    if htf_direction == "LONG" and hs["reversal_confirmed"]:
        pass
    elif htf_direction == "SHORT" and hs["reversal_confirmed"]:
        pass

    score = max(0, min(100, score))

    if score < MIN_SCORE:
        return None

    # Trigger precisa estar minimamente alinhado; o HTF não deve gerar
    # alerta apenas porque as médias longas estão alinhadas.
    if trigger_score < 8:
        return None

    side = htf_direction

    # Não perseguir preço já esticado no micro movimento.
    if side == "LONG" and r5 > 1.2:
        return None
    if side == "SHORT" and r5 < -1.2:
        return None

    resistance = structure["resistance"]
    support = structure["support"]

    if side == "LONG":
        entry = current
        structural_stop = support * 0.998 if support else current * 0.985
        risk = entry - structural_stop
        if risk <= 0:
            return None
        tp1 = entry + risk * 1.8
        tp2 = entry + risk * 2.7
    else:
        entry = current
        structural_stop = resistance * 1.002 if resistance else current * 1.015
        risk = structural_stop - entry
        if risk <= 0:
            return None
        tp1 = entry - risk * 1.8
        tp2 = entry - risk * 2.7

    rr1 = 1.8
    rr2 = 2.7

    if rr1 < MIN_RR:
        return None

    if score >= STRONG_SCORE:
        quality = "STRONG"
    elif score >= 82:
        quality = "GOOD"
    else:
        quality = "OPPORTUNITY"

    reasons = []

    if hs["bias"]:
        reasons.append(f"HTF {hs['bias']}")
    if tf15:
        reasons.append(f"15m {tf15['bias']}")
    if tf1h:
        reasons.append(f"1h {tf1h['bias']}")
    if tf4h:
        reasons.append(f"4h {tf4h['bias']}")
    if tf15 and tf15["rsi"] is not None:
        reasons.append(f"RSI15 {tf15['rsi']:.0f}")
    if tf15_vol >= 1.2:
        reasons.append(f"Vol15 x{tf15_vol:.1f}")
    if vol_ratio >= 1.2:
        reasons.append(f"Trigger vol x{vol_ratio:.1f}")
    if structure["breakout"]:
        reasons.append("Rompimento")
    if rsi_delta > 0 and side == "LONG":
        reasons.append("RSI subindo")
    if rsi_delta < 0 and side == "SHORT":
        reasons.append("RSI caindo")

    return {
        "symbol": symbol,
        "side": side,
        "score": round(score, 1),
        "raw_score": round(raw_score, 1),
        "structure_score": structure_score,
        "trend_score": htf_score,
        "volume_score": volume_score,
        "momentum_score": momentum_score,
        "oi_score": oi_score,
        "volatility_score": trigger_score,
        "market_score": market_score,
        "entry_score": entry_score,
        "htf_score": htf_score,
        "htf_bias": hs["bias"],
        "tf5m_bias": tf5["bias"] if tf5 else "NEUTRAL",
        "tf15m_bias": tf15["bias"] if tf15 else "NEUTRAL",
        "tf1h_bias": tf1h["bias"] if tf1h else "NEUTRAL",
        "tf4h_bias": tf4h["bias"] if tf4h else "NEUTRAL",
        "htf_rsi15": tf15["rsi"] if tf15 else 0.0,
        "htf_rsi1h": tf1h["rsi"] if tf1h else 0.0,
        "htf_volume15": tf15_vol,
        "htf_volume1h": tf1h_vol,
        "entry": entry,
        "stop": structural_stop,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": rr1,
        "rr2": rr2,
        "quality": quality,
        "r5": r5,
        "r10": r10,
        "r15": r15,
        "r30": r30,
        "r60": r60,
        "accel15": accel15,
        "accel5": accel5,
        "rsi": rsi_current,
        "rsi_delta": rsi_delta,
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
        "oi_change": oi,
        "btc15": market.get("btc_summary", {}).get("score", 0),
        "btc30": 0.0,
        "btc60": market.get("btc_summary", {}).get("score", 0),
        "eth15": market.get("eth_summary", {}).get("score", 0),
        "eth30": 0.0,
        "eth60": market.get("eth_summary", {}).get("score", 0),
        "volume_ratio": vol_ratio,
        "breakout": structure["breakout"],
        "false_breakout": structure["false_breakout"],
        "regime": market["regime"],
        "reasons": reasons,
        "penalties": penalties,
        "reversal_confirmed": hs["reversal_confirmed"]
    }



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

    symbol = candidate["symbol"]

    if symbol_recently_alerted(symbol):
        return None

    with db_lock:
        conn = db_connect()

        cur = conn.execute("""
        INSERT INTO signals (
            detected_at,
            detected_at_str,
            symbol,
            side,
            alert_sent,
            alert_sent_at,

            entry_price,
            stop_price,
            tp1_price,
            tp2_price,

            score,
            raw_score,

            structure_score,
            trend_score,
            volume_score,
            momentum_score,
            oi_score,
            volatility_score,
            market_score,
            entry_score,

            htf_score,
            htf_bias,
            tf5m_bias,
            tf15m_bias,
            tf1h_bias,
            tf4h_bias,
            htf_rsi15,
            htf_rsi1h,
            htf_volume15,
            htf_volume1h,

            r5,
            r10,
            r15,
            r30,
            r60,

            accel_15,
            accel_5,

            rsi,
            rsi_delta,

            ema9,
            ema21,
            ema50,

            oi_change,

            btc15,
            btc30,
            btc60,

            eth15,
            eth30,
            eth60,

            volume_ratio,

            breakout,
            false_breakout,

            setup_quality,
            reasons
        )
        VALUES (
            ?,?,?,?,?,?,
            ?,?,?,?,
            ?,?,
            ?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,
            ?,?,
            ?,?,?,
            ?,
            ?,?,?,
            ?,?,?,
            ?,
            ?,?,
            ?,?
        )
        """, (
            detected_at,
            datetime.fromtimestamp(
                detected_at,
                timezone.utc
            ).isoformat(),

            symbol,
            candidate["side"],

            0,
            None,

            candidate["entry"],
            candidate["stop"],
            candidate["tp1"],
            candidate["tp2"],

            candidate["score"],
            candidate["raw_score"],

            candidate["structure_score"],
            candidate["trend_score"],
            candidate["volume_score"],
            candidate["momentum_score"],
            candidate["oi_score"],
            candidate["volatility_score"],
            candidate["market_score"],
            candidate["entry_score"],

            candidate["htf_score"],
            candidate["htf_bias"],
            candidate["tf5m_bias"],
            candidate["tf15m_bias"],
            candidate["tf1h_bias"],
            candidate["tf4h_bias"],
            candidate["htf_rsi15"],
            candidate["htf_rsi1h"],
            candidate["htf_volume15"],
            candidate["htf_volume1h"],

            candidate["r5"],
            candidate["r10"],
            candidate["r15"],
            candidate["r30"],
            candidate["r60"],

            candidate["accel15"],
            candidate["accel5"],

            candidate["rsi"],
            candidate["rsi_delta"],

            candidate["ema9"],
            candidate["ema21"],
            candidate["ema50"],

            candidate["oi_change"],

            candidate["btc15"],
            candidate["btc30"],
            candidate["btc60"],

            candidate["eth15"],
            candidate["eth30"],
            candidate["eth60"],

            candidate["volume_ratio"],

            int(candidate["breakout"]),
            int(candidate["false_breakout"]),

            candidate["quality"],

            json.dumps({
                "reasons": candidate["reasons"],
                "penalties": candidate["penalties"],
                "htf_bias": candidate.get("htf_bias"),
                "tf5m_bias": candidate.get("tf5m_bias"),
                "tf15m_bias": candidate.get("tf15m_bias"),
                "tf1h_bias": candidate.get("tf1h_bias"),
                "tf4h_bias": candidate.get("tf4h_bias"),
                "htf_score": candidate.get("htf_score")
            }, ensure_ascii=False)
        ))

        signal_id = cur.lastrowid

        signal_number = f"SINAL #{signal_id:06d}"

        conn.execute(
            "UPDATE signals SET signal_number=? WHERE id=?",
            (signal_number, signal_id)
        )

        conn.commit()
        conn.close()

    last_alert_symbol[symbol] = time.time()

    active_observations[signal_id] = {
        "id": signal_id,
        "symbol": symbol,
        "side": candidate["side"],
        "entry": candidate["entry"],
        "detected_at": detected_at,

        "max_fav": 0.0,
        "max_adv": 0.0,

        "mfe_at": detected_at,
        "mfe_elapsed": 0.0,

        "mae_at": detected_at,
        "mae_elapsed": 0.0,

        "outcome": None,
        "outcome_at": None,
        "outcome_elapsed": None
    }

    return signal_id


def mark_alert_sent(signal_id):

    with db_lock:
        conn = db_connect()
        conn.execute(
            "UPDATE signals SET alert_sent=1, alert_sent_at=? WHERE id=?",
            (time.time(), signal_id)
        )
        conn.commit()
        conn.close()


def discard_signal(signal_id):

    active_observations.pop(signal_id, None)

    with db_lock:
        conn = db_connect()
        conn.execute(
            "DELETE FROM signals WHERE id=?",
            (signal_id,)
        )
        conn.commit()
        conn.close()


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
# LABORATÓRIO — ACOMPANHAMENTO 24H
# ============================================================

def update_active_observations(tickers, now):

    prices = {
        x["symbol"]: x["price"]
        for x in tickers
    }

    finished = []

    for signal_id, obs in list(
        active_observations.items()
    ):

        price = prices.get(
            obs["symbol"]
        )

        if price is None:
            continue

        elapsed = (
            now - obs["detected_at"]
        )

        ret = side_return(
            obs["side"],
            obs["entry"],
            price
        )

        # ----------------------------------------------------
        # MFE
        # ----------------------------------------------------

        if ret > obs["max_fav"]:

            obs["max_fav"] = ret

            obs["mfe_at"] = now

            obs["mfe_elapsed"] = elapsed

        # ----------------------------------------------------
        # MAE
        # ----------------------------------------------------

        if ret < obs["max_adv"]:

            obs["max_adv"] = ret

            obs["mae_at"] = now

            obs["mae_elapsed"] = elapsed

        # ----------------------------------------------------
        # RESULTADO
        # ----------------------------------------------------

        if obs["outcome"] is None:

            if ret >= GAIN_TARGET:

                obs["outcome"] = "GAIN"

                obs["outcome_at"] = now

                obs["outcome_elapsed"] = elapsed

            elif ret <= LOSS_TARGET:

                obs["outcome"] = "LOSS"

                obs["outcome_at"] = now

                obs["outcome_elapsed"] = elapsed

        # ----------------------------------------------------
        # HORIZONTES
        # ----------------------------------------------------

        fields = {}

        if elapsed >= 30:
            fields["ret_30"] = ret

        if elapsed >= 60:
            fields["ret_60"] = ret

        if elapsed >= 180:
            fields["ret_180"] = ret

        if elapsed >= 300:
            fields["ret_300"] = ret

        if elapsed >= 600:
            fields["ret_600"] = ret

        if elapsed >= 900:
            fields["ret_900"] = ret

        # ----------------------------------------------------
        # FINAL DA JANELA DE 24 HORAS
        # ----------------------------------------------------

        if elapsed >= LAB_WINDOW:

            if obs["outcome"] is None:

                obs["outcome"] = "EXPIRED"

                obs["outcome_at"] = now

                obs["outcome_elapsed"] = LAB_WINDOW

            fields.update({

                "mfe_900":
                    obs["max_fav"],

                "mae_900":
                    obs["max_adv"],

                "max_fav":
                    obs["max_fav"],

                "max_adv":
                    obs["max_adv"],

                "mfe_at":
                    obs["mfe_at"],

                "mfe_elapsed":
                    obs["mfe_elapsed"],

                "mae_at":
                    obs["mae_at"],

                "mae_elapsed":
                    obs["mae_elapsed"],

                "outcome":
                    obs["outcome"],

                "outcome_at":
                    obs["outcome_at"],

                "outcome_elapsed":
                    obs["outcome_elapsed"],

                "complete":
                    1,

                "completed_at":
                    now
            })

            finished.append(
                signal_id
            )

        # ----------------------------------------------------
        # SALVA NO BANCO
        # ----------------------------------------------------

        if fields:

            assignments = ", ".join(
                f"{key}=?"
                for key in fields
            )

            with db_lock:

                conn = db_connect()

                conn.execute(
                    f"""
                    UPDATE signals
                    SET {assignments}
                    WHERE id=?
                    """,
                    (
                        *fields.values(),
                        signal_id
                    )
                )

                conn.commit()
                conn.close()

    # --------------------------------------------------------
    # REMOVE FINALIZADOS DA MEMÓRIA
    # --------------------------------------------------------

    for signal_id in finished:

        active_observations.pop(
            signal_id,
            None
        )


# ============================================================
# RECUPERA OBSERVAÇÕES APÓS RESTART
# ============================================================

def load_pending_observations():

    active_observations.clear()

    cutoff = (
        time.time()
        - LAB_WINDOW
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
            outcome_elapsed

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

            "outcome_elapsed":
                row["outcome_elapsed"]
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

        "mfe":
            average(
                "mfe_900",
                rows
            ),

        "mae":
            average(
                "mae_900",
                rows
            ),

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

        "loss_time":
            average(
                "outcome_elapsed",
                losses
            )
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
            lambda r:
                (r["volume_ratio"] or 0)
                >= 1.5
        ),

        (
            "HTF alinhado",
            lambda r:
                (r["htf_score"] or 0)
                >= 24
        ),

        (
            "15m + 1h alinhados",
            lambda r:
                r["tf15m_bias"] is not None
                and r["tf15m_bias"] != "NEUTRAL"
                and r["tf15m_bias"] == r["tf1h_bias"]
        )
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
            f"E {metrics['expired_pct']:.0f}% | "
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

        f"⌛ EXPIRED: "
        f"*{metrics['expired']} "
        f"({metrics['expired_pct']:.1f}%)*",

        "",

        f"MFE médio 15m: "
        f"*{metrics['mfe']:.2f}%*",

        f"MAE médio 15m: "
        f"*{metrics['mae']:.2f}%*",

        f"Retorno médio 5m: "
        f"*{metrics['ret5']:.2f}%*",

        f"Retorno médio 15m: "
        f"*{metrics['ret15']:.2f}%*",

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
            "V5.4 — BLOCO "
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
                "V5.4 — RELATÓRIO SEMANAL",
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

def format_alert(candidate):

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

        f"🚨 *SINAL #{candidate.get('signal_id', 'PENDENTE'):06d}*" if isinstance(candidate.get('signal_id'), int) else "🚨 *SINAL PENDENTE*",
        "",
        f"{icon} *{side} — "
        f"{candidate['symbol']}*",

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

        f"*HTF:* "
        f"{candidate.get('htf_bias', 'NEUTRAL')} | "
        f"5m {candidate.get('tf5m_bias', 'NEUTRAL')} | "
        f"15m {candidate.get('tf15m_bias', 'NEUTRAL')} | "
        f"1h {candidate.get('tf1h_bias', 'NEUTRAL')} | "
        f"4h {candidate.get('tf4h_bias', 'NEUTRAL')}",

        f"*RSI:* "
        f"rápido {candidate.get('rsi', 0):.0f} | "
        f"15m {candidate.get('htf_rsi15', 0):.0f} | "
        f"1h {candidate.get('htf_rsi1h', 0):.0f}",

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

def opposite_recent_signal(symbol, side):

    cutoff = time.time() - (OPPOSITE_FLIP_MINUTES * 60)

    with db_lock:
        conn = db_connect()
        row = conn.execute("""
        SELECT side, detected_at
        FROM signals
        WHERE symbol=?
          AND alert_sent=1
          AND detected_at>=?
        ORDER BY id DESC
        LIMIT 1
        """, (symbol, cutoff)).fetchone()
        conn.close()

    if not row:
        return False, None

    return row["side"] != side, row["side"]


def choose_best(candidates, now):

    available = []

    for candidate in candidates:
        symbol = candidate["symbol"]

        if symbol_recently_alerted(symbol):
            continue

        opposite, previous_side = opposite_recent_signal(
            symbol,
            candidate["side"]
        )

        if opposite:
            # Flip só passa com confirmação dos timeframes maiores e score forte.
            if not candidate.get("reversal_confirmed"):
                continue
            if candidate.get("score", 0) < OPPOSITE_FLIP_SCORE:
                continue
            if candidate.get("htf_score", 0) < 24:
                continue

        available.append(candidate)

    if not available:
        return None

    available.sort(
        key=lambda candidate: (
            candidate["score"],
            candidate.get("htf_score", 0),
            candidate["quality"] == "STRONG",
            candidate.get("htf_volume15", 0),
            candidate["volume_ratio"],
            abs(candidate["r15"])
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

            # Atualiza a lista usada pelo thread de contexto HTF.
            global htf_symbols
            with htf_lock:
                htf_symbols = [
                    t["symbol"]
                    for t in selected[:HTF_SYMBOL_LIMIT]
                ]

            market = htf_market_context()

            # Sem BTC/ETH HTF ainda, não liberamos sinais novos.
            if market["btc"] is None and market["eth"] is None:
                market = {
                    **market,
                    "regime": "SIDEWAYS"
                }

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

                signal_id = register_signal(
                    best,
                    time.time()
                )

                if signal_id:

                    best["signal_id"] = signal_id

                    message = format_alert(
                        best
                    )

                    sent = send_telegram(
                        message
                    )

                    if sent:

                        mark_alert_sent(signal_id)

                        last_alert_global_local = time.time()

                        print(
                            "[ALERT]",
                            f"SINAL #{signal_id:06d}",
                            best["side"],
                            best["symbol"],
                            f"score={best['score']:.0f}"
                        )

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

        "htf_cached_symbols": len(htf_cache),
        "htf_last_refresh": last_htf_refresh,

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

        "htf_cached_symbols": len(htf_cache),
        "htf_last_refresh": last_htf_refresh,

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

    # Contexto multi-timeframe oficial MEXC
    threading.Thread(
        target=htf_refresh_loop,
        daemon=True
    ).start()

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
