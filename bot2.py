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

VERSION = "V5.2"

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
LAB_WINDOW = 15 * 60

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

DB_PATH = os.getenv("ANALYTICS_DB", "pump_hunter_v52.db")


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

def structure_analysis(history):

    if len(history) < 120:
        return {
            "bull": False,
            "bear": False,
            "breakout": False,
            "false_breakout": False,
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
            "false_breakout": False,
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

        "false_breakout":
            false_breakout or false_breakdown,

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

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    accel15 = r15 - (r30 / 2)

    accel5 = r5 - (r10 - r5)

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    rsi_current = rsi(prices[-120:], 14)

    rsi_previous = rsi(
        prices[-132:-12],
        14
    )

    if rsi_current is None:
        return None

    if rsi_previous is None:
        rsi_previous = rsi_current

    rsi_delta = rsi_current - rsi_previous

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    ema9 = ema(prices[-100:], 9)
    ema21 = ema(prices[-100:], 21)
    ema50 = ema(prices[-100:], 50)

    if None in (ema9, ema21, ema50):
        return None

    # --------------------------------------------------------
    # ESTRUTURA
    # --------------------------------------------------------

    structure = structure_analysis(history)

    # --------------------------------------------------------
    # VOLUME / OI
    # --------------------------------------------------------

    oi = oi_change(history, 60)

    vol_ratio = volume_ratio(history, 60)

    # --------------------------------------------------------
    # MERCADO
    # --------------------------------------------------------

    btc15 = market["btc15"]
    btc30 = market["btc30"]
    btc60 = market["btc60"]

    eth15 = market["eth15"]
    eth30 = market["eth30"]
    eth60 = market["eth60"]

    # --------------------------------------------------------
    # TENDÊNCIA
    # --------------------------------------------------------

    trend_score = 0

    if current > ema9:
        trend_score += 4

    if ema9 > ema21:
        trend_score += 5

    if ema21 > ema50:
        trend_score += 6

    # --------------------------------------------------------
    # ESTRUTURA — 20
    # --------------------------------------------------------

    structure_score = 0

    if structure["bull"]:
        structure_score += 5

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

    structure_score = min(
        structure_score,
        20
    )

    # --------------------------------------------------------
    # VOLUME — 15
    # --------------------------------------------------------

    volume_score = 0

    if vol_ratio >= 1.20:
        volume_score += 5

    if vol_ratio >= 1.50:
        volume_score += 5

    if vol_ratio >= 2.00:
        volume_score += 5

    volume_score = min(
        volume_score,
        15
    )

    # --------------------------------------------------------
    # MOMENTUM — 10
    # --------------------------------------------------------

    momentum_score = 0

    if r15 > 0.30:
        momentum_score += 3

    if r30 > 0.60:
        momentum_score += 2

    if accel15 > 0.20:
        momentum_score += 3

    if accel5 > 0.08:
        momentum_score += 2

    momentum_score = min(
        momentum_score,
        10
    )

    # --------------------------------------------------------
    # OI — 15
    # --------------------------------------------------------

    oi_score = 0

    if oi > 0.20:
        oi_score += 5

    if oi > 0.50:
        oi_score += 5

    if oi > 1.00:
        oi_score += 5

    # --------------------------------------------------------
    # VOLATILIDADE — 10
    # --------------------------------------------------------

    volatility_score = 0

    recent_returns = []

    for i in range(
        max(1, len(prices) - 60),
        len(prices)
    ):
        if prices[i - 1] != 0:
            recent_returns.append(
                pct_change(
                    prices[i - 1],
                    prices[i]
                )
            )

    vol = rolling_std(recent_returns)

    if vol > 0.03:
        volatility_score += 3

    if vol > 0.06:
        volatility_score += 3

    if 0.01 < abs(r15) < 2.5:
        volatility_score += 4

    volatility_score = min(
        volatility_score,
        10
    )

    # --------------------------------------------------------
    # MERCADO — 10
    # --------------------------------------------------------

    market_score = 0

    if btc15 >= 0:
        market_score += 2

    if btc60 >= 0:
        market_score += 2

    if eth15 >= 0:
        market_score += 2

    if eth60 >= 0:
        market_score += 2

    if market["regime"] in (
        "BULLISH",
        "EXPANSION"
    ):
        market_score += 2

    market_score = min(
        market_score,
        10
    )

    # --------------------------------------------------------
    # ENTRADA — 5
    # --------------------------------------------------------

    entry_score = 0

    if structure["breakout"]:
        entry_score += 3

    if abs(
        structure["distance_resistance"]
    ) < 0.8:
        entry_score += 2

    entry_score = min(
        entry_score,
        5
    )

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    raw_score = (
        structure_score
        + min(trend_score, 15)
        + volume_score
        + momentum_score
        + min(oi_score, 15)
        + volatility_score
        + market_score
        + entry_score
    )

    score = raw_score

    penalties = []

    # BTC contra
    if btc15 < -0.60:
        score -= 8
        penalties.append("BTC contra")

    if btc60 < -1.50:
        score -= 7
        penalties.append("BTC forte queda")

    # ETH contra
    if eth15 < -0.60:
        score -= 4
        penalties.append("ETH contra")

    # RSI excessivo
    if rsi_current > 78:
        score -= 10
        penalties.append("RSI esticado")

    # OI negativo
    if oi < -0.50:
        score -= 6
        penalties.append("OI negativo")

    # contra tendência
    if current < ema21:
        score -= 8
        penalties.append("abaixo EMA21")

    if ema21 < ema50:
        score -= 6
        penalties.append("EMA21 < EMA50")

    # falso rompimento
    if structure["false_breakout"]:
        score -= 15
        penalties.append("falso rompimento")

    # movimento já muito esticado
    if r30 > 2.50:
        score -= 8
        penalties.append("entrada esticada")

    # --------------------------------------------------------
    # DIREÇÃO
    # --------------------------------------------------------

    bullish = (
        current > ema21
        and r15 > 0
        and r30 > 0
    )

    bearish = (
        current < ema21
        and r15 < 0
        and r30 < 0
    )

    if not bullish and not bearish:
        return None

    side = "LONG" if bullish else "SHORT"

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    if side == "SHORT":

        # Para esta versão o score de SHORT
        # é uma inversão controlada do contexto.
        #
        # Mantemos a mesma estrutura para que
        # o laboratório possa comparar LONG/SHORT.

        score = raw_score

        if btc15 > 0.60:
            score -= 8
            penalties.append("BTC contra SHORT")

        if btc60 > 1.50:
            score -= 7
            penalties.append("BTC forte alta")

        if eth15 > 0.60:
            score -= 4
            penalties.append("ETH contra SHORT")

    score = max(
        0,
        min(100, score)
    )

    # --------------------------------------------------------
    # VALIDAÇÃO DO SETUP
    # --------------------------------------------------------

    if score < MIN_SCORE:
        return None

    if side == "LONG":

        if r5 <= 0:
            return None

        if r15 <= 0:
            return None

        if rsi_current < 50:
            return None

    else:

        if r5 >= 0:
            return None

        if r15 >= 0:
            return None

        if rsi_current > 50:
            return None

    # --------------------------------------------------------
    # ENTRADA / STOP / TP
    # --------------------------------------------------------

    resistance = structure["resistance"]
    support = structure["support"]

    if side == "LONG":

        entry = current

        structural_stop = (
            support * 0.998
            if support
            else current * 0.985
        )

        risk = entry - structural_stop

        if risk <= 0:
            return None

        tp1 = entry + risk * 1.8
        tp2 = entry + risk * 2.7

    else:

        entry = current

        structural_stop = (
            resistance * 1.002
            if resistance
            else current * 1.015
        )

        risk = structural_stop - entry

        if risk <= 0:
            return None

        tp1 = entry - risk * 1.8
        tp2 = entry - risk * 2.7

    rr1 = 1.8
    rr2 = 2.7

    if rr1 < MIN_RR:
        return None

    # --------------------------------------------------------
    # QUALIDADE
    # --------------------------------------------------------

    if score >= STRONG_SCORE:
        quality = "STRONG"

    elif score >= 82:
        quality = "GOOD"

    else:
        quality = "OPPORTUNITY"

    # --------------------------------------------------------
    # MOTIVOS
    # --------------------------------------------------------

    reasons = []

    if structure["breakout"]:
        reasons.append("Rompimento")

    if vol_ratio >= 1.20:
        reasons.append(
            f"Volume x{vol_ratio:.1f}"
        )

    if oi > 0.20:
        reasons.append(
            f"OI +{oi:.2f}%"
        )

    if ema9 > ema21:
        reasons.append("EMA9 > EMA21")

    if ema21 > ema50:
        reasons.append("EMA21 > EMA50")

    if accel15 > 0.20:
        reasons.append("Aceleração")

    if btc15 >= 0:
        reasons.append("BTC alinhado")

    if eth15 >= 0:
        reasons.append("ETH alinhado")

    return {
        "symbol": symbol,
        "side": side,

        "score": round(score, 1),
        "raw_score": round(raw_score, 1),

        "structure_score": structure_score,
        "trend_score": min(trend_score, 15),
        "volume_score": volume_score,
        "momentum_score": momentum_score,
        "oi_score": min(oi_score, 15),
        "volatility_score": volatility_score,
        "market_score": market_score,
        "entry_score": entry_score,

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

        "btc15": btc15,
        "btc30": btc30,
        "btc60": btc60,

        "eth15": eth15,
        "eth30": eth30,
        "eth60": eth60,

        "volume_ratio": vol_ratio,

        "breakout": structure["breakout"],
        "false_breakout": structure["false_breakout"],

        "regime": market["regime"],

        "reasons": reasons,
        "penalties": penalties
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

            1,
            time.time(),

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
                "penalties": candidate["penalties"]
            }, ensure_ascii=False)
        ))

        signal_id = cur.lastrowid

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
        # FINAL DOS 15 MINUTOS
        # ----------------------------------------------------

        if elapsed >= LAB_WINDOW:

            if obs["outcome"] is None:

                obs["outcome"] = "NEUTRAL"

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
            'NEUTRAL'
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
            'NEUTRAL'
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

    neutral = [
        r for r in rows
        if r["outcome"] == "NEUTRAL"
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

        "neutral":
            len(neutral),

        "gain_pct":
            len(gains)
            / total
            * 100,

        "loss_pct":
            len(losses)
            / total
            * 100,

        "neutral_pct":
            len(neutral)
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

        f"🟡 NEUTRAL: "
        f"*{metrics['neutral']} "
        f"({metrics['neutral_pct']:.1f}%)*",

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
            "V5.2 — BLOCO "
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
                "V5.2 — RELATÓRIO SEMANAL",
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

                message = format_alert(
                    best
                )

                sent = send_telegram(
                    message
                )

                if sent:

                    signal_id = register_signal(
                        best,
                        time.time()
                    )

                    if signal_id:

                        last_alert_global_local = (
                            time.time()
                        )

                        print(
                            "[ALERT]",
                            best["side"],
                            best["symbol"],
                            f"score={best['score']:.0f}"
                        )

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
                    f"Monitoradas: "
                    f"{len(selected)}/"
                    f"{MAX_CONTRACTS} | "
                    f"Candidatos: "
                    f"{len(candidates)} | "
                    f"melhor="
                    f"{best['symbol'] "
                    f"if best else 'nenhuma'} | "
                    f"BTC15="
                    f"{market['btc15']:.2f}% | "
                    f"BTC60="
                    f"{market['btc60']:.2f}% | "
                    f"regime="
                    f"{market['regime']}"
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

    # Porta do Render
    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

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
