import os
import time
import threading
import sqlite3
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
