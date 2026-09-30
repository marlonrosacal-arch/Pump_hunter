import os
import time
import threading
from collections import deque
from datetime import datetime

import requests
from flask import Flask, jsonify

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MEXC_BASE = "https://contract.mexc.com"

# ============================================================
# CONFIGURAÇÃO V5
# ============================================================

SCAN_INTERVAL = 5
MAX_CONTRACTS = 50

# IMPORTANTE:
# 1 alerta por minuto é apenas o LIMITE.
# O bot NÃO é obrigado a enviar alerta.
ALERT_INTERVAL = 60

# Tempo mínimo antes de alertar novamente a mesma moeda
COOLDOWN_SYMBOL = 8 * 60

# Atualiza a lista de contratos a cada 15 minutos
CONTRACT_REFRESH = 15 * 60

# Score mínimo para gerar alerta
MIN_SCORE = 72

# Score considerado oportunidade forte
STRONG_SCORE = 82

# +5% em 60s = pump confirmado
PUMP_60S = 5.0

# Movimento mínimo
MIN_MOVE_15S = 0.35
MIN_MOVE_30S = 0.70
MIN_MOVE_60S = 1.20

# RSI
RSI_MIN = 52.0
RSI_MAX_ENTRY = 78.0
RSI_RISE_MIN = 4.0

# BTC serve como contexto/penalidade.
# Não bloqueia automaticamente uma altcoin forte.
BTC_DROP_15S = -0.60
BTC_DROP_60S = -1.50

# Histórico de aproximadamente 8 minutos
HISTORY_SECONDS = 8 * 60
MAX_HISTORY = HISTORY_SECONDS // SCAN_INTERVAL + 20


# ============================================================
# ESTADO
# ============================================================

app = Flask(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "PumpHunterV5/1.0"
})

histories = {}
last_alert_by_symbol = {}

last_alert_time = 0.0
last_scan_time = 0.0
last_successful_api = 0.0

last_error = ""

contracts = []

running = True


# ============================================================
# UTILITÁRIOS
# ============================================================

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_json(path, params=None, timeout=8):
    global last_error
    global last_successful_api

    try:
        response = session.get(
            MEXC_BASE + path,
            params=params,
            timeout=timeout
        )

        response.raise_for_status()

        data = response.json()

        last_successful_api = time.time()

        return data

    except Exception as e:

        last_error = (
            f"{type(e).__name__}: {e}"
        )

        return None


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not BOT_TOKEN or not CHAT_ID:

        print(
            "ERRO: BOT_TOKEN ou CHAT_ID "
            "não configurado."
        )

        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

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
            f"Telegram HTTP "
            f"{response.status_code}: "
            f"{response.text[:300]}"
        )

    except Exception as e:

        print(
            f"Erro Telegram: {e}"
        )

    return False


# ============================================================
# CONTRATOS MEXC
# ============================================================

def refresh_contracts():

    global contracts

    data = get_json(
        "/api/v1/contract/detail",
        timeout=8
    )

    if not data or not data.get("success"):

        print(
            f"[{now_str()}] "
            "⚠️ Falha ao atualizar contratos."
        )

        return

    items = data.get("data") or []

    symbols = []

    for item in items:

        symbol = item.get("symbol", "")

        if symbol.endswith("_USDT"):

            symbols.append(symbol)

    if symbols:

        contracts = symbols

        print(
            f"[{now_str()}] "
            f"Contratos encontrados: "
            f"{len(contracts)}"
        )


# ============================================================
# TICKERS
# ============================================================

def get_all_tickers():

    data = get_json(
        "/api/v1/contract/ticker",
        timeout=8
    )

    if not data or not data.get("success"):

        return {}

    raw = data.get("data")

    if isinstance(raw, list):

        items = raw

    elif isinstance(raw, dict):

        if (
            "symbol" in raw
            and "lastPrice" in raw
        ):

            items = [raw]

        else:

            items = []

    else:

        items = []

    result = {}

    for item in items:

        try:

            symbol = item.get("symbol")

            price = float(
                item.get("lastPrice")
            )

            if not symbol or price <= 0:
                continue

            result[symbol] = {

                "price": price,

                "volume24": float(
                    item.get("volume24") or 0
                ),

                "amount24": float(
                    item.get("amount24") or 0
                ),

                "holdVol": float(
                    item.get("holdVol") or 0
                ),

                "riseFallRate": float(
                    item.get("riseFallRate") or 0
                ) * 100,

                "timestamp": int(
                    item.get(
                        "timestamp",
                        time.time() * 1000
                    )
                )
            }

        except Exception:

            continue

    return result


# ============================================================
# CÁLCULOS
# ============================================================

def pct_change(old, new):

    if old is None or old <= 0:
        return 0.0

    return (
        (new / old) - 1.0
    ) * 100.0


def value_at(hist, seconds_ago, index):

    if not hist:
        return None

    target = (
        time.time() - seconds_ago
    )

    for ts, price, oi in reversed(hist):

        if ts <= target:

            return (
                price
                if index == 1
                else oi
            )

    return hist[0][index]


def ema(values, period):

    if len(values) < period:
        return None

    k = 2 / (period + 1)

    value = (
        sum(values[:period])
        / period
    )

    for item in values[period:]:

        value = (
            item * k
            + value * (1 - k)
        )

    return value


# ============================================================
# RSI
# ============================================================

def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i] - values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            avg_gain * (period - 1)
            + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1)
            + losses[i]
        ) / period

    if avg_loss == 0:

        return 100.0

    rs = avg_gain / avg_loss

    return (
        100
        - (100 / (1 + rs))
    )


# ============================================================
# ANÁLISE
# ============================================================

def analyze(symbol, ticker, btc):

    hist = histories.get(symbol)

    if not hist or len(hist) < 25:
        return None

    prices = [
        x[1]
        for x in hist
    ]

    current = prices[-1]

    p5 = value_at(
        hist, 5, 1
    )

    p10 = value_at(
        hist, 10, 1
    )

    p15 = value_at(
        hist, 15, 1
    )

    p30 = value_at(
        hist, 30, 1
    )

    p60 = value_at(
        hist, 60, 1
    )

    if None in (
        p5,
        p10,
        p15,
        p30,
        p60
    ):

        return None

    r5 = pct_change(
        p5, current
    )

    r10 = pct_change(
        p10, current
    )

    r15 = pct_change(
        p15, current
    )

    r30 = pct_change(
        p30, current
    )

    r60 = pct_change(
        p60, current
    )

    # ========================================================
    # ACELERAÇÃO
    # ========================================================

    accel_15 = (
        r15 - (r30 / 2.0)
    )

    accel_5 = (
        r5 - (r10 - r5)
    )

    # ========================================================
    # MÁXIMAS RECENTES
    # ========================================================

    now = time.time()

    recent30 = [
        price
        for ts, price, oi in hist
        if ts >= now - 30
    ]

    recent60 = [
        price
        for ts, price, oi in hist
        if ts >= now - 60
    ]

    high30 = (
        max(recent30)
        if recent30
        else current
    )

    high60 = (
        max(recent60)
        if recent60
        else current
    )

    from_high30 = pct_change(
        high30,
        current
    )

    from_high60 = pct_change(
        high60,
        current
    )

    # ========================================================
    # RSI
    # ========================================================

    rsi_now = rsi(
        prices[-80:],
        14
    )

    if rsi_now is None:
        return None

    rsi_previous = None

    if len(prices) >= 92:

        rsi_previous = rsi(
            prices[-92:-12],
            14
        )

    rsi_delta = (

        rsi_now - rsi_previous

        if rsi_previous is not None

        else 0.0
    )

    # ========================================================
    # EMA
    # ========================================================

    ema9 = ema(
        prices[-60:],
        9
    )

    ema21 = ema(
        prices[-60:],
        21
    )

    ema50 = ema(
        prices[-70:],
        50
    )

    if None in (
        ema9,
        ema21,
        ema50
    ):

        return None

    # ========================================================
    # OPEN INTEREST
    # ========================================================

    oi_now = ticker.get(
        "holdVol",
        0.0
    )

    oi60 = value_at(
        hist,
        60,
        2
    )

    oi_change = (

        pct_change(
            oi60,
            oi_now
        )

        if oi60

        else 0.0
    )

    # ========================================================
    # BTC
    # ========================================================

    btc15 = btc.get(
        "r15",
        0.0
    )

    btc30 = btc.get(
        "r30",
        0.0
    )

    btc60 = btc.get(
        "r60",
        0.0
    )

    # ========================================================
    # SCORE
    # ========================================================

    score = 0

    reasons = []

    # --------------------------------------------------------
    # MOVIMENTO
    # --------------------------------------------------------

    if r15 >= MIN_MOVE_15S:

        score += 14

        reasons.append(
            f"15s +{r15:.2f}%"
        )

    if r30 >= MIN_MOVE_30S:

        score += 12

        reasons.append(
            f"30s +{r30:.2f}%"
        )

    if r60 >= MIN_MOVE_60S:

        score += 10

        reasons.append(
            f"60s +{r60:.2f}%"
        )

    if r5 > 0.15:

        score += 8

        reasons.append(
            f"5s +{r5:.2f}%"
        )

    # --------------------------------------------------------
    # ACELERAÇÃO
    # --------------------------------------------------------

    if accel_15 > 0.20:

        score += 10

        reasons.append(
            "aceleração"
        )

    if accel_5 > 0.08:

        score += 6

        reasons.append(
            "aceleração curta"
        )

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if (
        rsi_now >= RSI_MIN
        and rsi_delta >= RSI_RISE_MIN
    ):

        score += 14

        reasons.append(
            f"RSI {rsi_now:.0f} subindo"
        )

    elif (
        rsi_now >= 55
        and rsi_delta > 1.5
    ):

        score += 8

        reasons.append(
            f"RSI {rsi_now:.0f} ↑"
        )

    if rsi_now > RSI_MAX_ENTRY:

        score -= 15

        reasons.append(
            f"RSI esticado {rsi_now:.0f}"
        )

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if current > ema9:

        score += 5

        reasons.append(
            "preço > EMA9"
        )

    if ema9 > ema21:

        score += 6

        reasons.append(
            "EMA9 > EMA21"
        )

    if ema21 > ema50:

        score += 5

        reasons.append(
            "EMA21 > EMA50"
        )

    # --------------------------------------------------------
    # ROMPIMENTO
    # --------------------------------------------------------

    if current >= high30 * 0.9995:

        score += 10

        reasons.append(
            "nova máxima 30s"
        )

    elif current >= high60 * 0.9995:

        score += 6

        reasons.append(
            "nova máxima 60s"
        )

    # --------------------------------------------------------
    # OPEN INTEREST
    # --------------------------------------------------------

    if oi_change > 0.30:

        score += 7

        reasons.append(
            f"OI +{oi_change:.2f}%"
        )

    elif oi_change < -0.50:

        score -= 5

        reasons.append(
            f"OI {oi_change:.2f}%"
        )

    # --------------------------------------------------------
    # BTC
    # --------------------------------------------------------

    if btc15 <= BTC_DROP_15S:

        score -= 5

        reasons.append(
            "BTC pressionando"
        )

    if btc60 <= BTC_DROP_60S:

        score -= 6

        reasons.append(
            "BTC fraco"
        )

    # --------------------------------------------------------
    # EXAUSTÃO
    # --------------------------------------------------------

    if (
        r15 < -0.30
        and r60 >= PUMP_60S
    ):

        score -= 25

        reasons.append(
            "pump devolvendo"
        )

    if from_high30 < -0.60:

        score -= 15

        reasons.append(
            "abaixo da máxima 30s"
        )

    if from_high60 < -1.00:

        score -= 10

        reasons.append(
            "abaixo da máxima 60s"
        )

    # +5% em 60s = confirmação
    # mas não é obrigatório.

    pump_confirmed = (
        r60 >= PUMP_60S
    )

    # ========================================================
    # VALIDAÇÃO FINAL
    # ========================================================

    valid = (

        score >= MIN_SCORE

        and r5 > 0

        and r15 > 0

        and rsi_now >= RSI_MIN

        and rsi_delta >= 0

        and from_high30 > -0.60
    )

    return {

        "symbol": symbol,

        "score": max(
            0,
            min(100, int(score))
        ),

        "r5": r5,
        "r15": r15,
        "r30": r30,
        "r60": r60,

        "rsi": rsi_now,
        "rsi_delta": rsi_delta,

        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,

        "oi_change": oi_change,

        "btc15": btc15,
        "btc30": btc30,
        "btc60": btc60,

        "pump_confirmed":
            pump_confirmed,

        "valid":
            valid,

        "reasons":
            reasons,

        "from_high30":
            from_high30,

        "from_high60":
            from_high60
    }


# ============================================================
# BTC
# ============================================================

def build_btc(tickers):

    ticker = tickers.get(
        "BTC_USDT"
    )

    if not ticker:

        return {
            "r15": 0.0,
            "r30": 0.0,
            "r60": 0.0
        }

    hist = histories.get(
        "BTC_USDT",
        []
    )

    if len(hist) < 5:

        return {
            "r15": 0.0,
            "r30": 0.0,
            "r60": 0.0
        }

    current = ticker["price"]

    result = {}

    for seconds in (
        15,
        30,
        60
    ):

        old_price = value_at(
            hist,
            seconds,
            1
        )

        result[
            f"r{seconds}"
        ] = (

            pct_change(
                old_price,
                current
            )

            if old_price

            else 0.0
        )

    return result


# ============================================================
# ESCOLHER MELHOR OPORTUNIDADE
# ============================================================

def choose_best(tickers, btc):

    candidates = []

    for symbol in list(
        contracts
    ):

        ticker = tickers.get(
            symbol
        )

        if not ticker:
            continue

        analysis = analyze(
            symbol,
            ticker,
            btc
        )

        if not analysis:
            continue

        if not analysis["valid"]:
            continue

        last_alert = (
            last_alert_by_symbol.get(
                symbol,
                0
            )
        )

        if (
            time.time()
            - last_alert
            < COOLDOWN_SYMBOL
        ):

            continue

        candidates.append(
            analysis
        )

    if not candidates:
        return None

    candidates.sort(

        key=lambda x: (
            x["score"],
            x["r15"],
            x["rsi_delta"],
            x["r5"]
        ),

        reverse=True
    )

    return candidates[0]


# ============================================================
# MENSAGEM TELEGRAM
# ============================================================

def format_alert(a):

    if a["pump_confirmed"]:

        title = (
            "🔴 PUMP CONFIRMADO"
        )

    elif (
        a["score"]
        >= STRONG_SCORE
    ):

        title = (
            "🟢 OPORTUNIDADE FORTE"
        )

    else:

        title = (
            "🟡 OPORTUNIDADE"
        )

    confirmations = "\n".join(

        f"• {reason}"

        for reason
        in a["reasons"][:8]
    )

    return (

        f"{title}\n\n"

        f"*{a['symbol']}*\n\n"

        f"🎯 Score: "
        f"*{a['score']}/100*\n\n"

        f"📊 Movimento:\n"

        f"5s: "
        f"*{a['r5']:+.2f}%*\n"

        f"15s: "
        f"*{a['r15']:+.2f}%*\n"

        f"30s: "
        f"*{a['r30']:+.2f}%*\n"

        f"60s: "
        f"*{a['r60']:+.2f}%*\n\n"

        f"📈 RSI: "
        f"*{a['rsi']:.1f}* "
        f"({a['rsi_delta']:+.1f})\n\n"

        f"📐 EMA9: "
        f"{a['ema9']:.6g}\n"

        f"EMA21: "
        f"{a['ema21']:.6g}\n"

        f"EMA50: "
        f"{a['ema50']:.6g}\n\n"

        f"📊 OI 60s: "
        f"*{a['oi_change']:+.2f}%*\n\n"

        f"₿ BTC:\n"

        f"15s: "
        f"{a['btc15']:+.2f}%\n"

        f"30s: "
        f"{a['btc30']:+.2f}%\n"

        f"60s: "
        f"{a['btc60']:+.2f}%\n\n"

        f"🔎 *Confirmações:*\n"

        f"{confirmations}\n\n"

        "⚠️ Sinal baseado em dados "
        "de mercado; não é garantia "
        "de continuação do movimento."
    )


# ============================================================
# MONITOR PRINCIPAL
# ============================================================

def monitor_loop():

    global last_scan_time
    global last_alert_time
    global contracts

    refresh_contracts()

    next_refresh = (
        time.time()
        + CONTRACT_REFRESH
    )

    scan_count = 0

    print(
        f"[{now_str()}] "
        "❤️ PUMP HUNTER V5 ONLINE"
    )

    print(
        f"[{now_str()}] "
        "Modo: qualidade > quantidade. "
        "Sem obrigação de alertar."
    )

    while running:

        started = time.time()

        try:

            # ------------------------------------------------
            # Atualiza contratos
            # ------------------------------------------------

            if (
                time.time()
                >= next_refresh
            ):

                refresh_contracts()

                next_refresh = (
                    time.time()
                    + CONTRACT_REFRESH
                )

            # ------------------------------------------------
            # Tickers
            # ------------------------------------------------

            tickers = get_all_tickers()

            if not tickers:

                print(
                    f"[{now_str()}] "
                    "⚠️ Sem dados de ticker."
                )

                time.sleep(
                    SCAN_INTERVAL
                )

                continue

            # ------------------------------------------------
            # Seleciona contratos por volume
            # ------------------------------------------------

            ranked = sorted(

                [
                    symbol

                    for symbol
                    in contracts

                    if symbol in tickers
                ],

                key=lambda symbol:
                    tickers[symbol].get(
                        "amount24",
                        0
                    ),

                reverse=True
            )[:MAX_CONTRACTS]

            contracts = ranked

            timestamp = time.time()

            # ------------------------------------------------
            # Histórico
            # ------------------------------------------------

            for symbol in ranked:

                ticker = tickers[
                    symbol
                ]

                history = (
                    histories.setdefault(
                        symbol,
                        deque(
                            maxlen=MAX_HISTORY
                        )
                    )
                )

                history.append(

                    (
                        timestamp,

                        ticker["price"],

                        ticker.get(
                            "holdVol",
                            0.0
                        )
                    )
                )

            # ------------------------------------------------
            # BTC
            # ------------------------------------------------

            if (
                "BTC_USDT"
                in tickers
            ):

                btc_ticker = (
                    tickers[
                        "BTC_USDT"
                    ]
                )

                history = (
                    histories.setdefault(
                        "BTC_USDT",
                        deque(
                            maxlen=MAX_HISTORY
                        )
                    )
                )

                history.append(

                    (
                        timestamp,

                        btc_ticker["price"],

                        btc_ticker.get(
                            "holdVol",
                            0.0
                        )
                    )
                )

            btc = build_btc(
                tickers
            )

            # ------------------------------------------------
            # Procura a melhor
            # ------------------------------------------------

            best = choose_best(
                tickers,
                btc
            )

            last_scan_time = (
                time.time()
            )

            scan_count += 1

            # ------------------------------------------------
            # ALERTA
            # ------------------------------------------------

            if best:

                elapsed_alert = (
                    time.time()
                    - last_alert_time
                )

                # Máximo 1 alerta/minuto.
                # NÃO existe obrigação de enviar.

                if (
                    elapsed_alert
                    >= ALERT_INTERVAL
                ):

                    best["btc15"] = (
                        btc["r15"]
                    )

                    best["btc30"] = (
                        btc["r30"]
                    )

                    best["btc60"] = (
                        btc["r60"]
                    )

                    print(

                        f"[{now_str()}] "
                        f"🚨 CANDIDATA: "

                        f"{best['symbol']} | "

                        f"Score "
                        f"{best['score']} | "

                        f"15s "
                        f"{best['r15']:+.2f}% | "

                        f"60s "
                        f"{best['r60']:+.2f}% | "

                        f"RSI "
                        f"{best['rsi']:.1f}"
                    )

                    if send_telegram(
                        format_alert(
                            best
                        )
                    ):

                        last_alert_time = (
                            time.time()
                        )

                        last_alert_by_symbol[
                            best["symbol"]
                        ] = (
                            last_alert_time
                        )

                        print(

                            f"[{now_str()}] "
                            f"✅ Alerta enviado: "
                            f"{best['symbol']}"
                        )

            # ------------------------------------------------
            # LOG RESUMIDO
            # ------------------------------------------------

            if (
                scan_count % 12
                == 0
            ):

                if best:

                    best_text = (

                        f"{best['symbol']} "
                        f"score="
                        f"{best['score']}"
                    )

                else:

                    best_text = (
                        "nenhuma"
                    )

                print(

                    f"[{now_str()}] "

                    f"Monitoradas: "
                    f"{len(ranked)}/"
                    f"{MAX_CONTRACTS} | "

                    f"BTC 15s="
                    f"{btc['r15']:+.2f}% | "

                    f"60s="
                    f"{btc['r60']:+.2f}% | "

                    f"melhor="
                    f"{best_text}"
                )

        except Exception as e:

            print(

                f"[{now_str()}] "
                f"❌ Erro no monitor: "

                f"{type(e).__name__}: "
                f"{e}"
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

def watchdog():

    while running:

        time.sleep(30)

        if not last_scan_time:
            continue

        age = (
            time.time()
            - last_scan_time
        )

        if age > 90:

            print(

                f"[{now_str()}] "
                f"⚠️ WATCHDOG: "
                f"último scan há "
                f"{age:.0f}s"
            )


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return (
        "Pump Hunter V5 online"
    )


@app.route("/status")
def status():

    scan_age = (

        time.time()
        - last_scan_time

        if last_scan_time

        else None
    )

    api_age = (

        time.time()
        - last_successful_api

        if last_successful_api

        else None
    )

    alert_age = (

        time.time()
        - last_alert_time

        if last_alert_time

        else None
    )

    return jsonify({

        "status":
            "online",

        "version":
            "V5",

        "last_scan_age_seconds":

            round(
                scan_age,
                1
            )

            if scan_age is not None

            else None,

        "last_api_ok_age_seconds":

            round(
                api_age,
                1
            )

            if api_age is not None

            else None,

        "contracts":
            len(contracts),

        "last_alert_age_seconds":

            round(
                alert_age,
                1
            )

            if alert_age is not None

            else None,

        "last_error":
            last_error
    })


# ============================================================
# INICIALIZAÇÃO
# ============================================================

def start():

    print(
        "Iniciando Bot 2..."
    )

    print(
        "===================================="
    )

    print(
        "BOT 2 MEXC PUMP HUNTER V5"
    )

    print(
        "===================================="
    )

    monitor = threading.Thread(

        target=monitor_loop,

        daemon=True,

        name="monitor"
    )

    monitor.start()

    watchdog_thread = (
        threading.Thread(

            target=watchdog,

            daemon=True,

            name="watchdog"
        )
    )

    watchdog_thread.start()

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        ),

        debug=False,

        use_reloader=False
    )


if __name__ == "__main__":

    start()
