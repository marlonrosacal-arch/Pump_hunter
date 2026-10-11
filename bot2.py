
import os
import time
import threading
import requests
import pandas as pd
import ta
import ccxt

# ============================================================
# MEXC SCANNER — LONG + SHORT
# Limite global: no máximo 1 alerta a cada 60 segundos
# ============================================================

VERSION = "MEXC SCANNER V1.1"

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TIMEFRAME = "5m"
CANDLE_LIMIT = 50

MAX_ALERTS_PER_MINUTE = 1
ALERT_INTERVAL = 60
SYMBOL_COOLDOWN = 15 * 60

# Controle global de alertas
last_alert_time = 0.0
last_signal_by_symbol = {}
alert_lock = threading.Lock()

# Sessão HTTP para o Telegram
http = requests.Session()

# MEXC Futures via CCXT — dados públicos
exchange = ccxt.mexc({
    "options": {"defaultType": "swap"},
    "enableRateLimit": True,
    "timeout": 20000,
})


def send_telegram_signal(message, symbol):
    """Envia no máximo um alerta global a cada 60 segundos."""
    global last_alert_time

    now = time.monotonic()

    with alert_lock:
        if now - last_alert_time < ALERT_INTERVAL:
            remaining = ALERT_INTERVAL - (now - last_alert_time)
            print(
                f"[LIMITE] Alerta ignorado para {symbol}. "
                f"Aguarde {remaining:.1f}s."
            )
            return False

        last_symbol_time = last_signal_by_symbol.get(symbol, 0.0)

        if now - last_symbol_time < SYMBOL_COOLDOWN:
            print(f"[COOLDOWN] Sinal repetido ignorado: {symbol}")
            return False

        if not TELEGRAM_TOKEN or not CHAT_ID:
            print("[ERRO] Configure BOT_TOKEN e CHAT_ID no Render.")
            return False

        url = (
            "https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/sendMessage"
        )

        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        }

        try:
            response = http.post(
                url,
                json=payload,
                timeout=15,
            )
            response.raise_for_status()

            result = response.json()

            if not result.get("ok"):
                print(f"[TELEGRAM ERRO] {result}")
                return False

            # Atualiza o limite somente após envio confirmado
            last_alert_time = time.monotonic()
            last_signal_by_symbol[symbol] = last_alert_time

            print(f"[ALERTA ENVIADO] {symbol}")
            return True

        except Exception as e:
            print(f"[TELEGRAM ERRO] {type(e).__name__}: {e}")
            return False


def get_active_altcoins():
    """Seleciona contratos perpétuos ativos liquidados em USDT."""
    try:
        markets = exchange.load_markets()
        symbols = []

        for symbol, market in markets.items():
            if not market.get("swap"):
                continue

            if market.get("active") is False:
                continue

            if market.get("quote") != "USDT":
                continue

            if market.get("settle") != "USDT":
                continue

            base = market.get("base", "")

            if base in ("BTC", "ETH", "USDC"):
                continue

            symbols.append(symbol)

        symbols.sort()
        return symbols

    except Exception as e:
        print(f"[ERRO MERCADOS] {type(e).__name__}: {e}")
        return []


def format_price(value):
    """Evita perder precisão em moedas de preço muito baixo."""
    return f"{float(value):.8g}"


def scan_symbol(symbol):
    try:
        bars = exchange.fetch_ohlcv(
            symbol,
            timeframe=TIMEFRAME,
            limit=CANDLE_LIMIT,
        )

        if not bars or len(bars) < 30:
            return

        df = pd.DataFrame(
            bars,
            columns=[
                "timestamp", "open", "high",
                "low", "close", "volume"
            ],
        )

        # Indicadores
        bb = ta.volatility.BollingerBands(
            close=df["close"],
            window=20,
            window_dev=2,
        )
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()

        df["rsi"] = ta.momentum.RSIIndicator(
            close=df["close"],
            window=14,
        ).rsi()

        # Média das 20 barras ANTERIORES
        df["vol_mean"] = (
            df["volume"].rolling(window=20).mean().shift(1)
        )

        df["atr"] = ta.volatility.AverageTrueRange(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=14,
        ).average_true_range()

        # Usa o último candle fechado, não o candle em formação
        row = df.iloc[-2]

        required = [
            row["close"],
            row["bb_high"],
            row["bb_low"],
            row["rsi"],
            row["vol_mean"],
            row["atr"],
            row["volume"],
        ]

        if any(pd.isna(value) for value in required):
            return

        if row["vol_mean"] <= 0 or row["atr"] <= 0:
            return

        # Volume pelo menos 80% acima da média anterior
        volume_explosivo = (
            row["volume"] >= row["vol_mean"] * 1.8
        )

        if not volume_explosivo:
            return

        close = float(row["close"])
        rsi = float(row["rsi"])
        atr = float(row["atr"])

        # Exemplo de símbolo CCXT:
        # SOL/USDT:USDT -> SOL/USDT
        coin = symbol.split(":")[0]

        sl_distance = atr * 1.5
        tp1_distance = atr * 1.5
        tp2_distance = atr * 3.0

        # ----------------------------------------------------
        # LONG
        # ----------------------------------------------------
        if close > row["bb_high"] and 55 < rsi < 68:
            sl = close - sl_distance
            tp1 = close + tp1_distance
            tp2 = close + tp2_distance

            msg = (
                "🔥 *MEXC SCANNER — LONG* 🔥\n\n"
                f"🪙 *Ativo:* `{coin}`\n"
                f"⏱️ *Período:* `{TIMEFRAME}`\n"
                f"📈 *Entrada de referência:* `{format_price(close)}`\n"
                f"📊 *RSI:* `{rsi:.2f}`\n"
                f"🔊 *Volume:* `{row['volume'] / row['vol_mean']:.2f}x` a média\n\n"
                f"🛡️ *STOP LOSS:* `{format_price(sl)}`\n"
                f"🎯 *TAKE PROFIT 1:* `{format_price(tp1)}`\n"
                f"🚀 *TAKE PROFIT 2:* `{format_price(tp2)}`\n\n"
                "_Motivo: rompimento da banda superior + RSI + volume._\n"
                "_Alerta informativo; não é uma ordem de negociação._"
            )

            send_telegram_signal(msg, symbol)

        # ----------------------------------------------------
        # SHORT
        # ----------------------------------------------------
        elif close < row["bb_low"] and 32 < rsi < 45:
            sl = close + sl_distance
            tp1 = close - tp1_distance
            tp2 = close - tp2_distance

            msg = (
                "📉 *MEXC SCANNER — SHORT* 📉\n\n"
                f"🪙 *Ativo:* `{coin}`\n"
                f"⏱️ *Período:* `{TIMEFRAME}`\n"
                f"📉 *Entrada de referência:* `{format_price(close)}`\n"
                f"📊 *RSI:* `{rsi:.2f}`\n"
                f"🔊 *Volume:* `{row['volume'] / row['vol_mean']:.2f}x` a média\n\n"
                f"🛡️ *STOP LOSS:* `{format_price(sl)}`\n"
                f"🎯 *TAKE PROFIT 1:* `{format_price(tp1)}`\n"
                f"🚀 *TAKE PROFIT 2:* `{format_price(tp2)}`\n\n"
                "_Motivo: rompimento da banda inferior + RSI + volume._\n"
                "_Alerta informativo; não é uma ordem de negociação._"
            )

            send_telegram_signal(msg, symbol)

    except ccxt.RateLimitExceeded as e:
        print(f"[LIMITE MEXC] {symbol}: {e}")

    except ccxt.NetworkError as e:
        print(f"[REDE] {symbol}: {e}")

    except Exception as e:
        print(
            f"[ERRO SCAN] {symbol}: "
            f"{type(e).__name__}: {e}"
        )


def run_scanner():
    print("=" * 55)
    print(f"🚀 {VERSION} iniciado")
    print(f"Timeframe: {TIMEFRAME}")
    print(f"Limite global: 1 alerta a cada {ALERT_INTERVAL}s")
    print(f"Cooldown por moeda: {SYMBOL_COOLDOWN // 60} min")
    print("=" * 55)

    while True:
        symbols = get_active_altcoins()

        if not symbols:
            print("[AVISO] Nenhum contrato encontrado. Tentando novamente.")
            time.sleep(60)
            continue

        print(f"[SCAN] {len(symbols)} contratos encontrados.")
        start = time.monotonic()

        for index, symbol in enumerate(symbols, start=1):
            scan_symbol(symbol)

            if index % 50 == 0:
                print(f"[SCAN] Progresso: {index}/{len(symbols)}")

        elapsed = time.monotonic() - start
        print(f"[SCAN] Concluído em {elapsed:.1f}s.")

        # Nova varredura após 120 segundos
        time.sleep(120)



app = Flask(__name__)

@app.route("/")
def home():
    return "MEXC Scanner ativo!", 200


if __name__ == "__main__":
    scanner_thread = threading.Thread(
        target=run_scanner,
        daemon=True
    )
    scanner_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

