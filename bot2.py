import os
import time
import threading
from collections import defaultdict, deque

import requests
from flask import Flask, jsonify


# =========================================================
# CONFIGURAÇÕES
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# NOVO domínio oficial da API Futures da MEXC
MEXC_BASE = "https://api.mexc.com"

INTERVALO_SCAN = 5

# Quantidade máxima de contratos monitorados
MAX_CONTRATOS = 100

# Máximo de 1 alerta por minuto
ALERTA_INTERVALO = 60

# Cooldown individual da moeda
COOLDOWN_MOEDA = 8 * 60

# =========================================================
# FILTRO PRINCIPAL
# =========================================================

# Pump confirmado:
PUMP_60S = 5.0

# Para detectar aceleração antes dos 5%
ACELERACAO_15S = 0.40
ACELERACAO_30S = 0.70

# Score mínimo para alerta normal
SCORE_MINIMO = 50

# Score mínimo para alerta antecipado
SCORE_ANTECIPADO = 60


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)

inicio_bot = time.time()
ultimo_alerta = 0
ultimo_alerta_moeda = {}

stats = {
    "scans": 0,
    "erros": 0,
    "alertas": 0,
    "candidatos": 0,
}


@app.route("/")
def home():
    return "🚀 PUMP RADAR V3 ONLINE"


@app.route("/status")
def status():
    return jsonify({
        "status": "online",
        "scans": stats["scans"],
        "erros": stats["erros"],
        "alertas": stats["alertas"],
        "candidatos": stats["candidatos"],
        "ultimo_alerta": ultimo_alerta,
        "tempo_rodando_segundos": int(time.time() - inicio_bot)
    })


def iniciar_servidor():
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )


# =========================================================
# TELEGRAM
# =========================================================

def enviar_telegram(texto):

    if not BOT_TOKEN or not CHAT_ID:
        print("ERRO: BOT_TOKEN ou CHAT_ID não configurado.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    dados = {
        "chat_id": CHAT_ID,
        "text": texto,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        r = requests.post(
            url,
            json=dados,
            timeout=10
        )

        if r.status_code != 200:
            print("Erro Telegram:", r.status_code, r.text)
            return False

        return True

    except Exception as e:
        print("Erro enviando Telegram:", e)
        return False


# =========================================================
# REQUEST MEXC
# =========================================================

def mexc_get(endpoint, params=None):

    url = MEXC_BASE + endpoint

    try:

        r = requests.get(
            url,
            params=params,
            timeout=10
        )

        if r.status_code != 200:
            print(
                f"ERRO MEXC {r.status_code}: "
                f"{endpoint} | {r.text[:300]}"
            )
            stats["erros"] += 1
            return None

        data = r.json()

        return data

    except Exception as e:

        print(
            f"ERRO REQUEST MEXC: "
            f"{endpoint} | {e}"
        )

        stats["erros"] += 1

        return None


# =========================================================
# CONTRATOS
# =========================================================

def obter_contratos():

    data = mexc_get(
        "/api/v1/contract/detail"
    )

    if not data:
        return []

    lista = data.get("data", [])

    contratos = []

    for item in lista:

        try:

            symbol = item.get("symbol", "")

            quote = str(
                item.get("quoteCoin", "")
            ).upper()

            settle = str(
                item.get("settleCoin", "")
            ).upper()

            state = item.get("state", 0)

            if (
                symbol
                and quote == "USDT"
                and settle == "USDT"
                and int(state) == 0
            ):
                contratos.append(symbol)

        except Exception:
            continue

    return contratos


# =========================================================
# TICKER
# =========================================================

def obter_tickers():

    data = mexc_get(
        "/api/v1/contract/ticker"
    )

    if not data:
        return {}

    lista = data.get("data", [])

    resultado = {}

    for item in lista:

        try:

            symbol = item.get("symbol")

            if not symbol:
                continue

            preco = float(
                item.get("lastPrice", 0)
            )

            if preco <= 0:
                continue

            resultado[symbol] = {
                "price": preco,

                # Open interest / posição aberta
                "holdVol": float(
                    item.get("holdVol", 0) or 0
                ),

                # Volume 24h
                "volume24": float(
                    item.get("volume24", 0) or 0
                ),

                "riseFallRate": float(
                    item.get("riseFallRate", 0) or 0
                ),
            }

        except Exception:
            continue

    return resultado


# =========================================================
# HISTÓRICO DE PREÇOS
# =========================================================

historico = defaultdict(
    lambda: deque(maxlen=90)
)


def registrar_precos(tickers):

    agora = time.time()

    for symbol, dados in tickers.items():

        historico[symbol].append(
            (
                agora,
                dados["price"],
                dados["holdVol"],
                dados["volume24"]
            )
        )


# =========================================================
# VARIAÇÃO
# =========================================================

def variacao(symbol, segundos):

    dados = historico.get(symbol)

    if not dados or len(dados) < 2:
        return None

    agora = time.time()

    preco_atual = dados[-1][1]

    alvo = agora - segundos

    anterior = None

    # Procuramos o preço mais próximo
    # do período desejado
    for item in dados:

        if item[0] <= alvo:
            anterior = item
        else:
            break

    if anterior is None:
        return None

    preco_antigo = anterior[1]

    if preco_antigo <= 0:
        return None

    return (
        (preco_atual - preco_antigo)
        / preco_antigo
    ) * 100


# =========================================================
# EMA
# =========================================================

def calcular_ema(precos, periodo):

    if len(precos) < periodo:
        return None

    multiplicador = 2 / (periodo + 1)

    ema = sum(precos[:periodo]) / periodo

    for preco in precos[periodo:]:
        ema = (
            (preco - ema)
            * multiplicador
        ) + ema

    return ema


def obter_emas(symbol):

    dados = historico.get(symbol)

    if not dados or len(dados) < 25:
        return None

    precos = [
        item[1]
        for item in dados
    ]

    ema9 = calcular_ema(
        precos,
        9
    )

    ema21 = calcular_ema(
        precos,
        21
    )

    ema50 = calcular_ema(
        precos,
        50
    )

    if not ema9 or not ema21:
        return None

    return {
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50
    }


# =========================================================
# BTC
# =========================================================

def obter_contexto_btc():

    v15 = variacao(
        "BTCUSDT",
        15
    )

    v30 = variacao(
        "BTCUSDT",
        30
    )

    v60 = variacao(
        "BTCUSDT",
        60
    )

    return (
        v15 or 0,
        v30 or 0,
        v60 or 0
    )


# =========================================================
# FORÇA RELATIVA
# =========================================================

def forca_relativa(
    movimento_moeda,
    movimento_btc
):

    return movimento_moeda - movimento_btc


# =========================================================
# SCORE
# =========================================================

def calcular_score(
    symbol,
    v15,
    v30,
    v60,
    btc15,
    btc30,
    btc60,
    tickers
):

    score = 0

    motivos = []

    # -----------------------------------------------------
    # 1. PUMP DE 60 SEGUNDOS
    # -----------------------------------------------------

    if v60 >= 5.0:

        score += 35

        motivos.append(
            f"🚨 PUMP 60s +{v60:.2f}%"
        )

    elif v60 >= 3.0:

        score += 25

        motivos.append(
            f"🔥 60s +{v60:.2f}%"
        )

    elif v60 >= 2.0:

        score += 18

        motivos.append(
            f"⚡ 60s +{v60:.2f}%"
        )

    elif v60 >= 1.0:

        score += 10

    # -----------------------------------------------------
    # 2. ACELERAÇÃO 30s
    # -----------------------------------------------------

    if v30 >= 2.0:

        score += 15

        motivos.append(
            f"🚀 30s +{v30:.2f}%"
        )

    elif v30 >= 1.0:

        score += 10

    elif v30 >= 0.5:

        score += 5

    # -----------------------------------------------------
    # 3. MOVIMENTO 15s
    # -----------------------------------------------------

    if v15 >= 1.0:

        score += 15

        motivos.append(
            f"⚡ 15s +{v15:.2f}%"
        )

    elif v15 >= 0.5:

        score += 10

    elif v15 >= 0.25:

        score += 5

    # -----------------------------------------------------
    # 4. FORÇA CONTRA BTC
    # -----------------------------------------------------

    rel30 = forca_relativa(
        v30,
        btc30
    )

    rel60 = forca_relativa(
        v60,
        btc60
    )

    if rel30 >= 1.0:

        score += 10

        motivos.append(
            "💪 Forte vs BTC"
        )

    elif rel30 >= 0.5:

        score += 6

    elif rel30 >= 0.2:

        score += 3

    if rel60 >= 1.0:

        score += 10

    elif rel60 >= 0.5:

        score += 6

    # -----------------------------------------------------
    # 5. EMA
    # -----------------------------------------------------

    emas = obter_emas(symbol)

    if emas:

        ema9 = emas["ema9"]
        ema21 = emas["ema21"]
        ema50 = emas["ema50"]

        preco = tickers[symbol]["price"]

        if preco > ema9 > ema21:

            score += 10

            motivos.append(
                "📈 EMA 9 > 21"
            )

        elif preco > ema9:

            score += 5

        if ema50:

            if ema9 > ema21 > ema50:

                score += 10

                motivos.append(
                    "📈 EMA 9 > 21 > 50"
                )

            elif ema9 > ema21:

                score += 5

    # -----------------------------------------------------
    # 6. OPEN INTEREST
    # -----------------------------------------------------

    dados_hist = historico.get(symbol)

    if dados_hist and len(dados_hist) >= 2:

        oi_atual = dados_hist[-1][2]
        oi_antigo = dados_hist[0][2]

        if oi_antigo > 0:

            oi_change = (
                (oi_atual - oi_antigo)
                / oi_antigo
            ) * 100

            if oi_change >= 5:

                score += 10

                motivos.append(
                    f"📊 OI +{oi_change:.1f}%"
                )

            elif oi_change >= 2:

                score += 5

    return min(score, 100), motivos


# =========================================================
# CLASSIFICAÇÃO
# =========================================================

def classificacao(score, v60):

    if v60 >= PUMP_60S:

        return "🔴 PUMP CONFIRMADO"

    if score >= 75:

        return "🟠 MOVIMENTO MUITO FORTE"

    if score >= 60:

        return "🟡 ACELERAÇÃO FORTE"

    return "⚪ CANDIDATO"


# =========================================================
# SELEÇÃO DOS CANDIDATOS
# =========================================================

def analisar(tickers):

    btc15, btc30, btc60 = obter_contexto_btc()

    candidatos = []

    for symbol in list(tickers.keys()):

        if symbol == "BTCUSDT":
            continue

        try:

            v15 = variacao(
                symbol,
                15
            )

            v30 = variacao(
                symbol,
                30
            )

            v60 = variacao(
                symbol,
                60
            )

            if (
                v15 is None
                or v30 is None
                or v60 is None
            ):
                continue

            # -------------------------------------------------
            # Só consideramos movimento de alta.
            # -------------------------------------------------

            if v60 < 0 and v30 < 0 and v15 < 0:
                continue

            # -------------------------------------------------
            # SCORE
            # -------------------------------------------------

            score, motivos = calcular_score(
                symbol,
                v15,
                v30,
                v60,
                btc15,
                btc30,
                btc60,
                tickers
            )

            # -------------------------------------------------
            # FILTRO ESPECIAL:
            # +5% nos últimos 60 segundos
            # sempre vira candidato.
            # -------------------------------------------------

            pump_confirmado = (
                v60 >= PUMP_60S
            )

            # -------------------------------------------------
            # Candidatos antecipados
            # -------------------------------------------------

            acelerando = (
                v15 >= ACELERACAO_15S
                and v30 >= ACELERACAO_30S
            )

            if (
                pump_confirmado
                or score >= SCORE_MINIMO
                or acelerando
            ):

                candidatos.append({
                    "symbol": symbol,
                    "score": score,
                    "v15": v15,
                    "v30": v30,
                    "v60": v60,
                    "btc15": btc15,
                    "btc30": btc30,
                    "btc60": btc60,
                    "motivos": motivos,
                    "pump": pump_confirmado
                })

        except Exception as e:

            print(
                f"Erro analisando {symbol}: {e}"
            )

    candidatos.sort(
        key=lambda x: (
            x["pump"],
            x["score"],
            x["v60"],
            x["v30"],
            x["v15"]
        ),
        reverse=True
    )

    return candidatos


# =========================================================
# ALERTA
# =========================================================

def pode_alertar(candidato):

    global ultimo_alerta

    agora = time.time()

    symbol = candidato["symbol"]

    # -----------------------------------------------------
    # Máximo 1 alerta por minuto
    # -----------------------------------------------------

    if (
        agora - ultimo_alerta
        < ALERTA_INTERVALO
    ):
        return False

    # -----------------------------------------------------
    # Cooldown da moeda
    # -----------------------------------------------------

    ultimo_moeda = (
        ultimo_alerta_moeda.get(
            symbol,
            0
        )
    )

    if (
        agora - ultimo_moeda
        < COOLDOWN_MOEDA
    ):
        return False

    return True


def enviar_alerta(candidato):

    global ultimo_alerta

    symbol = candidato["symbol"]

    score = candidato["score"]

    v15 = candidato["v15"]
    v30 = candidato["v30"]
    v60 = candidato["v60"]

    btc15 = candidato["btc15"]
    btc30 = candidato["btc30"]
    btc60 = candidato["btc60"]

    motivos = candidato["motivos"]

    tipo = classificacao(
        score,
        v60
    )

    texto = f"""
{tipo}

<b>{symbol}</b>

🎯 Score: <b>{score}/100</b>

📊 Movimento:
15s: <b>{v15:+.2f}%</b>
30s: <b>{v30:+.2f}%</b>
60s: <b>{v60:+.2f}%</b>

₿ BTC:
15s: {btc15:+.2f}%
30s: {btc30:+.2f}%
60s: {btc60:+.2f}%

<b>Confirmações:</b>
"""

    if motivos:

        for motivo in motivos[:6]:

            texto += f"{motivo}\n"

    else:

        texto += "Movimento detectado.\n"

    if v60 >= PUMP_60S:

        texto += (
            "\n🚨 <b>ATENÇÃO: "
            f"+{v60:.2f}% EM 60 SEGUNDOS</b>"
        )

    texto += (
        "\n\n⏱️ Radar V3"
        "\n⚠️ Sinal baseado em dados de mercado; "
        "não é garantia de continuação do movimento."
    )

    sucesso = enviar_telegram(
        texto
    )

    if sucesso:

        agora = time.time()

        ultimo_alerta = agora

        ultimo_alerta_moeda[
            symbol
        ] = agora

        stats["alertas"] += 1

        print(
            f"🚨 ALERTA ENVIADO: "
            f"{symbol} | "
            f"Score {score} | "
            f"60s {v60:+.2f}%"
        )

        return True

    return False


# =========================================================
# LOOP PRINCIPAL
# =========================================================

def monitorar():

    print("=" * 60)
    print("🚀 PUMP RADAR V3 INICIANDO")
    print("=" * 60)

    print(
        f"Filtro pump confirmado: "
        f"+{PUMP_60S:.1f}% / 60s"
    )

    print(
        f"Máximo de alertas: "
        f"1 por {ALERTA_INTERVALO}s"
    )

    print(
        f"Cooldown por moeda: "
        f"{COOLDOWN_MOEDA // 60} minutos"
    )

    contratos = obter_contratos()

    if not contratos:

        print(
            "❌ Não foi possível obter contratos."
        )

        return

    print(
        f"Contratos Futures USDT encontrados: "
        f"{len(contratos)}"
    )

    # Limitamos a quantidade inicial
    contratos = contratos[:MAX_CONTRATOS]

    print(
        f"Monitorando até "
        f"{len(contratos)} contratos."
    )

    enviar_telegram(
        "🚀 <b>PUMP RADAR V3 ONLINE</b>\n\n"
        f"Monitorando até {len(contratos)} Futures USDT.\n"
        f"🚨 Pump confirmado: +{PUMP_60S:.1f}% em 60s.\n"
        "📊 Ranking por score.\n"
        "⏱️ Máximo 1 alerta por minuto."
    )

    ultimo_log = 0

    while True:

        try:

            stats["scans"] += 1

            tickers = obter_tickers()

            if not tickers:

                print(
                    "⚠️ Nenhum ticker recebido."
                )

                time.sleep(
                    INTERVALO_SCAN
                )

                continue

            # Somente contratos válidos
            tickers = {
                s: d
                for s, d in tickers.items()
                if s in contratos
            }

            registrar_precos(
                tickers
            )

            # -------------------------------------------------
            # Análise
            # -------------------------------------------------

            candidatos = analisar(
                tickers
            )

            stats["candidatos"] = len(
                candidatos
            )

            # -------------------------------------------------
            # LOG RESUMIDO
            # -------------------------------------------------

            agora = time.time()

            if (
                agora - ultimo_log
                >= 60
            ):

                ultimo_log = agora

                print(
                    f"\n📡 Monitoradas: "
                    f"{len(tickers)}/{len(contratos)}"
                )

                if candidatos:

                    print(
                        "🏆 TOP CANDIDATOS:"
                    )

                    for c in candidatos[:5]:

                        print(
                            f"{c['symbol']} | "
                            f"Score {c['score']} | "
                            f"15s {c['v15']:+.2f}% | "
                            f"30s {c['v30']:+.2f}% | "
                            f"60s {c['v60']:+.2f}%"
                        )

                else:

                    print(
                        "Nenhum candidato forte "
                        "neste minuto."
                    )

            # -------------------------------------------------
            # Melhor candidato
            # -------------------------------------------------

            if candidatos:

                melhor = candidatos[0]

                if pode_alertar(
                    melhor
                ):

                    enviar_alerta(
                        melhor
                    )

            time.sleep(
                INTERVALO_SCAN
            )

        except Exception as e:

            stats["erros"] += 1

            print(
                "❌ ERRO NO LOOP:",
                repr(e)
            )

            time.sleep(
                INTERVALO_SCAN
            )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    servidor = threading.Thread(
        target=iniciar_servidor,
        daemon=True
    )

    servidor.start()

    monitorar()
