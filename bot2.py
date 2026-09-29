import os
import time
import signal
import threading
import traceback
from collections import defaultdict, deque

import requests
from flask import Flask, jsonify


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MEXC_BASE = "https://api.mexc.com"

# Intervalo principal
INTERVALO_SCAN = 5

# Quantidade máxima de contratos acompanhados
MAX_CONTRATOS = 50

# Máximo de alertas
ALERTA_INTERVALO = 60

# Depois de alertar uma moeda, espera este tempo
COOLDOWN_MOEDA = 8 * 60

# ============================================================
# FILTROS
# ============================================================

# Principal requisito:
# +5% ou mais nos últimos 60 segundos
PUMP_60S = 5.0

# Movimentos antecipados
MIN_15S = 0.35
MIN_30S = 0.60
MIN_60S = 0.80

# Score mínimo para alerta antecipado
SCORE_MINIMO = 55

# Score especial para movimentos muito fortes
SCORE_FORTE = 70

# BTC
BTC_MAX_QUEDA_15S = -0.45
BTC_MAX_QUEDA_60S = -1.00

# Histórico
HISTORICO_MAX = 100

# Timeout das APIs
HTTP_TIMEOUT = 8

# ============================================================
# ESTADO
# ============================================================

app = Flask(__name__)

session = requests.Session()

historico = defaultdict(lambda: deque(maxlen=HISTORICO_MAX))

ultimo_alerta_global = 0
ultimo_alerta_moeda = {}

contratos_monitorados = []

rodando = True

estado = {
    "inicio": time.time(),
    "ultimo_scan": None,
    "ultimo_sucesso_mexc": None,
    "ultimo_erro": None,
    "scans": 0,
    "erros": 0,
    "alertas": 0,
    "contratos": 0,
}


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():
    return "Pump Hunter V4 ONLINE"


@app.route("/status")
def status():
    agora = time.time()

    ultimo_scan = estado["ultimo_scan"]

    idade_scan = None
    if ultimo_scan:
        idade_scan = round(agora - ultimo_scan, 1)

    return jsonify({
        "status": "online",
        "versao": "V4",
        "uptime_segundos": round(agora - estado["inicio"]),
        "ultimo_scan_segundos_atras": idade_scan,
        "ultimo_sucesso_mexc": estado["ultimo_sucesso_mexc"],
        "ultimo_erro": estado["ultimo_erro"],
        "scans": estado["scans"],
        "erros": estado["erros"],
        "alertas": estado["alertas"],
        "contratos_monitorados": estado["contratos"],
        "ultima_alerta_global_segundos_atras": (
            round(agora - ultimo_alerta_global, 1)
            if ultimo_alerta_global
            else None
        )
    })


def iniciar_servidor():
    porta = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=porta,
        threaded=True,
        use_reloader=False
    )


# ============================================================
# HTTP
# ============================================================

def get_json(url, params=None, tentativas=3):
    ultimo_erro = None

    for tentativa in range(tentativas):
        try:
            resposta = session.get(
                url,
                params=params,
                timeout=HTTP_TIMEOUT
            )

            resposta.raise_for_status()

            return resposta.json()

        except Exception as e:
            ultimo_erro = e

            if tentativa < tentativas - 1:
                time.sleep(0.7)

    raise ultimo_erro


# ============================================================
# TELEGRAM
# ============================================================

def enviar_telegram(mensagem):
    if not BOT_TOKEN or not CHAT_ID:
        print("ERRO: BOT_TOKEN ou CHAT_ID não configurado.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    dados = {
        "chat_id": CHAT_ID,
        "text": mensagem,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        resposta = session.post(
            url,
            data=dados,
            timeout=HTTP_TIMEOUT
        )

        if resposta.ok:
            return True

        print(
            "Erro Telegram:",
            resposta.status_code,
            resposta.text[:300]
        )

    except Exception as e:
        print("Erro ao enviar Telegram:", e)

    return False


# ============================================================
# CONTRATOS
# ============================================================

def obter_contratos():
    """
    Busca os contratos Futures USDT disponíveis.
    Depois filtra e ordena por liquidez/volume.
    """

    url = f"{MEXC_BASE}/api/v1/contract/detail"

    try:
        dados = get_json(url)

        if not isinstance(dados, dict):
            raise ValueError("Resposta inválida da MEXC")

        lista = dados.get("data", [])

        candidatos = []

        for item in lista:

            simbolo = item.get("symbol", "")

            if not simbolo.endswith("USDT"):
                continue

            # Alguns contratos podem estar inativos
            if item.get("state") not in (None, 0, 1, "0", "1"):
                continue

            candidatos.append(simbolo)

        # Sempre garantir BTC
        if "BTC_USDT" in candidatos:
            candidatos.remove("BTC_USDT")
            candidatos.insert(0, "BTC_USDT")

        print(
            f"Contratos Futures encontrados: {len(candidatos)}"
        )

        return candidatos

    except Exception as e:
        print("Erro ao obter contratos:", e)
        estado["erros"] += 1
        estado["ultimo_erro"] = str(e)

        return []


# ============================================================
# TICKER FUTURES
# ============================================================

def obter_tickers():
    """
    A MEXC retorna os tickers Futures.
    Uma única chamada traz os dados de vários contratos.
    """

    url = f"{MEXC_BASE}/api/v1/contract/ticker"

    dados = get_json(url)

    if not isinstance(dados, dict):
        raise ValueError("Ticker Futures inválido")

    lista = dados.get("data", [])

    resultado = {}

    for item in lista:

        simbolo = item.get("symbol")

        if not simbolo:
            continue

        try:
            preco = float(item.get("lastPrice", 0))

            if preco <= 0:
                continue

            volume = float(
                item.get("amount24", 0)
                or item.get("volume24", 0)
                or 0
            )

            oi = float(
                item.get("holdVol", 0)
                or 0
            )

            resultado[simbolo] = {
                "preco": preco,
                "volume": volume,
                "oi": oi,
                "timestamp": time.time()
            }

        except Exception:
            continue

    return resultado


# ============================================================
# HISTÓRICO
# ============================================================

def atualizar_historico(tickers):

    agora = time.time()

    for simbolo in contratos_monitorados:

        dados = tickers.get(simbolo)

        if not dados:
            continue

        preco = dados["preco"]
        volume = dados["volume"]
        oi = dados["oi"]

        historico[simbolo].append({
            "t": agora,
            "p": preco,
            "v": volume,
            "oi": oi
        })


# ============================================================
# VARIAÇÃO
# ============================================================

def variacao(hist, segundos):

    if not hist:
        return 0.0

    agora = hist[-1]["t"]
    preco_atual = hist[-1]["p"]

    alvo = agora - segundos

    referencia = None

    # Procura o ponto mais próximo do tempo desejado
    for item in reversed(hist):

        if item["t"] <= alvo:
            referencia = item
            break

    if referencia is None:
        return 0.0

    preco_anterior = referencia["p"]

    if preco_anterior <= 0:
        return 0.0

    return ((preco_atual / preco_anterior) - 1) * 100


# ============================================================
# EMA
# ============================================================

def calcular_ema(valores, periodo):

    if len(valores) < periodo:
        return None

    k = 2 / (periodo + 1)

    ema = sum(valores[:periodo]) / periodo

    for preco in valores[periodo:]:
        ema = (
            preco * k
            + ema * (1 - k)
        )

    return ema


def obter_emas(hist):

    if not hist:
        return None, None, None

    precos = [x["p"] for x in hist]

    ema9 = calcular_ema(precos, 9)
    ema21 = calcular_ema(precos, 21)
    ema50 = calcular_ema(precos, 50)

    return ema9, ema21, ema50


# ============================================================
# CONTEXTO BTC
# ============================================================

def obter_contexto_btc(tickers):

    btc_hist = historico.get("BTC_USDT")

    if not btc_hist or len(btc_hist) < 3:
        return {
            "r15": 0,
            "r30": 0,
            "r60": 0,
            "preco": 0
        }

    return {
        "r15": variacao(btc_hist, 15),
        "r30": variacao(btc_hist, 30),
        "r60": variacao(btc_hist, 60),
        "preco": btc_hist[-1]["p"]
    }


# ============================================================
# FORÇA RELATIVA
# ============================================================

def forca_relativa(m15, m30, m60, btc):

    rel15 = m15 - btc["r15"]
    rel30 = m30 - btc["r30"]
    rel60 = m60 - btc["r60"]

    return (
        rel15 * 0.30
        + rel30 * 0.30
        + rel60 * 0.40
    )


# ============================================================
# SCORE
# ============================================================

def calcular_score(
    m15,
    m30,
    m60,
    btc,
    ema9,
    ema21,
    ema50,
    oi_atual,
    oi_anterior
):

    score = 0

    # --------------------------------------------------------
    # MOVIMENTO
    # --------------------------------------------------------

    if m15 >= 0.35:
        score += 15

    if m15 >= 0.60:
        score += 8

    if m15 >= 1.0:
        score += 7

    if m30 >= 0.60:
        score += 12

    if m30 >= 1.0:
        score += 8

    if m60 >= 0.80:
        score += 10

    if m60 >= 2.0:
        score += 10

    if m60 >= 5.0:
        score += 15

    # --------------------------------------------------------
    # ACELERAÇÃO
    # --------------------------------------------------------

    aceleracao = (
        (m15 * 0.55)
        + (m30 * 0.30)
        + (m60 * 0.15)
    )

    if aceleracao >= 0.40:
        score += 5

    if aceleracao >= 0.80:
        score += 5

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if ema9 and ema21 and ema50:

        if ema9 > ema21:
            score += 8

        if ema21 > ema50:
            score += 7

        if ema9 > ema21 > ema50:
            score += 5

    # --------------------------------------------------------
    # BTC
    # --------------------------------------------------------

    if btc["r15"] >= -0.10:
        score += 4

    if btc["r30"] >= -0.20:
        score += 4

    if btc["r60"] >= -0.40:
        score += 4

    # --------------------------------------------------------
    # FORÇA RELATIVA
    # --------------------------------------------------------

    rel = forca_relativa(
        m15,
        m30,
        m60,
        btc
    )

    if rel > 0.30:
        score += 5

    if rel > 0.70:
        score += 5

    if rel > 1.50:
        score += 5

    # --------------------------------------------------------
    # OPEN INTEREST
    # --------------------------------------------------------

    if oi_atual > 0 and oi_anterior > 0:

        variacao_oi = (
            (oi_atual / oi_anterior) - 1
        ) * 100

        if variacao_oi > 0.5:
            score += 3

        if variacao_oi > 2:
            score += 5

    return min(score, 100)


# ============================================================
# CLASSIFICAÇÃO
# ============================================================

def classificar_alerta(m60, score):

    if m60 >= PUMP_60S:
        return "🔴 PUMP CONFIRMADO"

    if score >= SCORE_FORTE or m60 >= 2.5:
        return "🟠 MOVIMENTO MUITO FORTE"

    return "🟡 ACELERAÇÃO FORTE"


# ============================================================
# CANDIDATOS
# ============================================================

def analisar_mercado(tickers):

    btc = obter_contexto_btc(tickers)

    candidatos = []

    for simbolo in contratos_monitorados:

        if simbolo == "BTC_USDT":
            continue

        hist = historico.get(simbolo)

        if not hist or len(hist) < 15:
            continue

        m15 = variacao(hist, 15)
        m30 = variacao(hist, 30)
        m60 = variacao(hist, 60)

        # Não queremos moedas claramente caindo
        if m15 < -0.30:
            continue

        if m60 < -0.50:
            continue

        # Não alertar enquanto BTC estiver em queda muito forte
        if btc["r15"] <= BTC_MAX_QUEDA_15S:
            continue

        if btc["r60"] <= BTC_MAX_QUEDA_60S:
            continue

        ema9, ema21, ema50 = obter_emas(hist)

        oi_atual = hist[-1]["oi"]

        oi_anterior = hist[0]["oi"]

        score = calcular_score(
            m15,
            m30,
            m60,
            btc,
            ema9,
            ema21,
            ema50,
            oi_atual,
            oi_anterior
        )

        # ----------------------------------------------------
        # REGRAS DE ENTRADA
        # ----------------------------------------------------

        pump_confirmado = m60 >= PUMP_60S

        aceleracao = (
            m15 >= MIN_15S
            and m30 >= MIN_30S
            and m60 >= MIN_60S
        )

        movimento_forte = (
            m15 >= 0.60
            and m30 >= 1.0
        )

        if not (
            pump_confirmado
            or aceleracao
            or movimento_forte
        ):
            continue

        if not pump_confirmado and score < SCORE_MINIMO:
            continue

        # ----------------------------------------------------
        # COOLDOWN
        # ----------------------------------------------------

        agora = time.time()

        ultimo = ultimo_alerta_moeda.get(simbolo, 0)

        if agora - ultimo < COOLDOWN_MOEDA:
            continue

        # ----------------------------------------------------
        # FORÇA RELATIVA
        # ----------------------------------------------------

        rel = forca_relativa(
            m15,
            m30,
            m60,
            btc
        )

        candidatos.append({
            "simbolo": simbolo,
            "m15": m15,
            "m30": m30,
            "m60": m60,
            "score": score,
            "rel": rel,
            "ema9": ema9,
            "ema21": ema21,
            "ema50": ema50,
            "btc15": btc["r15"],
            "btc60": btc["r60"],
            "oi": oi_atual,
            "pump": pump_confirmado
        })

    # --------------------------------------------------------
    # ORDENAÇÃO
    # --------------------------------------------------------

    candidatos.sort(
        key=lambda x: (
            x["pump"],
            x["score"],
            x["m60"],
            x["m30"],
            x["rel"]
        ),
        reverse=True
    )

    return candidatos


# ============================================================
# ALERTA
# ============================================================

def montar_alerta(c):

    tipo = classificar_alerta(
        c["m60"],
        c["score"]
    )

    simbolo = c["simbolo"]

    preco = 0

    hist = historico.get(simbolo)

    if hist:
        preco = hist[-1]["p"]

    mensagem = (
        f"{tipo}\n\n"
        f"<b>{simbolo}</b>\n"
        f"Preço: <code>{preco:.8g}</code>\n\n"

        f"⚡ 15s: <b>{c['m15']:+.2f}%</b>\n"
        f"🚀 30s: <b>{c['m30']:+.2f}%</b>\n"
        f"🔥 60s: <b>{c['m60']:+.2f}%</b>\n\n"

        f"🏆 Score: <b>{c['score']}/100</b>\n"
        f"💪 Força vs BTC: <b>{c['rel']:+.2f}</b>\n\n"

        f"📊 BTC 15s: {c['btc15']:+.2f}%\n"
        f"📊 BTC 60s: {c['btc60']:+.2f}%\n\n"
    )

    if c["ema9"] and c["ema21"] and c["ema50"]:

        mensagem += (
            f"EMA 9: {c['ema9']:.8g}\n"
            f"EMA 21: {c['ema21']:.8g}\n"
            f"EMA 50: {c['ema50']:.8g}\n\n"
        )

    mensagem += (
        "⚠️ Radar de movimento — "
        "não é garantia de continuação."
    )

    return mensagem


def tentar_alertar(candidato):

    global ultimo_alerta_global

    agora = time.time()

    # --------------------------------------------------------
    # LIMITE GLOBAL: 1 ALERTA/MINUTO
    # --------------------------------------------------------

    if agora - ultimo_alerta_global < ALERTA_INTERVALO:
        return False

    mensagem = montar_alerta(candidato)

    sucesso = enviar_telegram(mensagem)

    if not sucesso:
        return False

    simbolo = candidato["simbolo"]

    ultimo_alerta_global = agora
    ultimo_alerta_moeda[simbolo] = agora

    estado["alertas"] += 1

    print(
        f"🚨 ALERTA ENVIADO | "
        f"{simbolo} | "
        f"score={candidato['score']} | "
        f"60s={candidato['m60']:+.2f}%"
    )

    return True


# ============================================================
# LOG
# ============================================================

def mostrar_resumo(candidatos):

    if not candidatos:
        print("Nenhum sinal qualificado neste ciclo.")
        return

    melhor = candidatos[0]

    print(
        "🏆 MELHOR CANDIDATO | "
        f"{melhor['simbolo']} | "
        f"score={melhor['score']} | "
        f"15s={melhor['m15']:+.2f}% | "
        f"30s={melhor['m30']:+.2f}% | "
        f"60s={melhor['m60']:+.2f}%"
    )


# ============================================================
# SELEÇÃO DE LIQUIDEZ
# ============================================================

def selecionar_contratos(tickers):

    candidatos = []

    for simbolo, dados in tickers.items():

        if not simbolo.endswith("USDT"):
            continue

        if dados["preco"] <= 0:
            continue

        volume = dados["volume"]

        candidatos.append(
            (simbolo, volume)
        )

    candidatos.sort(
        key=lambda x: x[1],
        reverse=True
    )

    selecionados = [
        x[0]
        for x in candidatos[:MAX_CONTRATOS]
    ]

    # BTC sempre presente
    if "BTC_USDT" in tickers:

        if "BTC_USDT" in selecionados:
            selecionados.remove("BTC_USDT")

        selecionados.insert(0, "BTC_USDT")

    return selecionados[:MAX_CONTRATOS]


# ============================================================
# LOOP PRINCIPAL
# ============================================================

def monitorar():

    global contratos_monitorados

    print("=" * 60)
    print("🚀 PUMP HUNTER V4 INICIANDO")
    print("=" * 60)

    print(
        f"Intervalo: {INTERVALO_SCAN}s"
    )

    print(
        f"Máximo contratos: {MAX_CONTRATOS}"
    )

    print(
        f"Pump confirmado: +{PUMP_60S}% / 60s"
    )

    print(
        "Máximo alertas: 1/min"
    )

    print("=" * 60)

    ultimo_refresh_contratos = 0

    while rodando:

        inicio_ciclo = time.time()

        try:

            # ------------------------------------------------
            # REFRESH DE CONTRATOS
            # ------------------------------------------------

            if (
                not contratos_monitorados
                or time.time() - ultimo_refresh_contratos > 15 * 60
            ):

                contratos = obter_contratos()

                if contratos:
                    contratos_monitorados = contratos

                ultimo_refresh_contratos = time.time()

            # ------------------------------------------------
            # TICKERS
            # ------------------------------------------------

            tickers = obter_tickers()

            estado["ultimo_sucesso_mexc"] = time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            # ------------------------------------------------
            # SELEÇÃO POR LIQUIDEZ
            # ------------------------------------------------

            selecionados = selecionar_contratos(
                tickers
            )

            if selecionados:

                contratos_monitorados = selecionados

            estado["contratos"] = len(
                contratos_monitorados
            )

            # ------------------------------------------------
            # HISTÓRICO
            # ------------------------------------------------

            atualizar_historico(
                tickers
            )

            # ------------------------------------------------
            # ANÁLISE
            # ------------------------------------------------

            candidatos = analisar_mercado(
                tickers
            )

            mostrar_resumo(
                candidatos
            )

            # ------------------------------------------------
            # ALERTA
            # ------------------------------------------------

            if candidatos:

                tentar_alertar(
                    candidatos[0]
                )

            estado["scans"] += 1
            estado["ultimo_scan"] = time.time()

            # ------------------------------------------------
            # LOG PERIÓDICO
            # ------------------------------------------------

            if estado["scans"] % 12 == 0:

                print(
                    f"❤️ V4 ONLINE | "
                    f"scans={estado['scans']} | "
                    f"contratos={len(contratos_monitorados)} | "
                    f"alertas={estado['alertas']} | "
                    f"erros={estado['erros']}"
                )

        except Exception as e:

            estado["erros"] += 1
            estado["ultimo_erro"] = str(e)

            print("=" * 60)
            print("⚠️ ERRO NO CICLO")
            print(str(e))
            print("=" * 60)

            traceback.print_exc()

            # Nunca deixar uma exceção matar o monitor
            time.sleep(3)

        # ----------------------------------------------------
        # CONTROLE DO INTERVALO
        # ----------------------------------------------------

        duracao = time.time() - inicio_ciclo

        espera = max(
            1,
            INTERVALO_SCAN - duracao
        )

        time.sleep(espera)


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(signum, frame):

    global rodando

    print(
        f"🛑 Sinal {signum} recebido. "
        "Encerrando V4..."
    )

    rodando = False


signal.signal(
    signal.SIGTERM,
    shutdown_handler
)

signal.signal(
    signal.SIGINT,
    shutdown_handler
)


# ============================================================
# WATCHDOG
# ============================================================

def watchdog():

    print("🛡️ Watchdog iniciado.")

    ultimo_scan_conhecido = 0

    while rodando:

        time.sleep(30)

        ultimo = estado["ultimo_scan"]

        if ultimo is None:
            continue

        # Se o loop ficar mais de 2 minutos sem registrar scan,
        # avisamos no log.
        if ultimo != ultimo_scan_conhecido:

            ultimo_scan_conhecido = ultimo

        idade = time.time() - ultimo

        if idade > 120:

            print(
                "🚨 WATCHDOG: "
                f"sem scan há {idade:.0f}s"
            )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    servidor = threading.Thread(
        target=iniciar_servidor,
        daemon=True
    )

    servidor.start()

    watchdog_thread = threading.Thread(
        target=watchdog,
        daemon=True
    )

    watchdog_thread.start()

    monitorar()

    print("V4 encerrado.")
