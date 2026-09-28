import os
import time
import threading
import requests
from collections import defaultdict, deque
from flask import Flask, jsonify

# ============================================================
# BOT 2 - MEXC PUMP HUNTER
# ============================================================

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÕES
# ============================================================

BOT_TOKEN = os.getenv("8523359888:AAFcISRXoEjnmXfPuaak5nZmT-N8o0mQ0ho")
CHAT_ID = os.getenv("1475544551")

# Quantidade de moedas monitoradas
NUM_MOEDAS = 50

# Consulta de preços
INTERVALO = 5

# Recria a seleção de moedas a cada 30 minutos
INTERVALO_RESELECAO = 1800

# Histórico mantido
HISTORICO_SEGUNDOS = 90

# ============================================================
# CRITÉRIOS DE DETECÇÃO
# ============================================================

# Movimento mínimo para gerar possível alerta
PUMP_15S = 0.40
PUMP_30S = 0.70
PUMP_60S = 1.00

# Movimento máximo em 15 segundos para evitar algumas
# situações extremamente anormais/spikes isolados
MAX_15S = 8.0

# Tempo mínimo entre alertas da mesma moeda
COOLDOWN = 600

# Depois de cair abaixo desse movimento, libera novo alerta
RESET_MOVIMENTO = 0.15

# ============================================================
# FILTROS PARA SELEÇÃO DAS MOEDAS
# ============================================================

# Volume mínimo aproximado em USDT nas últimas 24h
VOLUME_MINIMO = 500_000

# Evita moedas que já estejam disparando no momento da seleção
VAR_24H_MIN = -15
VAR_24H_MAX = 15

# ============================================================
# ENDPOINTS MEXC
# ============================================================

BASE_URL = "https://api.mexc.com/api/v3"

SESSION = requests.Session()

# ============================================================
# ESTADO DO BOT
# ============================================================

historico = defaultdict(
    lambda: deque()
)

ultimo_alerta = {}

moedas = []

ultima_selecao = 0

rodando = False

lock = threading.Lock()


# ============================================================
# TELEGRAM
# ============================================================

def enviar(mensagem):

    if not BOT_TOKEN:
        print("ERRO: BOT_TOKEN não configurado.")
        return False

    if not CHAT_ID:
        print("ERRO: CHAT_ID não configurado.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:

        resposta = SESSION.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": mensagem
            },
            timeout=10
        )

        if resposta.status_code != 200:

            print(
                "Erro Telegram:",
                resposta.status_code,
                resposta.text
            )

            return False

        return True

    except Exception as e:

        print(
            "Erro ao enviar Telegram:",
            e
        )

        return False


# ============================================================
# TESTE TELEGRAM
# ============================================================

def testar_telegram():

    mensagem = (
        "✅ BOT 2 MEXC INICIADO\n\n"
        "🔎 Pump Hunter ativo\n"
        "📡 Selecionando moedas automaticamente\n"
        "⚡ Procurando movimentos rápidos"
    )

    return enviar(mensagem)


# ============================================================
# BUSCA TICKER 24H
# ============================================================

def pegar_tickers_24h():

    url = f"{BASE_URL}/ticker/24hr"

    try:

        resposta = SESSION.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        if not isinstance(dados, list):

            print(
                "Resposta inesperada da MEXC:",
                dados
            )

            return []

        return dados

    except Exception as e:

        print(
            "Erro ao consultar ticker 24h:",
            e
        )

        return []


# ============================================================
# SELECIONA 50 MOEDAS
# ============================================================

def selecionar_moedas():

    global moedas
    global ultima_selecao

    print("\n====================================")
    print("SELECIONANDO MOEDAS")
    print("====================================")

    tickers = pegar_tickers_24h()

    if not tickers:

        print(
            "Não foi possível obter os tickers."
        )

        return False

    candidatos = []

    for item in tickers:

        try:

            symbol = item.get("symbol", "")

            if not symbol.endswith("USDT"):
                continue

            # Evita pares estranhos
            if symbol.endswith("USDCUSDT"):
                continue

            if symbol.endswith("TUSDUSDT"):
                continue

            if symbol.endswith("USDPUSDT"):
                continue

            # Evita tokens alavancados
            nomes_proibidos = [
                "5L",
                "5S",
                "3L",
                "3S",
                "2L",
                "2S"
            ]

            if any(
                symbol.endswith(x + "USDT")
                for x in nomes_proibidos
            ):
                continue

            volume = float(
                item.get(
                    "quoteVolume",
                    0
                )
            )

            variacao = float(
                item.get(
                    "priceChangePercent",
                    0
                )
            )

            preco = float(
                item.get(
                    "lastPrice",
                    0
                )
            )

            if preco <= 0:
                continue

            if volume < VOLUME_MINIMO:
                continue

            if variacao < VAR_24H_MIN:
                continue

            if variacao > VAR_24H_MAX:
                continue

            # ------------------------------------------------
            # SCORE
            # ------------------------------------------------
            #
            # Não é previsão de pump.
            #
            # É apenas uma forma de selecionar ativos
            # com volume + alguma volatilidade.
            #

            volume_score = min(
                volume / 5_000_000,
                10
            )

            volatilidade_score = min(
                abs(variacao),
                10
            )

            score = (
                volume_score * 0.65
                +
                volatilidade_score * 0.35
            )

            candidatos.append(
                (
                    score,
                    symbol,
                    volume,
                    variacao
                )
            )

        except Exception:
            continue

    # Ordena pelo score
    candidatos.sort(
        key=lambda x: x[0],
        reverse=True
    )

    selecionadas = candidatos[:NUM_MOEDAS]

    if not selecionadas:

        print(
            "Nenhuma moeda passou pelos filtros."
        )

        return False

    moedas = [
        item[1]
        for item in selecionadas
    ]

    ultima_selecao = time.time()

    print(
        f"\n{len(moedas)} moedas selecionadas:"
    )

    for i, item in enumerate(
        selecionadas,
        start=1
    ):

        score, symbol, volume, variacao = item

        print(
            f"{i:02d}. "
            f"{symbol:<18} "
            f"24h={variacao:+.2f}% "
            f"vol=${volume:,.0f}"
        )

    return True


# ============================================================
# PEGA PREÇOS ATUAIS
# ============================================================

def pegar_precos():

    url = f"{BASE_URL}/ticker/price"

    try:

        resposta = SESSION.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        mapa = {}

        for item in dados:

            try:

                symbol = item["symbol"]

                preco = float(
                    item["price"]
                )

                mapa[symbol] = preco

            except Exception:
                continue

        return mapa

    except Exception as e:

        print(
            "Erro ao consultar preços:",
            e
        )

        return {}


# ============================================================
# SALVA PREÇO
# ============================================================

def salvar_preco(
    moeda,
    preco
):

    agora = time.time()

    historico[moeda].append(
        (
            agora,
            preco
        )
    )

    limite = (
        agora -
        HISTORICO_SEGUNDOS
    )

    while (
        historico[moeda]
        and
        historico[moeda][0][0]
        < limite
    ):

        historico[moeda].popleft()


# ============================================================
# PEGA PREÇO DE X SEGUNDOS ATRÁS
# ============================================================

def pegar_preco_anterior(
    moeda,
    segundos
):

    agora = time.time()

    registros = historico[moeda]

    if not registros:
        return None

    alvo = agora - segundos

    melhor_preco = None
    menor_distancia = float("inf")

    for timestamp, preco in registros:

        distancia = abs(
            timestamp - alvo
        )

        if distancia < menor_distancia:

            menor_distancia = distancia
            melhor_preco = preco

    # Aceita somente se houver registro
    # razoavelmente próximo do momento desejado

    if menor_distancia > 7:
        return None

    return melhor_preco


# ============================================================
# CALCULA %
# ============================================================

def calcular_variacao(
    atual,
    anterior
):

    if not anterior:
        return None

    return (
        (atual - anterior)
        /
        anterior
    ) * 100


# ============================================================
# VERIFICA UMA MOEDA
# ============================================================

def analisar_moeda(
    moeda,
    preco
):

    salvar_preco(
        moeda,
        preco
    )

    p15 = pegar_preco_anterior(
        moeda,
        15
    )

    p30 = pegar_preco_anterior(
        moeda,
        30
    )

    p60 = pegar_preco_anterior(
        moeda,
        60
    )

    # Ainda não temos histórico suficiente
    if (
        p15 is None
        or
        p30 is None
        or
        p60 is None
    ):
        return

    v15 = calcular_variacao(
        preco,
        p15
    )

    v30 = calcular_variacao(
        preco,
        p30
    )

    v60 = calcular_variacao(
        preco,
        p60
    )

    if (
        v15 is None
        or
        v30 is None
        or
        v60 is None
    ):
        return

    agora = time.time()

    # ========================================================
    # DETECTOR
    # ========================================================

    movimento = (
        v15 >= PUMP_15S
        and
        v30 >= PUMP_30S
        and
        v60 >= PUMP_60S
        and
        v15 <= MAX_15S
    )

    # ========================================================
    # ALERTA
    # ========================================================

    if movimento:

        ultimo = ultimo_alerta.get(
            moeda,
            0
        )

        if (
            agora - ultimo
            >= COOLDOWN
        ):

            mensagem = (
                "🚨 MOVIMENTO FORTE DETECTADO\n\n"

                f"💰 {moeda}\n"
                f"💵 Preço: {preco:.10g}\n\n"

                f"⚡ 15 segundos: "
                f"+{v15:.2f}%\n"

                f"📈 30 segundos: "
                f"+{v30:.2f}%\n"

                f"🚀 60 segundos: "
                f"+{v60:.2f}%\n\n"

                "📡 MEXC Spot\n"
                "⚠️ Sinal baseado em movimento "
                "de curto prazo."
            )

            enviado = enviar(
                mensagem
            )

            if enviado:

                ultimo_alerta[moeda] = agora

                print(
                    "\n🚨 ALERTA:",
                    moeda,
                    f"+{v15:.2f}%",
                    f"+{v30:.2f}%",
                    f"+{v60:.2f}%"
                )

    # ========================================================
    # LOG
    # ========================================================

    print(
        f"{moeda} | "
        f"15s {v15:+.2f}% | "
        f"30s {v30:+.2f}% | "
        f"60s {v60:+.2f}%"
    )


# ============================================================
# CICLO DE MONITORAMENTO
# ============================================================

def verificar():

    global ultima_selecao

    # --------------------------------------------------------
    # Atualiza seleção periodicamente
    # --------------------------------------------------------

    if (
        not moedas
        or
        time.time() - ultima_selecao
        >= INTERVALO_RESELECAO
    ):

        sucesso = selecionar_moedas()

        if not sucesso:

            return

        # Quando troca a lista,
        # limpa históricos antigos.

        historico.clear()

        ultimo_alerta.clear()

    # --------------------------------------------------------
    # Busca preços
    # --------------------------------------------------------

    precos = pegar_precos()

    if not precos:

        print(
            "Nenhum preço recebido."
        )

        return

    encontrados = 0

    # --------------------------------------------------------
    # Analisa as 50
    # --------------------------------------------------------

    for moeda in moedas:

        preco = precos.get(
            moeda
        )

        if preco is None:

            print(
                f"Preço não encontrado: "
                f"{moeda}"
            )

            continue

        encontrados += 1

        try:

            analisar_moeda(
                moeda,
                preco
            )

        except Exception as e:

            print(
                f"Erro analisando "
                f"{moeda}: {e}"
            )

    print(
        f"Monitoradas: "
        f"{encontrados}/{len(moedas)}"
    )


# ============================================================
# LOOP PRINCIPAL
# ============================================================

def loop_bot():

    global rodando

    with lock:

        if rodando:
            return

        rodando = True

    print(
        "===================================="
    )

    print(
        "BOT 2 MEXC PUMP HUNTER"
    )

    print(
        "===================================="
    )

    # --------------------------------------------------------
    # Telegram
    # --------------------------------------------------------

    if not BOT_TOKEN:
        print(
            "⚠️ BOT_TOKEN não configurado!"
        )

    if not CHAT_ID:
        print(
            "⚠️ CHAT_ID não configurado!"
        )

    time.sleep(3)

    testar_telegram()

    # --------------------------------------------------------
    # Primeiro carregamento
    # --------------------------------------------------------

    selecionar_moedas()

    # --------------------------------------------------------
    # Loop
    # --------------------------------------------------------

    while True:

        inicio = time.time()

        try:

            verificar()

        except Exception as e:

            print(
                "ERRO NO LOOP:",
                e
            )

        duracao = (
            time.time() - inicio
        )

        espera = max(
            1,
            INTERVALO - duracao
        )

        time.sleep(
            espera
        )


# ============================================================
# WEB
# ============================================================

@app.route("/")
def home():

    return (
        "BOT 2 MEXC PUMP HUNTER ATIVO | "
        f"{len(moedas)} moedas"
    )


@app.route("/status")
def status():

    return jsonify({
        "bot": "MEXC Pump Hunter",
        "ativo": rodando,
        "moedas_monitoradas": len(moedas),
        "moedas": moedas,
        "intervalo": INTERVALO,
        "criterio": {
            "15s": PUMP_15S,
            "30s": PUMP_30S,
            "60s": PUMP_60S
        },
        "cooldown": COOLDOWN
    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    print(
        "Iniciando Bot 2..."
    )

    thread = threading.Thread(
        target=loop_bot,
        daemon=True
    )

    thread.start()

    porta = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=porta
    )
