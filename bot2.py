import os
import time
import threading
from collections import defaultdict, deque

import requests
from flask import Flask

# ============================================================
# CONFIGURAÇÃO
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MEXC_BASE = "https://api.mexc.com"

# Quantidade de contratos monitorados
MAX_CONTRATOS = 100

# Intervalo entre as leituras
INTERVALO = 5

# Atualiza a seleção de contratos a cada 15 minutos
ATUALIZAR_CONTRATOS = 15 * 60

# Histórico mantido por aproximadamente 2 minutos
HISTORICO_SEGUNDOS = 130

# ============================================================
# LIMITES DOS ALERTAS
# ============================================================

# ALERTA AMARELO
PUMP_10S = 0.18

# ALERTA LARANJA
PUMP_20S = 0.30
PUMP_30S = 0.45

# ALERTA VERMELHO
PUMP_60S = 0.70

# Evita considerar movimentos absurdos como sinal normal
MAX_MOVIMENTO_10S = 5.0

# Tempo mínimo para voltar a alertar a mesma moeda
COOLDOWN = 8 * 60

# ============================================================
# ESTRUTURAS
# ============================================================

app = Flask(__name__)

historico = defaultdict(lambda: deque(maxlen=150))

ultimo_alerta = {}

nivel_anterior = {}

contratos = []

ultima_atualizacao_contratos = 0

rodando = True


# ============================================================
# TELEGRAM
# ============================================================

def enviar(mensagem):
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
        resposta = requests.post(
            url,
            json=dados,
            timeout=10
        )

        if resposta.status_code != 200:
            print("Erro Telegram:", resposta.text)
            return False

        return True

    except Exception as e:
        print("Erro enviando Telegram:", e)
        return False


# ============================================================
# PEGAR CONTRATOS PERPÉTUOS
# ============================================================

def carregar_contratos():
    global contratos

    try:
        url = f"{MEXC_BASE}/api/v1/contract/detail"

        resposta = requests.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        lista = dados.get("data", [])

        novos = []

        for item in lista:

            symbol = item.get("symbol", "")
            quote = item.get("quoteCoin")
            settle = item.get("settleCoin")
            state = item.get("state")

            # Apenas contratos USDT perpétuos ativos
            if quote != "USDT":
                continue

            if settle != "USDT":
                continue

            if state != 0:
                continue

            if not symbol.endswith("_USDT"):
                continue

            novos.append(symbol)

        contratos = novos

        print(
            f"[CONTRATOS] {len(contratos)} contratos perpétuos USDT encontrados."
        )

        return contratos

    except Exception as e:
        print("Erro carregando contratos:", e)

        return contratos


# ============================================================
# PEGAR TICKERS DOS FUTUROS
# ============================================================

def obter_tickers():

    try:

        url = f"{MEXC_BASE}/api/v1/contract/ticker"

        resposta = requests.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        resultado = dados.get("data")

        if resultado is None:
            return []

        # Dependendo da resposta da API,
        # data pode ser lista ou objeto.
        if isinstance(resultado, dict):

            # Caso venha uma estrutura paginada
            if "resultList" in resultado:
                return resultado["resultList"]

            return [resultado]

        if isinstance(resultado, list):
            return resultado

        return []

    except Exception as e:

        print("Erro obtendo tickers:", e)

        return []


# ============================================================
# SELECIONAR OS CONTRATOS MAIS ATIVOS
# ============================================================

def selecionar_contratos():

    global contratos

    tickers = obter_tickers()

    if not tickers:
        return

    candidatos = []

    conjunto_contratos = set(contratos)

    for ticker in tickers:

        symbol = ticker.get("symbol")

        if symbol not in conjunto_contratos:
            continue

        try:

            preco = float(ticker.get("lastPrice", 0))
            volume = float(ticker.get("volume24", 0))
            variacao = float(ticker.get("riseFallRate", 0)) * 100

            if preco <= 0:
                continue

            if volume <= 0:
                continue

            # Ignora moedas que já estejam em movimentos extremos
            # para deixar espaço para detectar novos movimentos.
            if variacao < -30 or variacao > 30:
                continue

            # Score de atividade:
            # volume maior = mais prioridade
            # volatilidade moderada = mais prioridade
            volatilidade = abs(variacao)

            score = (
                (volume ** 0.5) *
                (1 + volatilidade / 10)
            )

            candidatos.append(
                (
                    score,
                    symbol
                )
            )

        except Exception:
            continue

    candidatos.sort(
        reverse=True,
        key=lambda x: x[0]
    )

    novos_contratos = [
        item[1]
        for item in candidatos[:MAX_CONTRATOS]
    ]

    if novos_contratos:

        contratos = novos_contratos

        print(
            f"[SELEÇÃO] Monitorando {len(contratos)} contratos."
        )

        print(
            "[SELEÇÃO]",
            ", ".join(contratos[:20])
        )


# ============================================================
# HISTÓRICO
# ============================================================

def salvar_preco(symbol, preco, volume, hold_vol):

    agora = time.time()

    historico[symbol].append(
        {
            "time": agora,
            "price": preco,
            "volume": volume,
            "hold": hold_vol
        }
    )


# ============================================================
# PEGAR PREÇO DE X SEGUNDOS ATRÁS
# ============================================================

def preco_anterior(symbol, segundos):

    agora = time.time()

    dados = historico.get(symbol)

    if not dados:
        return None

    alvo = agora - segundos

    melhor = None

    for item in dados:

        if item["time"] <= alvo:
            melhor = item

        else:
            break

    if melhor:
        return melhor["price"]

    return None


# ============================================================
# VARIAÇÃO
# ============================================================

def calcular_variacao(symbol, segundos):

    atual = historico[symbol][-1]["price"]

    anterior = preco_anterior(
        symbol,
        segundos
    )

    if anterior is None:
        return None

    if anterior <= 0:
        return None

    return (
        (atual - anterior) /
        anterior
    ) * 100


# ============================================================
# FORMATAR NÚMEROS
# ============================================================

def formatar_preco(preco):

    if preco >= 1000:
        return f"{preco:,.2f}"

    if preco >= 1:
        return f"{preco:.4f}"

    if preco >= 0.01:
        return f"{preco:.6f}"

    return f"{preco:.10f}"


def formatar_volume(volume):

    if volume >= 1_000_000_000:
        return f"${volume / 1_000_000_000:.2f}B"

    if volume >= 1_000_000:
        return f"${volume / 1_000_000:.2f}M"

    if volume >= 1_000:
        return f"${volume / 1_000:.2f}K"

    return f"${volume:.0f}"


# ============================================================
# DETECTOR DE PUMP
# ============================================================

def analisar(symbol, ticker):

    v10 = calcular_variacao(symbol, 10)
    v20 = calcular_variacao(symbol, 20)
    v30 = calcular_variacao(symbol, 30)
    v60 = calcular_variacao(symbol, 60)

    if v10 is None:
        return

    if v20 is None:
        return

    if v30 is None:
        return

    if v60 is None:
        return

    # --------------------------------------------------------
    # SOMENTE MOVIMENTO DE ALTA
    # --------------------------------------------------------

    if v10 <= 0 and v20 <= 0 and v30 <= 0:
        nivel = 0

    else:

        # ----------------------------------------------------
        # NÍVEL 3 - FORTE
        # ----------------------------------------------------

        if (
            v60 >= PUMP_60S
            and v30 >= PUMP_30S
            and v20 >= PUMP_20S
        ):

            nivel = 3

        # ----------------------------------------------------
        # NÍVEL 2 - ACELERAÇÃO
        # ----------------------------------------------------

        elif (
            v30 >= PUMP_30S
            and v20 >= PUMP_20S
        ):

            nivel = 2

        # ----------------------------------------------------
        # NÍVEL 1 - INÍCIO
        # ----------------------------------------------------

        elif v10 >= PUMP_10S:

            nivel = 1

        else:

            nivel = 0

    # Movimento exagerado em poucos segundos
    # pode ser ruído/spike.
    if v10 > MAX_MOVIMENTO_10S:
        nivel = 0

    nivel_antigo = nivel_anterior.get(symbol, 0)

    nivel_anterior[symbol] = nivel

    if nivel <= 0:
        return

    # Só alerta quando sobe de nível
    # ou quando passou o cooldown.
    agora = time.time()

    ultimo = ultimo_alerta.get(symbol, 0)

    passou_cooldown = (
        agora - ultimo
    ) >= COOLDOWN

    subiu_de_nivel = nivel > nivel_antigo

    if not subiu_de_nivel and not passou_cooldown:
        return

    ultimo_alerta[symbol] = agora

    preco = historico[symbol][-1]["price"]

    volume = historico[symbol][-1]["volume"]

    hold = historico[symbol][-1]["hold"]

    if nivel == 1:

        titulo = "🟡 INÍCIO DE MOVIMENTO"

    elif nivel == 2:

        titulo = "🟠 ACELERAÇÃO FORTE"

    else:

        titulo = "🔴 MOVIMENTO MUITO FORTE"

    mensagem = (
        f"<b>{titulo}</b>\n\n"
        f"🚀 <b>{symbol}</b>\n"
        f"💰 Preço: <b>{formatar_preco(preco)}</b>\n\n"

        f"⚡ 10s: <b>+{v10:.2f}%</b>\n"
        f"⚡ 20s: <b>+{v20:.2f}%</b>\n"
        f"⚡ 30s: <b>+{v30:.2f}%</b>\n"
        f"⚡ 60s: <b>+{v60:.2f}%</b>\n\n"

        f"📊 Volume 24h: <b>{formatar_volume(volume)}</b>\n"
        f"📈 Open Interest: <b>{hold:,.0f}</b>\n\n"

        f"⚠️ <i>Sinal de aceleração de preço. "
        f"Não significa que o movimento continuará.</i>"
    )

    print(
        f"[ALERTA {nivel}] {symbol} "
        f"10s={v10:.2f}% "
        f"20s={v20:.2f}% "
        f"30s={v30:.2f}% "
        f"60s={v60:.2f}%"
    )

    enviar(mensagem)


# ============================================================
# VERIFICAR MERCADO
# ============================================================

def verificar():

    global ultima_atualizacao_contratos

    agora = time.time()

    # Atualiza a lista de contratos
    if (
        not contratos
        or agora - ultima_atualizacao_contratos
        >= ATUALIZAR_CONTRATOS
    ):

        carregar_contratos()

        selecionar_contratos()

        ultima_atualizacao_contratos = agora

    tickers = obter_tickers()

    if not tickers:
        return

    ativos = set(contratos)

    for ticker in tickers:

        symbol = ticker.get("symbol")

        if symbol not in ativos:
            continue

        try:

            preco = float(
                ticker.get("lastPrice", 0)
            )

            volume = float(
                ticker.get("volume24", 0)
            )

            hold_vol = float(
                ticker.get("holdVol", 0)
            )

            if preco <= 0:
                continue

            salvar_preco(
                symbol,
                preco,
                volume,
                hold_vol
            )

            analisar(
                symbol,
                ticker
            )

        except Exception as e:

            print(
                f"Erro processando {symbol}: {e}"
            )


# ============================================================
# LOOP PRINCIPAL
# ============================================================

def loop_bot():

    print("=" * 60)
    print("BOT 2 - MEXC FUTURES PUMP HUNTER")
    print("=" * 60)

    enviar(
        "🚀 <b>BOT MEXC FUTURES INICIADO</b>\n\n"
        "🎯 Monitorando contratos perpétuos USDT\n"
        "🔎 Procurando acelerações rápidas\n"
        "📊 Analisando preço + volume + open interest\n"
        "⚡ Janela de análise: 10s / 20s / 30s / 60s\n\n"
        "⏳ Construindo histórico..."
    )

    while rodando:

        inicio = time.time()

        try:

            verificar()

        except Exception as e:

            print(
                "Erro no loop:",
                e
            )

        duracao = time.time() - inicio

        espera = max(
            1,
            INTERVALO - duracao
        )

        time.sleep(espera)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return """
    <html>
    <head>
        <title>MEXC Futures Pump Hunter</title>
    </head>

    <body>

        <h1>🚀 MEXC Futures Pump Hunter</h1>

        <p>Bot online.</p>

        <p>
        Monitorando contratos perpétuos USDT.
        </p>

        <p>
        Detector: 10s / 20s / 30s / 60s.
        </p>

    </body>
    </html>
    """


@app.route("/status")
def status():

    return {
        "status": "online",
        "contratos_monitorados": len(contratos),
        "historicos": len(historico),
        "alertas_registrados": len(ultimo_alerta)
    }


# ============================================================
# INICIAR
# ============================================================

if __name__ == "__main__":

    thread = threading.Thread(
        target=loop_bot,
        daemon=True
    )

    thread.start()

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
