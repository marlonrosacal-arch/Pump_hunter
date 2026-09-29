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

MAX_CONTRATOS = 100

# Coleta a cada 5 segundos
INTERVALO = 5

# Atualiza a seleção de contratos
ATUALIZAR_CONTRATOS = 15 * 60

# Histórico
HISTORICO_SEGUNDOS = 130


# ============================================================
# FILTROS DE QUALIDADE
# ============================================================

# Movimento mínimo para entrar no ranking
MIN_30S = 0.30
MIN_60S = 0.45

# Movimento máximo aceitável em 15s
# Evita alguns spikes absurdos
MAX_15S = 5.0

# Score mínimo para permitir alerta
SCORE_MINIMO = 55


# ============================================================
# CONTROLE DE ALERTAS
# ============================================================

# No máximo 1 alerta por minuto
INTERVALO_ALERTAS = 60

# A mesma moeda só pode voltar depois deste período
COOLDOWN_MOEDA = 8 * 60


# ============================================================
# ESTRUTURAS
# ============================================================

app = Flask(__name__)

historico = defaultdict(
    lambda: deque(maxlen=150)
)

contratos = []

ultima_atualizacao_contratos = 0

ultimo_alerta_global = 0

ultimo_alerta_moeda = {}

melhor_candidato = None

rodando = True


# ============================================================
# TELEGRAM
# ============================================================

def enviar(mensagem):

    if not BOT_TOKEN or not CHAT_ID:

        print(
            "ERRO: BOT_TOKEN ou CHAT_ID não configurado."
        )

        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

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

            print(
                "Erro Telegram:",
                resposta.text
            )

            return False

        return True

    except Exception as e:

        print(
            "Erro enviando Telegram:",
            e
        )

        return False


# ============================================================
# CONTRATOS FUTURES
# ============================================================

def carregar_contratos():

    global contratos

    try:

        url = (
            f"{MEXC_BASE}"
            f"/api/v1/contract/detail"
        )

        resposta = requests.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        lista = dados.get(
            "data",
            []
        )

        novos = []

        for item in lista:

            symbol = item.get(
                "symbol",
                ""
            )

            quote = item.get(
                "quoteCoin"
            )

            settle = item.get(
                "settleCoin"
            )

            state = item.get(
                "state"
            )

            # Apenas USDT perpétuo ativo
            if quote != "USDT":
                continue

            if settle != "USDT":
                continue

            if state != 0:
                continue

            if not symbol.endswith(
                "_USDT"
            ):
                continue

            novos.append(symbol)

        if novos:

            contratos = novos

        print(
            f"[CONTRATOS] "
            f"{len(contratos)} encontrados."
        )

        return contratos

    except Exception as e:

        print(
            "Erro carregando contratos:",
            e
        )

        return contratos


# ============================================================
# TICKERS
# ============================================================

def obter_tickers():

    try:

        url = (
            f"{MEXC_BASE}"
            f"/api/v1/contract/ticker"
        )

        resposta = requests.get(
            url,
            timeout=15
        )

        resposta.raise_for_status()

        dados = resposta.json()

        resultado = dados.get(
            "data"
        )

        if resultado is None:
            return []

        if isinstance(
            resultado,
            list
        ):
            return resultado

        if isinstance(
            resultado,
            dict
        ):

            if "resultList" in resultado:

                return resultado[
                    "resultList"
                ]

            return [resultado]

        return []

    except Exception as e:

        print(
            "Erro obtendo tickers:",
            e
        )

        return []


# ============================================================
# SELEÇÃO DOS CONTRATOS
# ============================================================

def selecionar_contratos():

    global contratos

    tickers = obter_tickers()

    if not tickers:
        return

    conjunto = set(
        contratos
    )

    candidatos = []

    for ticker in tickers:

        symbol = ticker.get(
            "symbol"
        )

        if symbol not in conjunto:
            continue

        try:

            preco = float(
                ticker.get(
                    "lastPrice",
                    0
                )
            )

            volume = float(
                ticker.get(
                    "volume24",
                    0
                )
            )

            variacao = float(
                ticker.get(
                    "riseFallRate",
                    0
                )
            ) * 100

            if preco <= 0:
                continue

            if volume <= 0:
                continue

            # Evita moedas já completamente esticadas
            if variacao < -30:
                continue

            if variacao > 30:
                continue

            volatilidade = abs(
                variacao
            )

            score = (
                (volume ** 0.5)
                *
                (
                    1
                    +
                    volatilidade / 10
                )
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
        reverse=True
    )

    selecionados = [
        item[1]
        for item in candidatos[
            :MAX_CONTRATOS
        ]
    ]

    if selecionados:

        contratos = selecionados

        print(
            f"[SELEÇÃO] "
            f"{len(contratos)} contratos."
        )


# ============================================================
# HISTÓRICO
# ============================================================

def salvar_preco(
    symbol,
    preco,
    volume,
    hold_vol
):

    agora = time.time()

    historico[
        symbol
    ].append(
        {
            "time": agora,
            "price": preco,
            "volume": volume,
            "hold": hold_vol
        }
    )


# ============================================================
# PREÇO ANTERIOR
# ============================================================

def preco_anterior(
    symbol,
    segundos
):

    dados = historico.get(
        symbol
    )

    if not dados:
        return None

    alvo = (
        time.time()
        - segundos
    )

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

def variacao(
    symbol,
    segundos
):

    dados = historico.get(
        symbol
    )

    if not dados:
        return None

    atual = dados[-1]["price"]

    anterior = preco_anterior(
        symbol,
        segundos
    )

    if anterior is None:
        return None

    if anterior <= 0:
        return None

    return (
        (
            atual - anterior
        )
        /
        anterior
    ) * 100


# ============================================================
# SCORE
# ============================================================

def calcular_score(
    symbol
):

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

    if v15 is None:
        return None

    if v30 is None:
        return None

    if v60 is None:
        return None

    # ----------------------------------------
    # FILTROS
    # ----------------------------------------

    if v30 < MIN_30S:
        return None

    if v60 < MIN_60S:
        return None

    if v15 < 0:
        return None

    if v15 > MAX_15S:
        return None

    # ----------------------------------------
    # SCORE DE PREÇO
    # ----------------------------------------

    score_15 = min(
        v15 / 0.50,
        1
    ) * 25

    score_30 = min(
        v30 / 1.00,
        1
    ) * 30

    score_60 = min(
        v60 / 1.50,
        1
    ) * 25

    # ----------------------------------------
    # ACELERAÇÃO
    # ----------------------------------------

    aceleracao = (
        v30 - v60 / 2
    )

    score_aceleracao = min(
        max(
            aceleracao,
            0
        ) / 0.60,
        1
    ) * 10

    # ----------------------------------------
    # CONSISTÊNCIA
    # ----------------------------------------

    consistencia = 0

    if v15 > 0:
        consistencia += 3

    if v30 > v15 * 0.8:
        consistencia += 3

    if v60 > v30:
        consistencia += 4

    score_consistencia = consistencia

    score = (
        score_15
        +
        score_30
        +
        score_60
        +
        score_aceleracao
        +
        score_consistencia
    )

    return {
        "score": min(
            score,
            100
        ),
        "v15": v15,
        "v30": v30,
        "v60": v60
    }


# ============================================================
# ANALISAR CANDIDATO
# ============================================================

def analisar_candidato(
    symbol
):

    global melhor_candidato

    resultado = calcular_score(
        symbol
    )

    if resultado is None:
        return

    score = resultado[
        "score"
    ]

    if score < SCORE_MINIMO:
        return

    agora = time.time()

    # Cooldown individual
    ultimo = ultimo_alerta_moeda.get(
        symbol,
        0
    )

    if (
        agora - ultimo
        <
        COOLDOWN_MOEDA
    ):

        return

    dados = historico[
        symbol
    ]

    if not dados:
        return

    atual = dados[-1]

    candidato = {
        "symbol": symbol,
        "score": score,
        "v15": resultado["v15"],
        "v30": resultado["v30"],
        "v60": resultado["v60"],
        "price": atual["price"],
        "volume": atual["volume"],
        "hold": atual["hold"],
        "time": agora
    }

    # Guarda somente o melhor
    if (
        melhor_candidato is None
        or
        score
        >
        melhor_candidato["score"]
    ):

        melhor_candidato = candidato

        print(
            f"[MELHOR] "
            f"{symbol} "
            f"score={score:.1f} "
            f"30s={resultado['v30']:.2f}% "
            f"60s={resultado['v60']:.2f}%"
        )


# ============================================================
# FORMATAÇÃO
# ============================================================

def formatar_preco(
    preco
):

    if preco >= 1000:

        return f"{preco:,.2f}"

    if preco >= 1:

        return f"{preco:.4f}"

    if preco >= 0.01:

        return f"{preco:.6f}"

    return f"{preco:.10f}"


def formatar_volume(
    volume
):

    if volume >= 1_000_000_000:

        return (
            f"${volume / 1_000_000_000:.2f}B"
        )

    if volume >= 1_000_000:

        return (
            f"${volume / 1_000_000:.2f}M"
        )

    if volume >= 1_000:

        return (
            f"${volume / 1_000:.2f}K"
        )

    return f"${volume:.0f}"


# ============================================================
# CLASSIFICAÇÃO
# ============================================================

def nivel_alerta(
    candidato
):

    v30 = candidato["v30"]
    v60 = candidato["v60"]
    score = candidato["score"]

    if (
        score >= 85
        and
        v30 >= 0.80
        and
        v60 >= 1.00
    ):

        return (
            "🔴",
            "MOVIMENTO MUITO FORTE"
        )

    if (
        score >= 70
        and
        v30 >= 0.50
        and
        v60 >= 0.70
    ):

        return (
            "🟠",
            "ACELERAÇÃO FORTE"
        )

    return (
        "🟡",
        "INÍCIO DE MOVIMENTO"
    )


# ============================================================
# ENVIAR O MELHOR ALERTA
# ============================================================

def enviar_melhor_alerta():

    global melhor_candidato
    global ultimo_alerta_global

    if melhor_candidato is None:

        return

    agora = time.time()

    # Nunca mais de 1 por minuto
    if (
        agora - ultimo_alerta_global
        <
        INTERVALO_ALERTAS
    ):

        return

    candidato = melhor_candidato

    # Limpa antes do envio
    melhor_candidato = None

    symbol = candidato[
        "symbol"
    ]

    score = candidato[
        "score"
    ]

    v15 = candidato[
        "v15"
    ]

    v30 = candidato[
        "v30"
    ]

    v60 = candidato[
        "v60"
    ]

    preco = candidato[
        "price"
    ]

    volume = candidato[
        "volume"
    ]

    hold = candidato[
        "hold"
    ]

    emoji, titulo = nivel_alerta(
        candidato
    )

    mensagem = (
        f"{emoji} <b>{titulo}</b>\n\n"

        f"🚀 <b>{symbol}</b>\n"

        f"⭐ Score: "
        f"<b>{score:.0f}/100</b>\n\n"

        f"💰 Preço: "
        f"<b>{formatar_preco(preco)}</b>\n\n"

        f"⚡ 15s: "
        f"<b>+{v15:.2f}%</b>\n"

        f"⚡ 30s: "
        f"<b>+{v30:.2f}%</b>\n"

        f"⚡ 60s: "
        f"<b>+{v60:.2f}%</b>\n\n"

        f"📊 Volume 24h: "
        f"<b>{formatar_volume(volume)}</b>\n"

        f"📈 Open Interest: "
        f"<b>{hold:,.0f}</b>\n\n"

        f"🏆 <b>Melhor sinal "
        f"detectado no último minuto.</b>\n\n"

        f"⚠️ <i>O score mede força do "
        f"movimento observado; não prevê "
        f"que o preço continuará subindo.</i>"
    )

    if enviar(mensagem):

        ultimo_alerta_global = agora

        ultimo_alerta_moeda[
            symbol
        ] = agora

        print(
            f"[ALERTA ENVIADO] "
            f"{symbol} "
            f"score={score:.1f}"
        )


# ============================================================
# VERIFICAR MERCADO
# ============================================================

def verificar():

    global ultima_atualizacao_contratos

    agora = time.time()

    # ----------------------------------------
    # Atualizar contratos
    # ----------------------------------------

    if (
        not contratos
        or
        agora
        -
        ultima_atualizacao_contratos
        >=
        ATUALIZAR_CONTRATOS
    ):

        carregar_contratos()

        selecionar_contratos()

        ultima_atualizacao_contratos = agora

    # ----------------------------------------
    # Tickers
    # ----------------------------------------

    tickers = obter_tickers()

    if not tickers:

        return

    ativos = set(
        contratos
    )

    for ticker in tickers:

        symbol = ticker.get(
            "symbol"
        )

        if symbol not in ativos:

            continue

        try:

            preco = float(
                ticker.get(
                    "lastPrice",
                    0
                )
            )

            volume = float(
                ticker.get(
                    "volume24",
                    0
                )
            )

            hold = float(
                ticker.get(
                    "holdVol",
                    0
                )
            )

            if preco <= 0:

                continue

            salvar_preco(
                symbol,
                preco,
                volume,
                hold
            )

            analisar_candidato(
                symbol
            )

        except Exception as e:

            print(
                f"Erro {symbol}:",
                e
            )


# ============================================================
# LOOP DO BOT
# ============================================================

def loop_bot():

    global melhor_candidato

    print("=" * 60)

    print(
        "MEXC FUTURES "
        "PUMP HUNTER - RANKING"
    )

    print("=" * 60)

    enviar(
        "🚀 <b>MEXC FUTURES "
        "PUMP HUNTER</b>\n\n"

        "🎯 Monitorando perpétuos USDT\n"

        "🔎 Até 100 contratos\n"

        "📊 Ranking de força 0-100\n"

        "⚡ Análise 15s / 30s / 60s\n\n"

        "🏆 Enviando somente o "
        "<b>melhor sinal por minuto</b>."
    )

    proximo_minuto = (
        time.time()
        +
        60
    )

    while rodando:

        inicio = time.time()

        try:

            verificar()

        except Exception as e:

            print(
                "Erro:",
                e
            )

        # ------------------------------------
        # A cada minuto escolhe o melhor
        # ------------------------------------

        agora = time.time()

        if agora >= proximo_minuto:

            enviar_melhor_alerta()

            proximo_minuto = (
                agora
                +
                60
            )

        duracao = (
            time.time()
            -
            inicio
        )

        espera = max(
            1,
            INTERVALO - duracao
        )

        time.sleep(
            espera
        )


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return """
    <html>

    <head>
        <title>
            MEXC Futures Pump Hunter
        </title>
    </head>

    <body>

        <h1>
            🚀 MEXC Futures Pump Hunter
        </h1>

        <p>
            Bot online.
        </p>

        <p>
            Monitorando perpétuos USDT.
        </p>

        <p>
            Ranking de sinais ativo.
        </p>

        <p>
            Máximo: 1 alerta por minuto.
        </p>

    </body>

    </html>
    """


@app.route("/status")
def status():

    return {
        "status": "online",
        "contratos_monitorados": len(
            contratos
        ),
        "historicos": len(
            historico
        ),
        "ultimo_alerta": (
            ultimo_alerta_global
        ),
        "melhor_candidato": (
            melhor_candidato[
                "symbol"
            ]
            if melhor_candidato
            else None
        )
    }


# ============================================================
# INICIALIZAÇÃO
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
