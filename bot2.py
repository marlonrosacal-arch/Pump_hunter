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

# Endpoint oficial de Futures
MEXC_BASE = "https://contract.mexc.com"

MAX_CONTRATOS = 100

# Coleta de preço a cada 5 segundos
INTERVALO = 5

# Atualização da lista de contratos
ATUALIZAR_CONTRATOS = 15 * 60

# Histórico rápido
HISTORICO_SEGUNDOS = 130


# ============================================================
# FILTROS
# ============================================================

# Movimento mínimo
MIN_30S = 0.25
MIN_60S = 0.35

# Evita spikes absurdos
MAX_15S = 5.0

# Score mínimo
SCORE_MINIMO = 45


# ============================================================
# ALERTAS
# ============================================================

# Máximo de 1 alerta por minuto
INTERVALO_ALERTAS = 60

# Mesma moeda só pode voltar depois de 8 minutos
COOLDOWN_MOEDA = 8 * 60


# ============================================================
# EMA
# ============================================================

EMA_RAPIDA = 9
EMA_MEDIA = 21
EMA_LENTA = 50

# Precisamos de candles suficientes
MIN_CANDLES_EMA = 50


# ============================================================
# ESTRUTURAS
# ============================================================

app = Flask(__name__)

historico = defaultdict(
    lambda: deque(maxlen=150)
)

# Candles de 1 minuto
candles_1m = defaultdict(
    lambda: deque(maxlen=100)
)

contratos = []

ultima_atualizacao_contratos = 0

ultimo_alerta_global = 0

ultimo_alerta_moeda = {}

melhor_candidato = None

rodando = True

ultima_coleta_candle = 0


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
            "Erro Telegram:",
            e
        )

        return False


# ============================================================
# CONTRATOS
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
            f"{len(contratos)} contratos."
        )

        return contratos

    except Exception as e:

        print(
            "Erro contratos:",
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
            "Erro ticker:",
            e
        )

        return []


# ============================================================
# SELEÇÃO DOS CONTRATOS MAIS ATIVOS
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

            # Evita extremos
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
# HISTÓRICO DE TICKS
# ============================================================

def salvar_preco(
    symbol,
    preco,
    volume,
    hold
):

    historico[
        symbol
    ].append(
        {
            "time": time.time(),
            "price": preco,
            "volume": volume,
            "hold": hold
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
        -
        segundos
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
# CRIAR CANDLE DE 1 MINUTO
# ============================================================

def atualizar_candle_1m():

    agora = int(
        time.time()
    )

    minuto = (
        agora // 60
    ) * 60

    for symbol in list(
        historico.keys()
    ):

        dados = historico[
            symbol
        ]

        if not dados:
            continue

        preco = dados[-1][
            "price"
        ]

        # Se não existe candle atual
        if (
            not candles_1m[symbol]
            or
            candles_1m[symbol][-1][
                "time"
            ]
            != minuto
        ):

            candles_1m[
                symbol
            ].append(
                {
                    "time": minuto,
                    "close": preco
                }
            )

        else:

            # Atualiza fechamento do minuto
            candles_1m[
                symbol
            ][-1][
                "close"
            ] = preco


# ============================================================
# EMA
# ============================================================

def calcular_ema(
    valores,
    periodo
):

    if len(valores) < periodo:

        return None

    multiplicador = (
        2 /
        (periodo + 1)
    )

    ema = sum(
        valores[
            :periodo
        ]
    ) / periodo

    for preco in valores[
        periodo:
    ]:

        ema = (
            (
                preco - ema
            )
            *
            multiplicador
        ) + ema

    return ema


# ============================================================
# PEGAR EMAS
# ============================================================

def obter_emas(
    symbol
):

    candles = candles_1m.get(
        symbol
    )

    if not candles:
        return None

    fechamentos = [
        c["close"]
        for c in candles
    ]

    if len(fechamentos) < MIN_CANDLES_EMA:

        return None

    ema9 = calcular_ema(
        fechamentos,
        EMA_RAPIDA
    )

    ema21 = calcular_ema(
        fechamentos,
        EMA_MEDIA
    )

    ema50 = calcular_ema(
        fechamentos,
        EMA_LENTA
    )

    if (
        ema9 is None
        or
        ema21 is None
        or
        ema50 is None
    ):

        return None

    return {
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50
    }


# ============================================================
# PRÉ-CARREGAR EMAS DA MEXC
# ============================================================

def carregar_ema_historica():

    print(
        "[EMA] "
        "Carregando histórico inicial..."
    )

    # Fazemos em lotes pequenos para
    # respeitar o limite da API.
    lista = contratos[
        :MAX_CONTRATOS
    ]

    for i in range(
        0,
        len(lista),
        10
    ):

        lote = lista[
            i:i + 10
        ]

        for symbol in lote:

            try:

                url = (
                    f"{MEXC_BASE}"
                    f"/api/v1/contract/kline/"
                    f"{symbol}"
                )

                parametros = {
                    "interval": "Min1"
                }

                resposta = requests.get(
                    url,
                    params=parametros,
                    timeout=15
                )

                if resposta.status_code != 200:

                    continue

                dados = resposta.json()

                data = dados.get(
                    "data"
                )

                if not data:

                    continue

                tempos = data.get(
                    "time",
                    []
                )

                fechamentos = data.get(
                    "close",
                    []
                )

                if not fechamentos:

                    continue

                # Últimos 100 candles
                inicio = max(
                    0,
                    len(fechamentos) - 100
                )

                for j in range(
                    inicio,
                    len(fechamentos)
                ):

                    candles_1m[
                        symbol
                    ].append(
                        {
                            "time": int(
                                tempos[j]
                            ),
                            "close": float(
                                fechamentos[j]
                            )
                        }
                    )

            except Exception as e:

                print(
                    f"[EMA] "
                    f"Erro {symbol}: {e}"
                )

        # Pequena pausa
        time.sleep(
            0.7
        )

    print(
        "[EMA] "
        "Histórico carregado."
    )


# ============================================================
# BTC
# ============================================================

def obter_btc_variacoes():

    v30 = variacao(
        "BTC_USDT",
        30
    )

    v60 = variacao(
        "BTC_USDT",
        60
    )

    return v30, v60


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

    # ----------------------------
    # Filtros básicos
    # ----------------------------

    if v30 < MIN_30S:
        return None

    if v60 < MIN_60S:
        return None

    if v15 < 0:
        return None

    if v15 > MAX_15S:
        return None

    # ----------------------------
    # BTC
    # ----------------------------

    btc30, btc60 = (
        obter_btc_variacoes()
    )

    if btc30 is None:
        btc30 = 0

    if btc60 is None:
        btc60 = 0

    # ----------------------------
    # Força relativa
    # ----------------------------

    forca30 = (
        v30 - btc30
    )

    forca60 = (
        v60 - btc60
    )

    # ----------------------------
    # EMAs
    # ----------------------------

    emas = obter_emas(
        symbol
    )

    if emas is None:

        # Sem histórico suficiente:
        # ainda pode participar,
        # mas perde pontos.
        ema_bonus = 0
        tendencia = "SEM_HIST"

    else:

        ema9 = emas["ema9"]
        ema21 = emas["ema21"]
        ema50 = emas["ema50"]

        ema_bonus = 0

        # EMA 9 acima da 21
        if ema9 > ema21:

            ema_bonus += 10

        # EMA 21 acima da 50
        if ema21 > ema50:

            ema_bonus += 8

        # Alinhamento completo
        if (
            ema9 > ema21
            and
            ema21 > ema50
        ):

            ema_bonus += 7

        # Preço acima da EMA 9
        dados = historico[
            symbol
        ]

        if dados:

            preco = dados[-1][
                "price"
            ]

            if preco > ema9:

                ema_bonus += 5

        tendencia = "ALTA" if (
            ema9 > ema21
            and
            ema21 > ema50
        ) else "MISTA"

    # ----------------------------
    # Score do preço
    # ----------------------------

    score_15 = min(
        v15 / 0.50,
        1
    ) * 15

    score_30 = min(
        v30 / 1.00,
        1
    ) * 25

    score_60 = min(
        v60 / 1.50,
        1
    ) * 20

    # ----------------------------
    # Força relativa
    # ----------------------------

    score_forca30 = min(
        max(
            forca30,
            0
        ) / 0.80,
        1
    ) * 10

    score_forca60 = min(
        max(
            forca60,
            0
        ) / 1.20,
        1
    ) * 10

    # ----------------------------
    # Aceleração
    # ----------------------------

    aceleracao = (
        v30
        -
        (v60 / 2)
    )

    score_aceleracao = min(
        max(
            aceleracao,
            0
        ) / 0.60,
        1
    ) * 10

    # ----------------------------
    # SCORE FINAL
    # ----------------------------

    score = (
        score_15
        +
        score_30
        +
        score_60
        +
        score_forca30
        +
        score_forca60
        +
        score_aceleracao
        +
        ema_bonus
    )

    return {
        "score": min(
            score,
            100
        ),
        "v15": v15,
        "v30": v30,
        "v60": v60,
        "btc30": btc30,
        "btc60": btc60,
        "forca30": forca30,
        "forca60": forca60,
        "ema_bonus": ema_bonus,
        "tendencia": tendencia
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

    # Cooldown da moeda
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

        "btc30": resultado["btc30"],

        "btc60": resultado["btc60"],

        "forca30": resultado["forca30"],

        "forca60": resultado["forca60"],

        "ema_bonus": resultado["ema_bonus"],

        "tendencia": resultado["tendencia"],

        "price": atual["price"],

        "volume": atual["volume"],

        "hold": atual["hold"],

        "time": agora
    }

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
            f"EMA={resultado['tendencia']} "
            f"BTC30={resultado['btc30']:.2f}%"
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
# NÍVEL DO ALERTA
# ============================================================

def nivel_alerta(
    candidato
):

    score = candidato[
        "score"
    ]

    v30 = candidato[
        "v30"
    ]

    v60 = candidato[
        "v60"
    ]

    tendencia = candidato[
        "tendencia"
    ]

    if (
        score >= 85
        and
        v30 >= 0.80
        and
        v60 >= 1.00
        and
        tendencia == "ALTA"
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
# ENVIAR MELHOR
# ============================================================

def enviar_melhor_alerta():

    global melhor_candidato
    global ultimo_alerta_global

    if melhor_candidato is None:

        return

    agora = time.time()

    if (
        agora - ultimo_alerta_global
        <
        INTERVALO_ALERTAS
    ):

        return

    candidato = melhor_candidato

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

    btc30 = candidato[
        "btc30"
    ]

    btc60 = candidato[
        "btc60"
    ]

    forca30 = candidato[
        "forca30"
    ]

    forca60 = candidato[
        "forca60"
    ]

    ema_bonus = candidato[
        "ema_bonus"
    ]

    tendencia = candidato[
        "tendencia"
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

    if tendencia == "ALTA":

        tendencia_texto = (
            "🟢 EMA 9 > EMA 21 > EMA 50"
        )

    elif tendencia == "MISTA":

        tendencia_texto = (
            "🟡 EMAs em tendência mista"
        )

    else:

        tendencia_texto = (
            "⚪ Histórico de EMA insuficiente"
        )

    mensagem = (

        f"{emoji} "
        f"<b>{titulo}</b>\n\n"

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

        f"₿ BTC 30s: "
        f"<b>{btc30:+.2f}%</b>\n"

        f"₿ BTC 60s: "
        f"<b>{btc60:+.2f}%</b>\n"

        f"💪 Força vs BTC 30s: "
        f"<b>{forca30:+.2f}%</b>\n"

        f"💪 Força vs BTC 60s: "
        f"<b>{forca60:+.2f}%</b>\n\n"

        f"📈 Tendência: "
        f"<b>{tendencia_texto}</b>\n"

        f"➕ Bônus EMA: "
        f"<b>+{ema_bonus:.0f}</b>\n\n"

        f"📊 Volume 24h: "
        f"<b>{formatar_volume(volume)}</b>\n"

        f"📈 Open Interest: "
        f"<b>{hold:,.0f}</b>\n\n"

        f"🏆 <b>Melhor sinal "
        f"detectado no último minuto.</b>\n\n"

        f"⚠️ <i>O score mede a força "
        f"observada. Não garante continuação "
        f"do movimento.</i>"
    )

    if enviar(mensagem):

        ultimo_alerta_global = agora

        ultimo_alerta_moeda[
            symbol
        ] = agora

        print(
            f"[ALERTA] "
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
    # Contratos
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
                f"Erro {symbol}: {e}"
            )


# ============================================================
# LOOP
# ============================================================

def loop_bot():

    global melhor_candidato
    global ultima_coleta_candle

    print("=" * 60)

    print(
        "MEXC FUTURES "
        "PUMP HUNTER"
    )

    print(
        "EMA + BTC + FORÇA RELATIVA"
    )

    print("=" * 60)

    enviar(

        "🚀 <b>MEXC FUTURES "
        "PUMP HUNTER</b>\n\n"

        "🎯 Perpétuos USDT\n"

        "🔎 Até 100 contratos\n"

        "📈 EMA 9 / 21 / 50\n"

        "₿ Filtro de tendência BTC\n"

        "💪 Força relativa\n"

        "🏆 Ranking automático\n\n"

        "⏱️ Máximo de "
        "<b>1 alerta por minuto</b>."
    )

    # ----------------------------------------
    # Carrega contratos primeiro
    # ----------------------------------------

    carregar_contratos()

    selecionar_contratos()

    # ----------------------------------------
    # Histórico das EMAs
    # ----------------------------------------

    carregar_ema_historica()

    # ----------------------------------------
    # Loop
    # ----------------------------------------

    proximo_minuto = (
        time.time()
        +
        60
    )

    ultimo_candle = 0

    while rodando:

        inicio = time.time()

        try:

            verificar()

            # Atualiza candles 1m
            agora = time.time()

            if (
                agora - ultimo_candle
                >= 5
            ):

                atualizar_candle_1m()

                ultimo_candle = agora

        except Exception as e:

            print(
                "Erro loop:",
                e
            )

        # ------------------------------------
        # Fecha o minuto e escolhe o melhor
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
            Perpétuos USDT.
        </p>

        <p>
            EMA 9 / 21 / 50.
        </p>

        <p>
            Filtro BTC + força relativa.
        </p>

        <p>
            Máximo de 1 alerta por minuto.
        </p>

    </body>

    </html>

    """


@app.route("/status")
def status():

    return {

        "status": "online",

        "contratos_monitorados":
            len(contratos),

        "historicos":
            len(historico),

        "candles_1m":
            len(candles_1m),

        "ultimo_alerta":
            ultimo_alerta_global,

        "melhor_candidato":
            (
                melhor_candidato[
                    "symbol"
                ]
                if melhor_candidato
                else None
            ),

        "score_melhor":
            (
                round(
                    melhor_candidato[
                        "score"
                    ],
                    1
                )
                if melhor_candidato
                else None
            )
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
