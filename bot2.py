import os
import time
import threading
import sqlite3
import json
from collections import deque
from datetime import datetime

import requests
from flask import Flask, jsonify


BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MEXC_BASE = "https://contract.mexc.com"


# ============================================================
# CONFIGURAÇÃO V5.1
# ============================================================

SCAN_INTERVAL = 5
MAX_CONTRACTS = 50

ALERT_INTERVAL = 60
COOLDOWN_SYMBOL = 8 * 60
CONTRACT_REFRESH = 15 * 60

MIN_SCORE = 72
STRONG_SCORE = 82

PUMP_60S = 5.0

MIN_MOVE_15S = 0.35
MIN_MOVE_30S = 0.70
MIN_MOVE_60S = 1.20

RSI_MIN = 52.0
RSI_MAX_ENTRY = 78.0
RSI_RISE_MIN = 4.0

BTC_DROP_15S = -0.60
BTC_DROP_60S = -1.50

HISTORY_SECONDS = 8 * 60
MAX_HISTORY = HISTORY_SECONDS // SCAN_INTERVAL + 20


# ============================================================
# LABORATÓRIO V5.1
# ============================================================

ANALYTICS_DB = os.getenv(
    "ANALYTICS_DB",
    "pump_hunter_v51.db"
)

ANALYTICS_HORIZONS = (
    10,
    30,
    60,
    180,
    300
)

ANALYTICS_REPORT_EVERY = 100
ANALYTICS_REPORT_DAYS = 7
ANALYTICS_MIN_BUCKET = 15


# ============================================================
# ESTADO
# ============================================================

app = Flask(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "PumpHunterV5.1/1.0"
})

histories = {}

last_alert_by_symbol = {}

last_alert_time = 0.0
last_scan_time = 0.0
last_successful_api = 0.0
last_error = ""

contracts = []

running = True

active_observations = {}

analytics_lock = threading.Lock()


# ============================================================
# UTILIDADES
# ============================================================

def now_str():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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
# BANCO DE DADOS
# ============================================================

def db_connect():

    conn = sqlite3.connect(
        ANALYTICS_DB,
        timeout=10
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_analytics_db():

    conn = db_connect()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            detected_at REAL NOT NULL,

            detected_at_str TEXT NOT NULL,

            symbol TEXT NOT NULL,

            alert_sent INTEGER DEFAULT 0,

            alert_sent_at REAL,

            entry_price REAL NOT NULL,

            score INTEGER,

            raw_score REAL,

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

            from_high30 REAL,
            from_high60 REAL,

            pump_confirmed INTEGER DEFAULT 0,

            reasons TEXT,

            ret_10 REAL,
            ret_30 REAL,
            ret_60 REAL,
            ret_180 REAL,
            ret_300 REAL,

            mfe_60 REAL,
            mae_60 REAL,

            max_fav REAL,
            max_adv REAL,

            complete INTEGER DEFAULT 0,

            completed_at REAL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_signals_symbol_time

        ON signals(symbol, detected_at)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_signals_complete

        ON signals(complete, detected_at)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_meta (

            key TEXT PRIMARY KEY,

            value TEXT
        )
    """)

    conn.commit()

    row = conn.execute(
        """
        SELECT value
        FROM analytics_meta
        WHERE key='last_report_at'
        """
    ).fetchone()

    if not row:

        conn.execute(
            """
            INSERT OR REPLACE INTO
            analytics_meta(key,value)

            VALUES(?,?)
            """,
            (
                "last_report_at",
                str(time.time())
            )
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO
            analytics_meta(key,value)

            VALUES(?,?)
            """,
            (
                "last_report_completed_count",
                "0"
            )
        )

        conn.commit()

    conn.close()


def db_get_meta(key, default=None):

    conn = db_connect()

    row = conn.execute(
        """
        SELECT value
        FROM analytics_meta
        WHERE key=?
        """,
        (key,)
    ).fetchone()

    conn.close()

    if row:
        return row["value"]

    return default


def db_set_meta(key, value):

    conn = db_connect()

    conn.execute(
        """
        INSERT OR REPLACE INTO
        analytics_meta(key,value)

        VALUES(?,?)
        """,
        (
            key,
            str(value)
        )
    )

    conn.commit()

    conn.close()


# ============================================================
# REGISTRO DOS SINAIS
# ============================================================

def signal_recently_registered(
    symbol,
    now_ts
):

    conn = db_connect()

    row = conn.execute(
        """
        SELECT id

        FROM signals

        WHERE symbol=?

        AND detected_at>=?

        ORDER BY detected_at DESC

        LIMIT 1
        """,
        (
            symbol,
            now_ts - COOLDOWN_SYMBOL
        )
    ).fetchone()

    conn.close()

    return row is not None


def latest_signal_id(symbol):

    conn = db_connect()

    row = conn.execute(
        """
        SELECT id

        FROM signals

        WHERE symbol=?

        ORDER BY detected_at DESC

        LIMIT 1
        """
    ).fetchone()

    conn.close()

    if row:
        return int(row["id"])

    return None


def register_signal(
    a,
    entry_price,
    detected_at
):

    if signal_recently_registered(
        a["symbol"],
        detected_at
    ):
        return None

    conn = db_connect()

    cur = conn.execute(
        """
        INSERT INTO signals (

            detected_at,
            detected_at_str,
            symbol,

            entry_price,

            score,
            raw_score,

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

            from_high30,
            from_high60,

            pump_confirmed,

            reasons
        )

        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """,
        (

            detected_at,

            datetime.fromtimestamp(
                detected_at
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

            a["symbol"],

            entry_price,

            a["score"],
            a.get(
                "raw_score",
                a["score"]
            ),

            a["r5"],
            a.get("r10", 0.0),
            a["r15"],
            a["r30"],
            a["r60"],

            a.get(
                "accel_15",
                0.0
            ),

            a.get(
                "accel_5",
                0.0
            ),

            a["rsi"],
            a["rsi_delta"],

            a["ema9"],
            a["ema21"],
            a["ema50"],

            a["oi_change"],

            a["btc15"],
            a["btc30"],
            a["btc60"],

            a["from_high30"],
            a["from_high60"],

            int(
                a["pump_confirmed"]
            ),

            json.dumps(
                a["reasons"],
                ensure_ascii=False
            )
        )
    )

    signal_id = cur.lastrowid

    conn.commit()

    conn.close()

    active_observations[
        signal_id
    ] = {

        "id": signal_id,

        "symbol": a["symbol"],

        "entry_time": detected_at,

        "entry_price": entry_price,

        "max_price": entry_price,

        "min_price": entry_price,

        "max60": entry_price,

        "min60": entry_price,

        "completed_horizons": set(),

        "last_price": entry_price
    }

    return signal_id


def mark_alert_sent(
    signal_id,
    alert_time
):

    if not signal_id:
        return

    conn = db_connect()

    conn.execute(
        """
        UPDATE signals

        SET
            alert_sent=1,
            alert_sent_at=?

        WHERE id=?
        """,
        (
            alert_time,
            signal_id
        )
    )

    conn.commit()

    conn.close()


# ============================================================
# RECUPERAÇÃO APÓS RESTART
# ============================================================

def load_pending_observations():

    conn = db_connect()

    rows = conn.execute(
        """
        SELECT

            id,
            symbol,
            detected_at,
            entry_price,

            ret_10,
            ret_30,
            ret_60,
            ret_180,
            ret_300

        FROM signals

        WHERE complete=0

        AND detected_at >= ?
        """,
        (
            time.time() - 600,
        )
    ).fetchall()

    conn.close()

    for row in rows:

        completed = set()

        for horizon, column in (

            (10, "ret_10"),
            (30, "ret_30"),
            (60, "ret_60"),
            (180, "ret_180"),
            (300, "ret_300")

        ):

            if row[column] is not None:
                completed.add(horizon)

        active_observations[
            int(row["id"])
        ] = {

            "id": int(row["id"]),

            "symbol": row["symbol"],

            "entry_time": float(
                row["detected_at"]
            ),

            "entry_price": float(
                row["entry_price"]
            ),

            "max_price": float(
                row["entry_price"]
            ),

            "min_price": float(
                row["entry_price"]
            ),

            "max60": float(
                row["entry_price"]
            ),

            "min60": float(
                row["entry_price"]
            ),

            "completed_horizons":
                completed,

            "last_price":
                float(row["entry_price"])
        }


# ============================================================
# ACOMPANHAMENTO DOS RESULTADOS
# ============================================================

def pct_from_entry(
    entry,
    current
):

    if not entry or entry <= 0:
        return 0.0

    return (
        (current / entry) - 1.0
    ) * 100.0


def update_active_observations(
    tickers,
    now_ts
):

    if not active_observations:
        return

    finished = []

    with analytics_lock:

        for signal_id, obs in list(
            active_observations.items()
        ):

            ticker = tickers.get(
                obs["symbol"]
            )

            if not ticker:
                continue

            current = ticker["price"]

            obs["last_price"] = current

            obs["max_price"] = max(
                obs["max_price"],
                current
            )

            obs["min_price"] = min(
                obs["min_price"],
                current
            )

            age = (
                now_ts -
                obs["entry_time"]
            )

            if age <= 60:

                obs["max60"] = max(
                    obs["max60"],
                    current
                )

                obs["min60"] = min(
                    obs["min60"],
                    current
                )

            due = [

                h

                for h in ANALYTICS_HORIZONS

                if (
                    age >= h
                    and h not in
                    obs["completed_horizons"]
                )
            ]

            if not due:
                continue

            conn = db_connect()

            for horizon in due:

                ret = pct_from_entry(
                    obs["entry_price"],
                    current
                )

                conn.execute(
                    f"""
                    UPDATE signals

                    SET ret_{horizon}=?

                    WHERE id=?
                    """,
                    (
                        ret,
                        signal_id
                    )
                )

                obs[
                    "completed_horizons"
                ].add(horizon)

            if (
                60 in
                obs["completed_horizons"]
            ):

                mfe60 = pct_from_entry(
                    obs["entry_price"],
                    obs["max60"]
                )

                mae60 = pct_from_entry(
                    obs["entry_price"],
                    obs["min60"]
                )

                conn.execute(
                    """
                    UPDATE signals

                    SET
                        mfe_60=?,
                        mae_60=?

                    WHERE id=?
                    """,
                    (
                        mfe60,
                        mae60,
                        signal_id
                    )
                )

            if (
                300 in
                obs["completed_horizons"]
            ):

                max_fav = pct_from_entry(
                    obs["entry_price"],
                    obs["max_price"]
                )

                max_adv = pct_from_entry(
                    obs["entry_price"],
                    obs["min_price"]
                )

                conn.execute(
                    """
                    UPDATE signals

                    SET

                        max_fav=?,
                        max_adv=?,

                        complete=1,

                        completed_at=?

                    WHERE id=?
                    """,
                    (
                        max_fav,
                        max_adv,

                        now_ts,

                        signal_id
                    )
                )

                finished.append(
                    signal_id
                )

            conn.commit()

            conn.close()

        for signal_id in finished:

            active_observations.pop(
                signal_id,
                None
            )


# ============================================================
# ESTATÍSTICAS
# ============================================================

def get_completed_count():

    conn = db_connect()

    row = conn.execute(
        """
        SELECT COUNT(*) AS n

        FROM signals

        WHERE complete=1
        """
    ).fetchone()

    conn.close()

    return int(row["n"])


def pct_positive(
    rows,
    column,
    threshold
):

    values = [

        r[column]

        for r in rows

        if r[column] is not None
    ]

    if not values:
        return None

    return (
        sum(
            1 for v in values
            if v >= threshold
        )
        /
        len(values)
    ) * 100.0


def mean_column(
    rows,
    column
):

    values = [

        float(r[column])

        for r in rows

        if r[column] is not None
    ]

    if not values:
        return None

    return sum(values) / len(values)


def fetch_last_completed(
    limit=100
):

    conn = db_connect()

    rows = conn.execute(
        """
        SELECT *

        FROM signals

        WHERE complete=1

        ORDER BY completed_at DESC

        LIMIT ?
        """,
        (limit,)
    ).fetchall()

    conn.close()

    return rows


# ============================================================
# DESCOBERTA DE PADRÕES
# ============================================================

def condition_stats(
    rows,
    condition
):

    selected = [

        r for r in rows
        if condition(r)
    ]

    if len(selected) < ANALYTICS_MIN_BUCKET:
        return None

    return {

        "n": len(selected),

        "avg60":
            mean_column(
                selected,
                "ret_60"
            ) or 0.0,

        "avg300":
            mean_column(
                selected,
                "ret_300"
            ) or 0.0,

        "mfe":
            mean_column(
                selected,
                "max_fav"
            ) or 0.0,

        "mae":
            mean_column(
                selected,
                "max_adv"
            ) or 0.0
    }


def discover_patterns(rows):

    patterns = [

        (
            "Score 85+",
            lambda r:
                r["score"] >= 85
        ),

        (
            "Score 90+",
            lambda r:
                r["score"] >= 90
        ),

        (
            "RSI 60–70",
            lambda r:
                60 <= r["rsi"] < 70
        ),

        (
            "RSI 70–80",
            lambda r:
                70 <= r["rsi"] < 80
        ),

        (
            "RSI 80–90",
            lambda r:
                80 <= r["rsi"] < 90
        ),

        (
            "RSI 90+",
            lambda r:
                r["rsi"] >= 90
        ),

        (
            "15s ≥ +0,50%",
            lambda r:
                r["r15"] >= 0.50
        ),

        (
            "30s ≥ +0,70%",
            lambda r:
                r["r30"] >= 0.70
        ),

        (
            "60s ≥ +1,20%",
            lambda r:
                r["r60"] >= 1.20
        ),

        (
            "Aceleração 15s",
            lambda r:
                r["accel_15"] > 0.20
        ),

        (
            "Aceleração curta",
            lambda r:
                r["accel_5"] > 0.08
        ),

        (
            "OI positivo",
            lambda r:
                r["oi_change"] > 0
        ),

        (
            "OI > +0,30%",
            lambda r:
                r["oi_change"] > 0.30
        ),

        (
            "EMA9 > EMA21",
            lambda r:
                r["ema9"] > r["ema21"]
        ),

        (
            "EMA21 > EMA50",
            lambda r:
                r["ema21"] > r["ema50"]
        ),

        (
            "Nova máxima 30s",
            lambda r:
                r["from_high30"] >= -0.05
        ),

        (
            "BTC 15s não negativo",
            lambda r:
                r["btc15"] >= 0
        ),

        (
            "Score 85+ + RSI 65–85",
            lambda r:
                (
                    r["score"] >= 85
                    and
                    65 <= r["rsi"] <= 85
                )
        ),

        (
            "Score 85+ + 30s ≥ +0,70%",
            lambda r:
                (
                    r["score"] >= 85
                    and
                    r["r30"] >= 0.70
                )
        ),

        (
            "RSI 65–85 + aceleração",
            lambda r:
                (
                    65 <= r["rsi"] <= 85
                    and
                    r["accel_15"] > 0.20
                )
        ),

        (
            "RSI 65–85 + OI positivo",
            lambda r:
                (
                    65 <= r["rsi"] <= 85
                    and
                    r["oi_change"] > 0
                )
        ),

        (
            "Score 85+ + OI positivo",
            lambda r:
                (
                    r["score"] >= 85
                    and
                    r["oi_change"] > 0
                )
        ),

        (
            "Score 85+ + RSI 65–85 + 30s forte",
            lambda r:
                (
                    r["score"] >= 85
                    and
                    65 <= r["rsi"] <= 85
                    and
                    r["r30"] >= 0.70
                )
        )
    ]

    baseline = mean_column(
        rows,
        "ret_60"
    )

    if baseline is None:
        return []

    results = []

    for name, condition in patterns:

        stats = condition_stats(
            rows,
            condition
        )

        if not stats:
            continue

        results.append({

            "name": name,

            "n": stats["n"],

            "avg60":
                stats["avg60"],

            "avg300":
                stats["avg300"],

            "mfe":
                stats["mfe"],

            "mae":
                stats["mae"],

            "uplift":
                stats["avg60"] - baseline
        })

    results.sort(
        key=lambda x:
            (
                x["uplift"],
                x["n"]
            ),
        reverse=True
    )

    return results


# ============================================================
# RELATÓRIO TELEGRAM
# ============================================================

def build_analytics_report(
    rows,
    total_completed
):

    if not rows:
        return None

    n = len(rows)

    hit05 = pct_positive(
        rows,
        "ret_60",
        0.50
    )

    hit10 = pct_positive(
        rows,
        "ret_60",
        1.00
    )

    hit20_300 = pct_positive(
        rows,
        "max_fav",
        2.00
    )

    avg60 = mean_column(
        rows,
        "ret_60"
    )

    avg300 = mean_column(
        rows,
        "ret_300"
    )

    avg_mfe = mean_column(
        rows,
        "max_fav"
    )

    avg_mae = mean_column(
        rows,
        "max_adv"
    )

    patterns = discover_patterns(
        rows
    )

    lines = [

        "🤖 *PUMP HUNTER V5.1 — LABORATÓRIO*",

        "",

        f"📊 *Últimos {n} sinais concluídos*",

        f"Total histórico: {total_completed}",

        "",

        "🎯 *Resultado*",

        (
            f"• ≥ +0,5% em 60s: "
            f"{hit05:.1f}%"
            if hit05 is not None
            else
            "• ≥ +0,5% em 60s: n/d"
        ),

        (
            f"• ≥ +1,0% em 60s: "
            f"{hit10:.1f}%"
            if hit10 is not None
            else
            "• ≥ +1,0% em 60s: n/d"
        ),

        (
            f"• ≥ +2,0% em até 5m: "
            f"{hit20_300:.1f}%"
            if hit20_300 is not None
            else
            "• ≥ +2,0% em até 5m: n/d"
        ),

        (
            f"• Retorno médio 60s: "
            f"{avg60:+.2f}%"
            if avg60 is not None
            else
            "• Retorno médio 60s: n/d"
        ),

        (
            f"• Retorno médio 5m: "
            f"{avg300:+.2f}%"
            if avg300 is not None
            else
            "• Retorno médio 5m: n/d"
        ),

        (
            f"• MFE médio 5m: "
            f"{avg_mfe:+.2f}%"
            if avg_mfe is not None
            else
            "• MFE médio 5m: n/d"
        ),

        (
            f"• MAE médio 5m: "
            f"{avg_mae:+.2f}%"
            if avg_mae is not None
            else
            "• MAE médio 5m: n/d"
        ),

        "",

        "📈 *Condições com melhor desempenho*"
    ]

    if patterns:

        for p in patterns[:3]:

            lines.append(
                f"• {p['name']} → "
                f"{p['avg60']:+.2f}%/60s "
                f"(n={p['n']}, "
                f"dif. {p['uplift']:+.2f}pp)"
            )

    else:

        lines.append(
            "• Ainda não há amostra suficiente."
        )

    worst = sorted(
        patterns,
        key=lambda x:
            x["uplift"]
    )[:2]

    lines += [

        "",

        "⚠️ *Pontos para investigar*"
    ]

    if worst:

        for p in worst:

            lines.append(
                f"• {p['name']} → "
                f"{p['avg60']:+.2f}%/60s "
                f"(n={p['n']})"
            )

    else:

        lines.append(
            "• Ainda não há dados suficientes."
        )

    lines += [

        "",

        "💡 *Leitura automática*",

        (
            "O laboratório compara as condições "
            "com a média dos sinais."
        ),

        (
            "Nenhuma regra da estratégia "
            "foi alterada automaticamente."
        ),

        "",

        (
            "📌 Amostra estatística; "
            "não representa garantia de "
            "resultado futuro."
        )
    ]

    return "\n".join(lines)


def maybe_send_analytics_report():

    total = get_completed_count()

    if total <= 0:
        return

    last_report_count = int(
        float(
            db_get_meta(
                "last_report_completed_count",
                "0"
            )
        )
    )

    last_report_at = float(
        db_get_meta(
            "last_report_at",
            str(time.time())
        )
    )

    count_trigger = (
        total - last_report_count
    ) >= ANALYTICS_REPORT_EVERY

    week_trigger = (

        time.time() -
        last_report_at

        >=
        ANALYTICS_REPORT_DAYS * 86400

        and
        total > last_report_count
    )

    if (
        not count_trigger
        and
        not week_trigger
    ):
        return

    rows = fetch_last_completed(
        ANALYTICS_REPORT_EVERY
    )

    report = build_analytics_report(
        rows,
        total
    )

    if not report:
        return

    if send_telegram(report):

        db_set_meta(
            "last_report_completed_count",
            total
        )

        db_set_meta(
            "last_report_at",
            time.time()
        )

        motivo = (

            "100 sinais concluídos"

            if count_trigger

            else
            "relatório semanal"
        )

        print(
            f"[{now_str()}] "
            f"📊 Relatório V5.1 enviado "
            f"({motivo})."
        )


# ============================================================
# MEXC
# ============================================================

def refresh_contracts():

    global contracts

    data = get_json(
        "/api/v1/contract/detail",
        timeout=8
    )

    if (
        not data
        or
        not data.get("success")
    ):

        print(
            f"[{now_str()}] "
            "⚠️ Falha ao atualizar contratos."
        )

        return

    items = data.get("data") or []

    symbols = []

    for item in items:

        symbol = item.get(
            "symbol",
            ""
        )

        if symbol.endswith(
            "_USDT"
        ):

            symbols.append(
                symbol
            )

    if symbols:

        contracts = symbols

        print(
            f"[{now_str()}] "
            f"Contratos encontrados: "
            f"{len(contracts)}"
        )


def get_all_tickers():

    data = get_json(
        "/api/v1/contract/ticker",
        timeout=8
    )

    if (
        not data
        or
        not data.get("success")
    ):
        return {}

    raw = data.get("data")

    if isinstance(
        raw,
        list
    ):

        items = raw

    elif isinstance(
        raw,
        dict
    ):

        if (
            "symbol" in raw
            and
            "lastPrice" in raw
        ):

            items = [raw]

        else:

            items = []

    else:

        items = []

    result = {}

    for item in items:

        try:

            symbol = item.get(
                "symbol"
            )

            price = float(
                item.get(
                    "lastPrice"
                )
            )

            if (
                not symbol
                or
                price <= 0
            ):
                continue

            result[symbol] = {

                "price": price,

                "volume24":
                    float(
                        item.get(
                            "volume24"
                        ) or 0
                    ),

                "amount24":
                    float(
                        item.get(
                            "amount24"
                        ) or 0
                    ),

                "holdVol":
                    float(
                        item.get(
                            "holdVol"
                        ) or 0
                    ),

                "riseFallRate":
                    float(
                        item.get(
                            "riseFallRate"
                        ) or 0
                    )
