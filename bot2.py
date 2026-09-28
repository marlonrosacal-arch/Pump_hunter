import requests
import time
from datetime import datetime
from flask import Flask
import threading
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot 2 - Pump Hunter - Rodando!"

# ==== COLOCA SEU TOKEN NOVO AQUI ====
TOKEN = "8523359888:AAFcISRXoEjnmXfPuaak5nZmT-8q8KzX2yM"
CHAT_ID = "1475544551"

# 50 moedas - caçador de IRYS
MOEDAS = [
"IRYSUSDT","ZORAUSDT","OGUSDT","PENGUUSDT","PEPEUSDT",
"DOGEUSDT","SHIBUSDT","FLOKIUSDT","BOMEUSDT","WIFUSDT",
"NOTUSDT","TONUSDT","SOLUSDT","AVAXUSDT","NEARUSDT",
"OPUSDT","APTUSDT","SUIUSDT","SEIUSDT","TIAUSDT",
"WUSDT","ENAUSDT","ETHFIUSDT","BNSOLUSDT","ZKUSDT",
"ARBUSDT","LDOUSDT","RENDERUSDT","FETUSDT","AGLDUSDT",
"TAOUSDT","ARUSDT","FILUSDT","INJUSDT","RUNEUSDT",
"LINKUSDT","UNIUSDT","AAVEUSDT","XRPUSDT","ADAUSDT",
"TRXUSDT","DOGSUSDT","XLMUSDT","HBARUSDT","DOTUSDT",
"ETCUSDT","LTCUSDT","BCHUSDT","MATICUSDT","AVAXUSDT"
]

def enviar(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        print(f"Erro ao enviar: {e}")

def verificar():
    try:
        # Pega variação 24h de todas as moedas
        r = requests.get("https://api.binance.com/api/v3/ticker/24hr", timeout=15).json()
        mapa = {item['symbol']: float(item['priceChangePercent']) for item in r}

        for moeda in MOEDAS:
            var = mapa.get(moeda, 0)
            if var >= 5: # 5% de pump
                nome = moeda.replace("USDT","")
                enviar(f"🚀 *PUMP DETECTADO!* 🚀\n\nMoeda: `{nome}`\nAlta: *{var:.2f}%* nas ultimas 24h\n\nPode ser a IRYS ou outra indo!")

    except Exception as e:
        print(f"Erro verificar: {e}")

def loop_bot():
    enviar("✅ Bot 2 - Caçador de Pumps iniciado! Monitorando 50 moedas a cada 2 min.")
    while True:
        verificar()
        time.sleep(120) # 2 minutos

# Inicia o bot em segundo plano
threading.Thread(target=loop_bot, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
