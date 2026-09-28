import requests
import time
from flask import Flask
import threading
import os

app = Flask(__name__)

TOKEN = "8523359888:AAFcISRXoEjnmXfPuaak5nZmT-N8o0mQ0ho"
CHAT_ID = "1475544551"

MOEDAS = ["IRYSUSDT","ZORAUSDT","OGUSDT","PENGUUSDT","PEPEUSDT","DOGEUSDT","SHIBUSDT","FLOKIUSDT","BOMEUSDT","WIFUSDT","NOTUSDT","TONUSDT","SOLUSDT","AVAXUSDT","NEARUSDT","OPUSDT","APTUSDT","SUIUSDT","SEIUSDT","TIAUSDT","WUSDT","ENAUSDT","ETHFIUSDT","BNSOLUSDT","ZKUSDT","ARBUSDT","LDOUSDT","RENDERUSDT","FETUSDT","TAOUSDT","ARUSDT","FILUSDT","INJUSDT","RUNEUSDT","LINKUSDT","UNIUSDT","AAVEUSDT","XRPUSDT","ADAUSDT","TRXUSDT","DOGSUSDT","XLMUSDT","HBARUSDT","DOTUSDT","ETCUSDT","LTCUSDT","BCHUSDT","MATICUSDT"]

def enviar(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
    except:
        pass

@app.route('/')
def home():
    return "Bot 2 - Pump Hunter Ativo!"

def verificar():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/24hr", timeout=15).json()
        mapa = {item['symbol']: float(item['priceChangePercent']) for item in r}
        for moeda in MOEDAS:
            var = mapa.get(moeda, 0)
            if var >= 5:
                enviar(f"🚀 *PUMP DETECTADO!* 🚀\n\nMoeda: `{moeda.replace('USDT','')}`\nAlta: *{var:.2f}%* em 24h\n\n👉 Checar na Binance!")
    except Exception as e:
        print(f"Erro: {e}")

def loop_bot():
    time.sleep(5)
    enviar("✅ *Bot 2 - Caçador de Pumps iniciado!*\nMonitorando 50 moedas.")
    while True:
        verificar()
        time.sleep(120)

threading.Thread(target=loop_bot, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
