import requests, time, os, threading
from flask import Flask
app = Flask(__name__)

TOKEN = "8523359888:AAFcISRXoEjnmXfPuaak5nZmT-N8o0mQ0ho" # <-- TROCA DEPOIS!
CHAT_ID = "1475544551"

MOEDAS = ["PONSUSDT","IRYSUSDT","ZORAUSDT","OGUSDT","MARSCOINUSDT","PENGUUSDT","PEPEUSDT","DOGEUSDT","SHIBUSDT","FLOKIUSDT","BOMEUSDT","WIFUSDT","NOTUSDT","TONUSDT","SOLUSDT","AVAXUSDT","NEARUSDT","OPUSDT","APTUSDT","SUIUSDT","SEIUSDT","TIAUSDT","WUSDT","ENAUSDT","ETHFIUSDT","ZKUSDT","ARBUSDT","LDOUSDT","RENDERUSDT","FETUSDT","TAOUSDT","ARUSDT","FILUSDT","INJUSDT","RUNEUSDT","LINKUSDT","UNIUSDT","AAVEUSDT","XRPUSDT","ADAUSDT","TRXUSDT","DOGSUSDT","XLMUSDT","HBARUSDT","DOTUSDT","ETCUSDT","LTCUSDT","BCHUSDT","MATICUSDT"]

ja_alertadas = set()

def enviar(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
    except: pass

@app.route('/')
def home():
    return "Bot 2 - MEXC Pump Hunter Ativo!"

def verificar():
    try:
        # MEXC API - pega 24hr de TODAS
        r = requests.get("https://api.mexc.com/api/v3/ticker/24hr", timeout=15).json()
        mapa = {item['symbol']: float(item['priceChangePercent']) for item in r}
        for moeda in MOEDAS:
            var = mapa.get(moeda, 0)
            if var >= 5 and moeda not in ja_alertadas:
                enviar(f"🚀 *PUMP DETECTADO!* 🚀\nMoeda: `{moeda}`\nAlta: *{var:.2f}%* em 24h\nMEXC!")
                ja_alertadas.add(moeda)
            # reseta se cair abaixo de 3%
            if var < 3 and moeda in ja_alertadas:
                ja_alertadas.remove(moeda)
    except Exception as e:
        print(f"Erro: {e}")

def loop_bot():
    time.sleep(5)
    enviar("✅ *Bot 2 MEXC iniciado!*\nMonitorando PONS, IRYS, MARS...")
    while True:
        verificar()
        time.sleep(120)

threading.Thread(target=loop_bot, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
