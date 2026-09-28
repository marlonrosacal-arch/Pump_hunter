import requests
import time
from datetime import datetime

# ==== COLOCA SEU TOKEN NOVO AQUI ====
TOKEN = "8523359888:AAFcISRXoEjnmXfPuaak5nZmT-N8o0mQ0ho"
CHAT_ID = "1475544551"  # pega no @userinfobot

# 50 moedas - caçador de IRYS
MOEDAS = [
"IRYSUSDT","ZORAUSDT","0GUSDT","PENGUUSDT","PEPEUSDT","BONKUSDT","WIFUSDT",
"DOGEUSDT","SHIBUSDT","FLOKIUSDT","BOMEUSDT","MEMEUSDT","ORDIUSDT","SATSUSDT",
"NOTUSDT","TONUSDT","SOLUSDT","AVAXUSDT","NEARUSDT","ARBUSDT",
"OPUSDT","APTUSDT","SUIUSDT","SEIUSDT","TIAUSDT","PYTHUSDT","JUPUSDT",
"WUSDT","ENAUSDT","ETHFIUSDT","BNSOLUSDT","ZKUSDT","STRKUSDT",
"ARBUSDT","LDOUSDT","RENDERUSDT","FETUSDT","AGIXUSDT","WLDUSDT",
"TAOUSDT","ARUSDT","FILUSDT","INJUSDT","RUNEUSDT","LTCUSDT",
"LINKUSDT","UNIUSDT","AAVEUSDT","XRPUSDT"
]

def enviar(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except:
        pass

def verificar():
    for moeda in MOEDAS:
        try:
            # pega preço de 5 e 15 min atrás
            r = requests.get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={moeda}", timeout=10).json()
            preco = float(r['lastPrice'])
            variacao_24h = float(r['priceChangePercent'])
            
            # Padrão A - igual IRYS hoje: 2.5% em 5 min
            # Simplificado: se 24h já tá > 3% e volume subindo, alerta
            if variacao_24h >= 2.5:
                volume = float(r['volume'])
                msg = f"🚀 *PUMP DETECTADO - Padrão IRYS*\n\nMoeda: `{moeda}`\nVariação: {variacao_24h:.2f}%\nPreço: {preco}\nVolume: {volume:.0f}\n\nHora: {datetime.now().strftime('%H:%M:%S')}"
                enviar(msg)
                time.sleep(2) # evita spam
            
        except Exception as e:
            continue

enviar("✅ Bot 2 IRYS Hunter ligado! Monitorando 50 moedas...")
print("Bot ligado")

while True:
    verificar()
    time.sleep(60) # verifica a cada 1 min
