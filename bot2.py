import time
import os
import pandas as pd
import ta
import ccxt
import requests

# O Render continuará lendo o Telegram das suas variáveis atuais
TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# CONFIGURAÇÃO PÚBLICA DA MEXC (Sem necessidade de API_KEY ou SECRET)
exchange = ccxt.mexc({
    'options': {'defaultType': 'swap'}, # Define que queremos o mercado de Futuros
    'enableRateLimit': True
})

TIMEFRAME = '5m'

def send_telegram_signal(message):
    url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try: requests.post(url, json=payload)
    except Exception as e: print(f"Erro Telegram: {e}")

def get_active_altcoins():
    try:
        # Carrega as moedas usando a API pública
        markets = exchange.load_markets()
        symbols = [
            symbol for symbol in markets.keys() 
            if ':USDT' in symbol and not any(x in symbol for x in ['BTC', 'ETH', 'USDC'])
        ]
        return symbols
    except Exception as e:
        print(f"Erro ao carregar mercados: {e}")
        return []

def scan_symbol(symbol):
    try:
        # Puxa os candles históricos públicos
        bars = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=50)
        if len(bars) < 30: return

        df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # 1. Bandas de Bollinger
        indicator_bb = ta.volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
        df['bb_high'] = indicator_bb.bollinger_hband()
        df['bb_low'] = indicator_bb.bollinger_lband()
        
        # 2. RSI
        df['rsi'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()
        
        # 3. Média de Volume
        df['vol_mean'] = df['volume'].rolling(window=20).mean()
        
        # 4. ATR (Mede a volatilidade para o Stop e Alvo)
        df['atr'] = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()

        last_row = df.iloc[-2]
        
        # Filtro de Volume Explosivo (1.8x maior que a média)
        volume_explosivo = last_row['volume'] > (last_row['vol_mean'] * 1.8)
        
        if not volume_explosivo:
            return

        current_close = last_row['close']
        rsi_value = last_row['rsi']
        atr_value = last_row['atr']
        coin_clean = symbol.split(':')[0] # Limpa o nome para o Telegram (Ex: SOL/USDT)

        # Multiplicadores do ATR para posições seguras em Low Caps
        sl_distance = atr_value * 1.5
        tp1_distance = atr_value * 1.5
        tp2_distance = atr_value * 3.0

        # GATILHO DE ALTA (LONG)
        if current_close > last_row['bb_high'] and 55 < rsi_value < 68:
            sl = current_close - sl_distance
            tp1 = current_close + tp1_distance
            tp2 = current_close + tp2_distance
            
            msg = (
                f"🔥 *MEXC SCANNER - ALERTA DE LONG* 🔥\n\n"
                f"🪙 *Ativo:* `{coin_clean}`\n"
                f"📈 *Preço de Entrada:* `{current_close:.5f}`\n\n"
                f"🛡️ *STOP LOSS:* `{sl:.5f}`\n"
                f"🎯 *TAKE PROFIT 1:* `{tp1:.5f}` (Parcial)\n"
                f"🚀 *TAKE PROFIT 2:* `{tp2:.5f}` (Final)\n\n"
                f"⚡ _Motivo: Força Compradora + Volume + Volatilidade Adaptada!_"
            )
            send_telegram_signal(msg)
            print(f"[SINAL] Long enviado para {coin_clean}")
            time.sleep(2)

        # GATILHO DE BAIXA (SHORT)
        elif current_close < last_row['bb_low'] and 32 < rsi_value < 45:
            sl = current_close + sl_distance
            tp1 = current_close - tp1_distance
            tp2 = current_close - deprecated = tp2_distance
            
            msg = (
                f"📉 *MEXC SCANNER - ALERTA DE SHORT* 📉\n\n"
                f"🪙 *Ativo:* `{coin_clean}`\n"
                f"📉 *Preço de Entrada:* `{current_close:.5f}`\n\n"
                f"🛡️ *STOP LOSS:* `{sl:.5f}`\n"
                f"🎯 *TAKE PROFIT 1:* `{tp1:.5f}` (Parcial)\n"
                f"🚀 *TAKE PROFIT 2:* `{tp2:.5f}` (Final)\n\n"
                f"⚡ _Motivo: Força Vendedora + Volume + Volatilidade Adaptada!_"
            )
            send_telegram_signal(msg)
            print(f"[SINAL] Short enviado para {coin_clean}")
            time.sleep(2)

    except Exception as e:
        pass

def run_scanner():
    print("🔄 Carregando lista de Altcoins da MEXC...")
    symbols = get_active_altcoins()
    print(f"🎯 Total de {len(symbols)} altcoins encontradas para monitoramento.")
    
    while True:
        print("\n=== Iniciando nova varredura de mercado ===")
        start_time = time.time()
        
        for symbol in symbols:
            scan_symbol(symbol)
            time.sleep(0.15) # Delay seguro para não estourar limite da API pública
            
        elapsed = time.time() - start_time
        print(f"✨ Varredura completada em {elapsed:.2f} segundos.")
        time.sleep(120)

if __name__ == "__main__":
    run_scanner()
