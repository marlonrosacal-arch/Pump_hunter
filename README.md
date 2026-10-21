# BTC Paper Trading 50x (MEXC)

Robô de **simulação** de operações BTC/USDT. Usa dados públicos e NÃO envia ordens reais nem precisa de API key da MEXC.

## Regras implementadas
- Saldo virtual inicial: 100 USDT; margem simulada por entrada: 1 USDT; alavancagem simulada: 50x.
- Candles de 5 minutos, rompimento de máxima/mínima dos 20 candles anteriores, volume acima de 1,5x a média de 20 candles e filtro de EMA50/EMA200.
- Stop inicial baseado na estrutura dos 5 candles anteriores.
- TP1 em 1R fecha 50%; após parcial, trailing stop acompanha a estrutura recente e tenta manter distância de 0,5%.
- Taxa simulada de 0,05% por execução e slippage de 0,02% por execução, ambos configuráveis. Não representam garantia de que sejam as taxas atuais da MEXC.
- Histórico SQLite, PnL líquido estimado, MFE/MAE e relatório após cada 100 trades fechados.
- Alertas no Telegram. Este pacote não cria imagem de gráfico; os alertas são em texto.

## Importante
- É paper trading: a alavancagem e a liquidação são aproximações simplificadas, não o motor de margem/liquidação da MEXC. Este código não calcula uma liquidação exata.
- O risco de 50x é extremo em conta real. Não use este programa para inferir que a estratégia será lucrativa; teste por bastante tempo e avalie PnL líquido, drawdown, profit factor e resultados por lado.
- Se stop e TP forem tocados no mesmo candle, o programa assume o stop primeiro (cenário conservador).
- Configure um disco persistente no Render para não perder o banco SQLite em reinicializações.
- Antes de usar, revise e teste o código. Dados de mercado e suporte a símbolos podem variar na API pública da MEXC.

## Rodar localmente
1. Instale Python 3.10+.
2. Extraia o ZIP e, dentro da pasta, execute:
   `pip install -r requirements.txt`
3. Copie `.env.example` para `.env` apenas como referência. O script não carrega `.env` automaticamente; configure as variáveis no sistema/IDE ou use um carregador de ambiente se desejar.
4. Configure `BOT_TOKEN` e `CHAT_ID` do Telegram. Sem isso, o robô continua no terminal.
5. Execute `python bot.py`.

## Publicar no GitHub e Render
1. Crie um repositório GitHub novo e envie os arquivos desta pasta.
2. No Render, crie um **Background Worker** conectado ao repositório.
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `python bot.py`
5. Adicione variáveis `BOT_TOKEN`, `CHAT_ID`, `DB_PATH=/var/data/paper_trading.db` e as outras desejadas.
6. Adicione disco persistente montado em `/var/data`.
7. Verifique os logs do worker e as mensagens no Telegram.

## Variáveis principais
`START_BALANCE`, `MARGIN_USDT`, `LEVERAGE`, `FEE_RATE`, `SLIPPAGE_RATE`, `VOLUME_MULTIPLIER`, `BREAKOUT_LOOKBACK`, `STOP_LOOKBACK`, `POLL_SECONDS`, `DB_PATH`.
