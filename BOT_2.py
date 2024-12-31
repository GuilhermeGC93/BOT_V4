import os
import ccxt
import websocket
import json
import pandas as pd
import ta
import requests
from datetime import datetime
from decimal import Decimal

# Retrieve Binance API credentials from environment variables
API_KEY = os.getenv('binance_api_key')
API_SECRET = os.getenv('binance_api_secret')

# Create a Binance exchange instance with API credentials
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': API_SECRET,
})

API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTC/USDT"
INTERVAL = "1m"  # 1 minute interval
LIMIT = 50  # Fetch 50 candlesticks (50 minutes)

# WebSocket endpoint for 1-minute candlestick data for BTC/USDT
SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

# Fetch the minimum quantity (minQty) for the BTC/USDT trading pair
def fetch_min_qty(symbol):
    """Fetches the minimum quantity (minQty) for a trading pair."""
    markets = exchange.load_markets()
    min_qty = markets[symbol]['limits']['amount']['min']
    return Decimal(min_qty)  # Return as Decimal to avoid scientific notation

# Fetch available USDT balance
def fetch_available_usdt():
    balance = exchange.fetch_balance()
    available_usdt = balance['free']['USDT']
    print(f"Available USDT: {available_usdt}")
    return available_usdt

# Calculate BTC to buy based on available USDT and minQty
def calculate_btc_to_buy(available_usdt, price, min_qty):
    btc_to_buy = available_usdt / price
    btc_to_buy = round(btc_to_buy, len(str(min_qty).split('.')[1]))  # Truncate to min_qty decimals
    print(f"BTC to Buy: {btc_to_buy}")
    return btc_to_buy

# Place a limit buy order for BTC
def place_limit_buy_order(symbol, btc_to_buy, price):
    order = exchange.create_limit_buy_order(symbol, btc_to_buy, price)
    print(f"Placed Limit Buy Order: {order}")
    return order

# Monitor order status and cancel or place sell orders accordingly
def monitor_order_and_price(order_id, instant_price):
    while True:
        order_status = exchange.fetch_order(order_id, SYMBOL)
        current_price = exchange.fetch_ticker(SYMBOL)['last']
        print('dados: ',order_status)
        print(f"Current Price: {current_price} | Order Status: {order_status['status']}")

        if current_price > instant_price * 1.005 and order_status['status'] != 'filled':
            # Cancel the order if price raises more than 0.5%
            print(f"Price raised more than 0.5%, cancelling order {order_id}")
            exchange.cancel_order(order_id, SYMBOL)

            if order_status['status'] == 'partially_filled':
                # Place sell order for partially bought BTC
                print(f"Order partially filled, placing sell order at 0.3% above instant price")
                btc_bought = order_status['filled']
                sell_price = instant_price * 1.003
                exchange.create_limit_sell_order(SYMBOL, btc_bought, sell_price)
                break
            break

        if order_status['status'] == 'closed':
            # Place sell order for fully bought BTC
            print(f"Order fully filled, placing sell order at 0.4% above instant price")
            btc_bought = order_status['filled']
            sell_price = instant_price * 1.004
            exchange.create_limit_sell_order(SYMBOL, btc_bought, sell_price)
            print('Symbol: ', SYMBOL)
            print('btc_bought: ', btc_bought)
            print('Sell_Price: ', sell_price)
            # ATÉ AQUI ESTÁ FUNCIONANDO. 
            # Necessário pegar ID da ordem de venda para monitorar, e definir lógica de monitoramento
            # Também é necessário definir regra para "stop loss"
            break

def fetch_historical_data():
    """Fetches the last 50 minutes of 1-minute candlestick data from Binance."""
    params = {
        "symbol": "BTCUSDT",
        "interval": INTERVAL,
        "limit": LIMIT
    }
    response = requests.get(API_URL, params=params)
    data = response.json()

    # Transform the data into a list of OHLCV format
    historical_candles = []
    for candle in data:
        historical_candles.append([
            candle[0] // 1000,  # Open time (in seconds)
            float(candle[1]),   # Open price
            float(candle[2]),   # High price
            float(candle[3]),   # Low price
            float(candle[4]),   # Close price
            float(candle[5])    # Volume
        ])
    return historical_candles

def on_message(ws, message):
    global candles
    
    # Parse the incoming WebSocket message
    data = json.loads(message)
    candle = data['k']
    
    # Extract candle information
    is_candle_closed = candle['x']
    open_time = candle['t'] // 1000  # Timestamp in seconds
    open_price = float(candle['o'])
    close_price = float(candle['c'])
    high_price = float(candle['h'])
    low_price = float(candle['l'])
    volume = float(candle['v'])

    # Only add completed candles
    if is_candle_closed:
        # Add the latest candle data to the list
        candles.append([open_time, open_price, high_price, low_price, close_price, volume])

        # Keep only the last 50 minutes of candles
        if len(candles) > 50:
            candles.pop(0)
        
        # Convert to DataFrame
        df = pd.DataFrame(candles, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['time'], unit='s')

        # Calculate MACD difference with custom periods
        short_window = 8  # Short moving average window
        long_window = 17   # Long moving average window
        signal_window = 5  # Signal line window
        df['macd_diff'] = ta.trend.macd_diff(df['close'], window_slow=long_window, window_fast=short_window, window_sign=signal_window)

        # Add macd_flag column
        df['macd_flag'] = (df['macd_diff'] > 0) & (df['macd_diff'].shift() <= 0)

        # Get the latest MACD difference value and macd_flag
        if not pd.isna(df['macd_diff'].iloc[-1]):
            latest_macd_diff = df['macd_diff'].iloc[-1]
            latest_macd_flag = df['macd_flag'].iloc[-1]
            print(f"Latest MACD diff: {latest_macd_diff:.5f} | MACD Flag: {latest_macd_flag} at {df['time'].iloc[-1]}")

            if latest_macd_flag:
                instant_price = close_price
                print(f"Instant Price: {instant_price}")

                # Fetch available USDT balance
                available_usdt = fetch_available_usdt()

                # Calculate BTC to buy
                min_qty = fetch_min_qty(SYMBOL)
                btc_to_buy = calculate_btc_to_buy(available_usdt, instant_price, min_qty)
                print(btc_to_buy)

                # Place a limit buy order
                order = place_limit_buy_order(SYMBOL, btc_to_buy, instant_price)

                # Monitor the order and price
                monitor_order_and_price(order['id'], instant_price)
        else:
            print("Insufficient data to calculate MACD (NaN)")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("Connection closed")

def on_open(ws):
    print("WebSocket connection opened")

if __name__ == "__main__":
    # Fetch historical data (last 50 minutes of 1-minute candles)
    print("Fetching historical data...")
    candles = fetch_historical_data()

    # Create a WebSocket connection
    ws = websocket.WebSocketApp(SOCKET, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
