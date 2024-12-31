import pandas as pd
import pandas_ta as pdta
import ta
import numpy as np
from datetime import datetime
import time as time
import itertools

def add_purchase_instants(df, variation, steps):
    # Calculate the threshold prices for each step
    threshold_prices = df['Close'] * variation
    
    # Use a rolling max to find the maximum value in the next `steps` rows for each row
    rolling_max = df['Close'].shift(-1).rolling(window=steps).max()
    
    # Check where the rolling max is greater than or equal to the threshold price
    purchase_instant = rolling_max >= threshold_prices
    
    # Add the result as a new column to the DataFrame
    df[f'v_{variation}_s_{steps}'] = purchase_instant.fillna(False).astype(bool)

    return df

now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV file into a pandas DataFrame
Banco = "BTCUSDT_2023_1min"
df = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Binance\\' + Banco + '.csv')

# Technical indicators parameters

variacao = [1.003, 1.004, 1.005, 1.0075, 1.01, 1.015, 1.02] # Lucro desejado
intervalo_subida = [60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 840, 960, 1200, 1320, 1440] # Tempo em minutos dentro do qual queremos que ocorra a subida de preço

# Calcula variacao
# Optimized loop using itertools.product for cleaner and faster iteration
for variacao_val, intervalo_subida_val in itertools.product(variacao, intervalo_subida):
    add_purchase_instants(df, variacao_val, intervalo_subida_val)
    print(f"contador variacao: {variacao.index(variacao_val)}   contador intervalo_subida: {intervalo_subida.index(intervalo_subida_val)}")


df.drop(columns=['Unnamed: 0','Open','High','Low','Close','Volume','OpenInterest'], axis=1, inplace=True)

now2 = datetime.now()
final = now2.strftime("%H:%M:%S")

print("Hora de inicio: ", inicio)
print("hora de termino: ", final)
df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Compra\\Dados_Compra.csv', index=False)






