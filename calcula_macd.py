import pandas as pd
import pandas_ta as pdta
import ta
import numpy as np
from datetime import datetime
import itertools

def macd_data(df, fast, slow, signal):
    # Compute MACD difference
    df_macd = ta.trend.macd_diff(df['Close'], slow, fast, signal)
    
    # Generate the column name for the current combination of parameters
    column_name = f's{slow}_f{fast}_sig{signal}'
    
    # Use vectorized operation to set values
    df[column_name] = (df_macd > 0) & (df_macd.shift() <= 0)
    
    return df

# Start the timer
now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV file into a pandas DataFrame
Banco = "BTCUSDT_2023_1min"
df = pd.read_csv(f'C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Binance\\{Banco}.csv')

# MACD parameters
macd_fast = [8, 12, 13]
macd_slow = [17, 21, 26]
macd_signal = [5, 7, 9, 11]

# Use itertools.product to replace nested loops
for fast, slow, signal in itertools.product(macd_fast, macd_slow, macd_signal):
    macd_data(df, fast, slow, signal)
    print(f'Processing fast={fast}, slow={slow}, signal={signal}')

# End the timer
now2 = datetime.now()
final = now2.strftime("%H:%M:%S")

# Drop unnecessary columns
df.drop(columns=['Unnamed: 0', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'], axis=1, inplace=True)

# Output start and end times
print("Hora de inicio: ", inicio)
print("Hora de termino: ", final)

# Save the DataFrame to CSV
df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Indicadores\\macd.csv', index=False)
