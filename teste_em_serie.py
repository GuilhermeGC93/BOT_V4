import pandas as pd
import pandas_ta as pdta
import ta
import numpy as np
from datetime import datetime
import time as time



def add_purchase_instants(df, variation, steps):
    df['v_' + str(variation) + '_s_' + str(steps)] = False # Initialize purchase_instant column as 0
    # Iterate over the DataFrame rows
    i = 0
    while i < len(df) - steps:
        # Check if the maximum price in the following 60 instants is equal to or greater than 101% of the purchase instant price
        if df['Close'][i+1:i+steps+1].max() >= df['Close'][i] * variation:
            # Set the purchase_instant value as 1
            df.loc[i, 'v_' + str(variation) + '_s_' + str(steps)] = True
        i += 1

    # Count the number of purchase instants
    #df['purchase_count'] = df['purchase_instant'].cumsum()

    # Count the number of positive variations between instants
    #df['positive_variation_count'] = (df['purchase_instant'] > df['purchase_instant'].shift(1)).astype(int).cumsum()

    return df

now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV file into a pandas DataFrame
Banco = "BTCUSDT_2023_1min"
df = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Binance\\' + Banco + '.csv')

# Technical indicators parameters

variacao = [1.003, 1.004, 1.005, 1.0075, 1.01, 1.015, 1.02] # Lucro desejado
intervalo_subida = [60, 120, 240, 360, 720, 1440] # Tempo em minutos dentro do qual queremos que ocorra a subida de preço

# Calcula variacao
cont_variacao = 0
while cont_variacao < len(variacao):
    cont_intervalo_subida = 0
    while cont_intervalo_subida < len(intervalo_subida):
        add_purchase_instants(df, variacao[cont_variacao], intervalo_subida[cont_intervalo_subida])
        print("contador variacao: ", cont_variacao, "   contador intervalo_subida: ", cont_intervalo_subida)
        cont_intervalo_subida = cont_intervalo_subida + 1
    cont_variacao = cont_variacao + 1

df.drop(columns=['Unnamed: 0','Open','High','Low','Close','Volume','OpenInterest'], axis=1, inplace=True)

now2 = datetime.now()
final = now2.strftime("%H:%M:%S")

print("Hora de inicio: ", inicio)
print("hora de termino: ", final)
df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Compra\\Dados_Compra.csv', index=False)


time.sleep(10)




def macd_data(df, fast, slow, signal):
    df_macd = ta.trend.macd_diff(df['Close'], slow, fast, signal)  # MACD
    df['s' + str(slow) + '_f' + str(fast) + '_sig' + str(signal)] = (df_macd > 0) & (df_macd.shift() <= 0)
    return df

now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV file into a pandas DataFrame
Banco = "BTCUSDT_2023_1min"
df = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Binance\\' + Banco + '.csv')

# Technical indicators parameters

macd_fast = [12,13]
macd_slow = [26,28]
macd_signal = [9,10]

# Calcula macd
cont_macd_signal = 0
while cont_macd_signal < len(macd_signal):
    cont_macd_slow = 0
    while cont_macd_slow < len(macd_slow):
        cont_macd_fast = 0
        while cont_macd_fast < len(macd_fast):
            macd_data(df, macd_fast[cont_macd_fast], macd_slow[cont_macd_slow], macd_signal[cont_macd_signal])
            cont_macd_fast = cont_macd_fast + 1
            print("contador cont_macd_fast: ", cont_macd_fast,"   contador cont_macd_slow: ", cont_macd_slow, "   contador cont_macd_signal: ", cont_macd_signal )
        cont_macd_slow = cont_macd_slow + 1
    cont_macd_signal = cont_macd_signal + 1


now2 = datetime.now()
final = now2.strftime("%H:%M:%S")

df.drop(columns=['Unnamed: 0','Open','High','Low','Close','Volume','OpenInterest'], axis=1, inplace=True)

print("Hora de inicio: ", inicio)
print("hora de termino: ", final)
df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Indicadores\\macd.csv', index=False)


time.sleep(10)





now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV file into a pandas DataFrame
dados_compra = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Compra\\Dados_Compra.csv')
dados_indicadores = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Indicadores\\macd.csv')

# colunas_dados_compra = dados_compra.columns
# colunas_dados_indicadores = dados_indicadores.columns

# print(colunas_dados_compra)
# print(colunas_dados_indicadores)

comparison_df = pd.DataFrame()

for col1 in dados_compra.columns:
    for col2 in dados_indicadores.columns:
        # Compare each row between the two columns
        comparison_result = []
        for i in range(len(dados_compra)):
            # Compare values row by row between the two columns
            if dados_compra[col1][i] == True and dados_indicadores[col2][i] == True:
                comparison_result.append('B')
            elif dados_compra[col1][i] == False and dados_indicadores[col2][i] == True:
                comparison_result.append('R')
            else:
                comparison_result.append('N')
        print('col1: ', col1, '   col2: ', col2)
    
    # Create a new column name by combining the names of the columns being compared
        new_col_name = f'{col1}_{col2}'
    
    # Add the comparison result to the new dataframe
        comparison_df[new_col_name] = comparison_result

print(comparison_df)

comparison_df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Tratados\\macd_tratado.csv', index=False)


time.sleep(10)




# Load the data from the CSV file
df = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Tratados\\macd_tratado.csv')

# Create an empty list to store the results
results = []

# Iterate through each column in the dataframe
for col in df.columns:
    # Count occurrences of 'N', 'B', and 'R' in the current column
    n_count = (df[col] == 'N').sum()
    b_count = (df[col] == 'B').sum()
    r_count = (df[col] == 'R').sum()
    
    # Store the results in a list
    column_result = [col, n_count, b_count, r_count]
    
    # Append the list to the results list
    results.append(column_result)

# Create a new dataframe from the list of results
results_df = pd.DataFrame(results, columns=['Column', 'N_count', 'B_count', 'R_count'])



# Save the results to a new CSV file (optional)
results_df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Tratados\\resumo.csv', index=False)

# Display the result dataframe
print(results_df)
