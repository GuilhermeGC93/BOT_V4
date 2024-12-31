import pandas as pd
from datetime import datetime
import numpy as np

now = datetime.now()
inicio = now.strftime("%H:%M:%S")

# Read the CSV files into pandas DataFrames
dados_compra = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Compra\\Dados_Compra.csv')
dados_indicadores = pd.read_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Indicadores\\macd.csv')

# Initialize a dictionary to collect the results for all column comparisons
comparison_dict = {}

# Loop over columns in both DataFrames and compare them
for col1 in dados_compra.columns:
    for col2 in dados_indicadores.columns:
        print('col1: ', col1, '   col2: ', col2)
        
        # Vectorized row comparison using NumPy for better performance
        compra_true = dados_compra[col1].values
        indicadores_true = dados_indicadores[col2].values
        
        # Apply the conditions in a vectorized manner
        comparison_result = np.where(
            (compra_true == True) & (indicadores_true == True), 'B', 
            np.where((compra_true == False) & (indicadores_true == True), 'R', 'N')
        )
        
        # Create the new column name by combining the names of the columns being compared
        new_col_name = f'{col1}_{col2}'
        
        # Store the result in the dictionary
        comparison_dict[new_col_name] = comparison_result

# Now perform the analysis on comparison_dict

# Initialize an empty list to store the analysis results
results = []

# Iterate through each column in the comparison dictionary
for col_name, comparison_result in comparison_dict.items():
    # Count occurrences of 'N', 'B', and 'R' in the current column
    n_count = (comparison_result == 'N').sum()
    b_count = (comparison_result == 'B').sum()
    r_count = (comparison_result == 'R').sum()
    
    # Calculate the b_ratio
    b_ratio = b_count / (b_count + r_count) if (b_count + r_count) > 0 else 0
    
    # Store the results in a list (Column name, N_count, B_count, R_count, b_ratio)
    column_result = [col_name, n_count, b_count, r_count, b_ratio]
    
    # Append the list to the results list
    results.append(column_result)

# Convert results into a pandas DataFrame for easy handling and display
results_df = pd.DataFrame(results, columns=['Column', 'N_count', 'B_count', 'R_count', 'b_ratio'])

# Sort the DataFrame by b_ratio in descending order
results_df.sort_values(by='b_ratio', ascending=False, inplace=True)

# Save the results to a new CSV file
results_df.to_csv('C:\\Users\\guilh\\OneDrive\\Área de Trabalho\\BOT_2024\\bot_v4\\Dados_Tratados\\resumo.csv', index=False)

# Display a message confirming that the results have been saved
print("Results have been saved to resumo.csv")
