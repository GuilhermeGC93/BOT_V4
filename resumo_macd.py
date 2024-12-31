import pandas as pd

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
