import pandas as pd

path_query = r'C:\Taranis\05 Time data\All teams\Query.xlsx'
df_query = pd.read_excel(path_query, index_col=False)



df_query['qoutes'] = '''"'''+df_query['Key'] +'''"'''+','

print(df_query['qoutes'].to_string(index=False))
