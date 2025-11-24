import yfinance as yf
import pandas as pd
import sqlite3

TICKER = "ASML"

# Buscar balance sheet anual
ticker = yf.Ticker(TICKER)
bs = ticker.balance_sheet.transpose()  # anos como linhas

# Criar / ligar à base de dados SQLite
conn = sqlite3.connect("teste1.db")

# Guardar o DataFrame numa tabela chamada "balance_sheet"
bs.to_sql("balance_sheet", conn, if_exists="replace", index=True)

conn.close()

print("Base de dados 'teste1.db' criada com sucesso!")
