import pandas as pd
from etl.load import save_duckdb
import duckdb
# data = {
#     "symbol" : ["btc","eth","xrp"],
#     "price": [70000, 3500, 0.58]
# }
# df = pd.DataFrame(data)
#
# # ذخیره در DuckDB
# save_duckdb(df)

connection = duckdb.connect("E:\ETL-project\data\processed\coingecko.duckdb")
print(connection.execute("SELECT * FROM markets LIMIT 10").fetchdf())
connection.close()