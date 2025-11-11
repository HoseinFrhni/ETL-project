import pandas as pd
from typing import List,Dict
pd.set_option('display.max_columns', None)

def normalize_market_items(csv_file_path:str):
    coin_data_frame = pd.read_csv(csv_file_path)
    coin_data_frame =coin_data_frame.drop_duplicates()
    coin_data_frame= coin_data_frame.drop(columns="Phone 2")
    print(coin_data_frame)


normalize_market_items(r"E:\ETL-project\data\raw\customers-100000.csv")

