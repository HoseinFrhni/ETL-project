import pandas as pd
from typing import List, Dict

from sqlalchemy.util import column_set

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def normalize_market_items(csv_file_path: str):
    coin_data_frame = pd.read_csv(csv_file_path)
    coin_data_frame = coin_data_frame.drop_duplicates()
    coin_data_frame = coin_data_frame.drop(columns="Phone 2")
    # clean last name column use strip with somthing like regex :)
    coin_data_frame["Last Name"] = coin_data_frame["Last Name"].str.strip("123./_@&,")
    # clean Phone 1 column with replace method for delete characters
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('[^a-zA-Z0-9]', '', regex=True)
    # transform column to str
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].apply(lambda x: str(x))
    # add '-' between each 3 characters
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].apply(lambda x: x[0:3] + '-' + x[3:6] + '-' + x[6:10])
    # in continue we clean records with NAN & nan & na & NA & NA-- & na-- ,...
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('na--', '', regex=True)
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('Na--', '', regex=True)
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('NA--', '', regex=True)
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('nA--', '', regex=True)
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('Nan', '', regex=True)
    coin_data_frame["Phone 1"] = coin_data_frame["Phone 1"].str.replace('NaN', '', regex=True)

    print(coin_data_frame.head(50))


normalize_market_items(r"E:\ETL-project\data\raw\customers-100000.csv")
