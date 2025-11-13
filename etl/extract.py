# extract file
import pathlib
import pandas as pd
import requests
import time
from typing import List, Dict

BASE = "https://api.coingecko.com/api/v3/coins/markets"
DATA_RAW = pathlib.Path("data/raw")
DATA_RAW.mkdir(parents=True, exist_ok=True)

def fetch_market(vs_currency="usd",per_page=250,page=1,extra_params=None) -> List[Dict]:
        params = {"vs_currency": vs_currency, "per_page": per_page, "page": page,"order": "market_cap_desc","sparkline":False}
        if extra_params:
            params.update(extra_params)
        r = requests.get(BASE,params=params)
        r.raise_for_status()
        return r.json()


def fetch_all_top_n(vs_currency="usd",per_page=250,top_n=500,delay=1.0,extra_params=None):
        out= []
        pages = (top_n + per_page - 1) // per_page
        for page in range(1,pages+1):
            print(f"Fetching page {page}/{pages}...")
            data = fetch_market(vs_currency=vs_currency,per_page=per_page,page=page)
            out.extend(data)
            time.sleep(delay)
        print(out)
        return out


def save_raw_data( db_path="data/raw/coin_extract.csv",top_n=500):
    print("fetching data raw from API...")
    items = fetch_all_top_n(vs_currency="usd",per_page=250,top_n=top_n,delay=1.0)
    df = pd.DataFrame(items)
    df.to_csv(db_path, index=False)
    print(f"Saved {len(df)} records to {db_path}")



save_raw_data()