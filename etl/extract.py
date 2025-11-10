import requests
import time
from typing import List, Dict

BASE = "https://api.coingecko.com/api/v3/coins/markets"

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


fetch_all_top_n(per_page=250,vs_currency="usd",top_n=500,delay=1.0)