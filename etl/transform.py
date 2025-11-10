import pandas as pd
from typing import List,Dict


def normalize_market_items(items:List[Dict]) ->pd.DataFrame:
    rows = []
    for item in items:
        rows.append({
            "id":item.get("id"),
            "symbol":item.get("symbol"),
            "name":item.get("name"),
            "currency_price":item.get("currency_price"),
            "market_cap":item.get("market_cap"),
            "total_volume":item.get("total_volume"),
            "price_change_24h":item.get("price_change_24h"),
            "price_change_pct_24h":item.get("price_change_pct_24h"),
            "last_updated":item.get("last_updated")
        })
    df = pd.DataFrame(rows)
    return df
