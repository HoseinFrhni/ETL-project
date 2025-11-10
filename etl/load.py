# etl/load.py
import os
import pandas as pd
import duckdb
from sqlalchemy import create_engine

def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print("✅ Saved CSV:", path)


def save_sqlite(df: pd.DataFrame, sqlite_uri="sqlite:///data/processed/coingecko.db", table="markets"):
    os.makedirs("data/processed", exist_ok=True)
    engine = create_engine(sqlite_uri)
    df.to_sql(table, con=engine, if_exists="replace", index=False)
    print("✅ Saved to SQLite:", sqlite_uri, "table:", table)


def save_duckdb(df: pd.DataFrame, db_path="data/processed/coingecko.duckdb", table="markets"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")  # ایجاد ساختار اولیه
    conn.register("temp_df", df)
    conn.execute(f"DELETE FROM {table}")  # جایگزینی داده‌ها
    conn.execute(f"INSERT INTO {table} SELECT * FROM temp_df")
    conn.close()
    print("✅ Saved to DuckDB:", db_path, "table:", table)
