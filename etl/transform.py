import pandas as pd
import duckdb
import pathlib

pd.set_option('display.max_columns', None)

RAW_CSV = r"E:\ETL-project\data\raw\coin_extract.csv"
DB_PATH = r"E:\ETL-project\data\processed\coins.duckdb"
PROCESSED_DIR = pathlib.Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def transform(csv_path: str = RAW_CSV):
    print("loading data into frame...")
    df = pd.read_csv(csv_path)
    df = df.drop(columns=[
        "roi",
        "atl_date",
        "atl_change_percentage",
        "ath_date",
        "ath_change_percentage",
        "fully_diluted_valuation",
        "image",
    ], errors="ignore")

    df["max_supply"] = (
        df["max_supply"]
        .astype(str)
        .str.replace(r"[^0-9\.]", "", regex=True)  # فقط عدد و نقطه مجاز
        .replace("nan", "", regex=True)  # حذف nan
        .replace("", pd.NA)  # تبدیل رشته خالی به NaN
    )
    df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce").apply(lambda x: f"{x:.2f}")
    df["total_supply"] = df["total_supply"].astype(str).replace(".0","",regex=True)
    df["circulating_supply"] = df["circulating_supply"].astype(str).replace(".0","",regex=True)
    df["market_cap_change_24h"] = df["market_cap_change_24h"].apply(lambda x: format(float(x), "g"))
    df["low_24h"] = df["low_24h"].apply(lambda x: format(float(x), "g"))
    df["high_24h"] = df["high_24h"].apply(lambda x: format(float(x), "g"))
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["max_supply"] = df["max_supply"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) else pd.NA
    )

    df["ath"] = df["ath"].apply(lambda x: ('%.10g' % x))
    df["atl"] = df["atl"].apply(lambda x: ('%.10g' % x))

    df["last_updated"] = pd.to_datetime(df["last_updated"]).dt.strftime("%Y-%m-%d")
    try:
        df.to_csv(r"E:\ETL-project\data\processed\clean_coins.csv")
        print("saved coin's cleaned data to clean_coins.csv")
    except Exception as e:
        print(e)


def load(csv_path: str, db_path: str):
    print("loading cleaned data into DuckDB...")

    try:
        # اتصال به دیتابیس
        con = duckdb.connect(db_path)

        # ساخت جدول (در صورتی که وجود نداشت)
        con.execute("""
            CREATE TABLE IF NOT EXISTS coins (
                id TEXT,
                symbol TEXT,
                name TEXT,
                current_price DOUBLE,
                market_cap DOUBLE,
                market_cap_rank INTEGER,
                total_volume DOUBLE,
                high_24h DOUBLE,
                low_24h DOUBLE,
                price_change_24h DOUBLE,
                price_change_percentage_24h DOUBLE,
                market_cap_change_24h TEXT,
                market_cap_change_percentage_24h DOUBLE,
                circulating_supply TEXT,
                total_supply TEXT,
                max_supply TEXT,
                ath TEXT,
                atl TEXT,
                last_updated DATE
            );
        """)

        # بارگذاری CSV داخل DuckDB
        con.execute(f"""
            CREATE OR REPLACE TABLE coins AS
            SELECT * FROM read_csv_auto('{csv_path}');
        """)

        print("data successfully loaded into DuckDB database!")

    except Exception as e:
        print("error loading data:", e)


transform()
load(
    csv_path=r"E:\ETL-project\data\processed\clean_coins.csv",
    db_path=DB_PATH
)
