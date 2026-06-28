from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
DIVIDENDS_DB = APP_DIR / "data" / "dividends.db"
US_UNIVERSE_CSV = APP_DIR / "data" / "us_universe.csv"
EUROPE_UNIVERSE_CSV = APP_DIR / "data" / "europe_etf_universe.csv"
SEC_PRICES_DB = APP_DIR.parent / "sec_data" / "prices.db"
STRATEGY_PRICE_CACHE_DB = APP_DIR / "data" / "strategy_price_cache.db"


def classify_us_asset(asset_type: object) -> str:
    text = str(asset_type or "").upper()
    if any(token in text for token in ("ETF", "FUND", "ETN")):
        return "USA ETF"
    return "USA Stock"


def load_universe() -> pd.DataFrame:
    frames = []
    if US_UNIVERSE_CSV.exists():
        us = pd.read_csv(US_UNIVERSE_CSV).fillna("")
        us["bucket"] = us["asset_type"].map(classify_us_asset)
        frames.append(us[["ticker", "bucket"]])
    if EUROPE_UNIVERSE_CSV.exists():
        eu = pd.read_csv(EUROPE_UNIVERSE_CSV).fillna("")
        eu["bucket"] = "Europe ETF"
        frames.append(eu[["ticker", "bucket"]])
    if not frames:
        return pd.DataFrame(columns=["ticker", "bucket"])
    universe = pd.concat(frames, ignore_index=True)
    universe["ticker"] = universe["ticker"].astype(str).str.upper().str.strip()
    return universe.drop_duplicates("ticker")


def dividend_coverage(universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if not DIVIDENDS_DB.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DIVIDENDS_DB)
    try:
        events = pd.read_sql_query(
            """
            SELECT ticker, ex_dividend_date, pay_date, cash_amount, currency, source
            FROM dividend_events
            WHERE ex_dividend_date>=? AND ex_dividend_date<=?
            """,
            conn,
            params=(start, end),
        )
    finally:
        conn.close()
    if events.empty:
        return pd.DataFrame()
    events["ticker"] = events["ticker"].astype(str).str.upper().str.strip()
    events = events.merge(universe, on="ticker", how="left")
    events["bucket"] = events["bucket"].fillna("Not in universe")
    return events.groupby("bucket", dropna=False).agg(
        events=("ticker", "count"),
        tickers=("ticker", "nunique"),
        pay_dates=("pay_date", lambda s: int(s.notna().sum())),
        missing_pay_dates=("pay_date", lambda s: int(s.isna().sum())),
        min_ex=("ex_dividend_date", "min"),
        max_ex=("ex_dividend_date", "max"),
    )


def price_coverage_from_db(db_path: Path, universe: pd.DataFrame, start: str, end: str, table: str) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    date_col = "date" if table == "daily_prices" and db_path == SEC_PRICES_DB else "price_date"
    conn = sqlite3.connect(db_path)
    try:
        prices = pd.read_sql_query(
            f"""
            SELECT ticker, COUNT(*) AS rows, MIN({date_col}) AS min_date, MAX({date_col}) AS max_date
            FROM {table}
            WHERE {date_col}>=? AND {date_col}<=?
            GROUP BY ticker
            """,
            conn,
            params=(start, end),
        )
    finally:
        conn.close()
    prices["ticker"] = prices["ticker"].astype(str).str.upper().str.strip()
    merged = universe.merge(prices, on="ticker", how="left")
    return merged.groupby("bucket", dropna=False).agg(
        universe=("ticker", "nunique"),
        priced_tickers=("rows", lambda s: int(s.notna().sum())),
        price_rows=("rows", "sum"),
        min_date=("min_date", "min"),
        max_date=("max_date", "max"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audita cobertura de dividendos y precios diarios.")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default=date.today().isoformat())
    args = parser.parse_args()

    universe = load_universe()
    print(f"Rango: {args.start}..{args.end}")
    print("\nUNIVERSO")
    print(universe.groupby("bucket").ticker.nunique().to_string())

    print("\nEVENTOS DIVIDENDOS")
    dividends = dividend_coverage(universe, args.start, args.end)
    print(dividends.to_string() if not dividends.empty else "Sin eventos")

    print("\nPRECIOS DIARIOS sec_data/prices.db")
    sec_prices = price_coverage_from_db(SEC_PRICES_DB, universe, args.start, args.end, "daily_prices")
    print(sec_prices.to_string() if not sec_prices.empty else "Sin precios SEC")

    print("\nPRECIOS DIARIOS data/strategy_price_cache.db")
    cache_prices = price_coverage_from_db(STRATEGY_PRICE_CACHE_DB, universe, args.start, args.end, "daily_prices")
    print(cache_prices.to_string() if not cache_prices.empty else "Sin cache de estrategia")


if __name__ == "__main__":
    main()
