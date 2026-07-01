from __future__ import annotations

import argparse
import concurrent.futures
import json
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import pandas as pd
except ModuleNotFoundError as exc:
    if exc.name != "pandas":
        raise
    print("Falta pandas en este Python. Instala dependencias con:")
    print("  python -m pip install -r requirements.txt")
    sys.exit(1)

import dividend_calendar_pipeline as pipeline


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DIVIDENDS_DB = DATA_DIR / "dividends.db"
PRICE_CACHE_DB = DATA_DIR / "strategy_price_cache.db"
SEC_PRICES_DB = APP_DIR.parent / "sec_data" / "prices.db"
CAPTURE_EVENTS_CSV = DATA_DIR / "capture_event_results.csv"
CAPTURE_TICKER_SIGNAL_CSV = DATA_DIR / "capture_ticker_signal.csv"
CAPTURE_SEGMENT_SIGNAL_CSV = DATA_DIR / "capture_segment_signal.csv"
YAHOO_PRICE_TIMEOUT = 10
_price_cache_lock = threading.Lock()


@dataclass(frozen=True)
class CaptureSettings:
    start: str
    end: str
    max_recovery_days: int = 90
    entry_lag_days: int = 1
    use_high_for_recovery: bool = False
    min_dividend_yield_pct: float = 0.0
    limit_tickers: int = 0


def to_unix(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


def from_unix(ts: int) -> str:
    return datetime.utcfromtimestamp(int(ts)).date().isoformat()


def init_price_cache() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(PRICE_CACHE_DB)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_prices (
                ticker TEXT NOT NULL,
                price_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume REAL,
                currency TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (ticker, price_date)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker_date ON daily_prices(ticker, price_date)")
        conn.commit()
    finally:
        conn.close()


def read_cached_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    if not PRICE_CACHE_DB.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(PRICE_CACHE_DB)
    try:
        return pd.read_sql_query(
            """
            SELECT ticker, price_date, open, high, low, close, adj_close, volume, currency
            FROM daily_prices
            WHERE ticker=? AND price_date>=? AND price_date<=?
            ORDER BY price_date
            """,
            conn,
            params=(ticker, start, end),
        )
    finally:
        conn.close()


def read_sec_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    if not SEC_PRICES_DB.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(SEC_PRICES_DB)
    try:
        df = pd.read_sql_query(
            """
            SELECT ticker, date AS price_date, open, high, low, close,
                   adj_close, volume, '' AS currency
            FROM daily_prices
            WHERE ticker=? AND date>=? AND date<=?
            ORDER BY date
            """,
            conn,
            params=(ticker.upper().strip(), start, end),
        )
    finally:
        conn.close()
    return df


def has_price_coverage(df: pd.DataFrame, start: str, end: str) -> bool:
    if df.empty or "price_date" not in df.columns:
        return False
    dates = pd.to_datetime(df["price_date"], errors="coerce").dropna()
    if dates.empty:
        return False
    # Trading calendars have weekends/holidays; allow a small edge gap.
    return dates.min() <= pd.Timestamp(start) + pd.Timedelta(days=7) and dates.max() >= pd.Timestamp(end) - pd.Timedelta(days=7)


def write_cached_prices(df: pd.DataFrame) -> None:
    if df.empty:
        return
    init_price_cache()
    rows = []
    fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for row in df.itertuples(index=False):
        rows.append(
            (
                row.ticker,
                row.price_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.adj_close,
                row.volume,
                row.currency,
                fetched_at,
            )
        )
    with _price_cache_lock:
        conn = sqlite3.connect(PRICE_CACHE_DB)
        try:
            conn.executemany(
                """
                INSERT INTO daily_prices (
                    ticker, price_date, open, high, low, close, adj_close, volume, currency, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker, price_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    adj_close=excluded.adj_close,
                    volume=excluded.volume,
                    currency=excluded.currency,
                    fetched_at=excluded.fetched_at
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()


def fetch_yahoo_prices(ticker: str, start: str, end: str, refresh: bool = False, progress: bool = False) -> pd.DataFrame:
    init_price_cache()
    sec_prices = read_sec_prices(ticker, start, end)
    if not refresh and has_price_coverage(sec_prices, start, end):
        if progress:
            print(f"  precios {ticker}: sec_data/prices.db ({len(sec_prices)} dias)", flush=True)
        return sec_prices

    cached = read_cached_prices(ticker, start, end)
    if not refresh and not cached.empty:
        if has_price_coverage(cached, start, end):
            if progress:
                print(f"  precios {ticker}: cache estrategia ({len(cached)} dias)", flush=True)
            return cached

    if progress:
        print(f"  precios {ticker}: descargando Yahoo", flush=True)
    symbol = pipeline.yahoo_symbol(ticker)
    params = {
        "period1": to_unix(start),
        "period2": to_unix(end) + 86400,
        "interval": "1d",
        "includeAdjustedClose": "true",
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{urlencode(params)}"
    with urlopen(Request(url, headers=pipeline.HTTP_HEADERS), timeout=YAHOO_PRICE_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"Yahoo prices status {response.status} for {ticker}")
        payload = json.loads(response.read().decode("utf-8"))

    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return cached
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adj = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    currency = (result.get("meta") or {}).get("currency") or ""
    rows = []
    for i, ts in enumerate(timestamps):
        close = (quote.get("close") or [None] * len(timestamps))[i]
        if close is None:
            continue
        rows.append(
            {
                "ticker": ticker,
                "price_date": from_unix(ts),
                "open": (quote.get("open") or [None] * len(timestamps))[i],
                "high": (quote.get("high") or [None] * len(timestamps))[i],
                "low": (quote.get("low") or [None] * len(timestamps))[i],
                "close": close,
                "adj_close": adj[i] if i < len(adj) else None,
                "volume": (quote.get("volume") or [None] * len(timestamps))[i],
                "currency": currency,
            }
        )
    fetched = pd.DataFrame(rows)
    write_cached_prices(fetched)
    combined = pd.concat([sec_prices, cached, fetched], ignore_index=True, sort=False)
    if combined.empty:
        return combined
    return combined.drop_duplicates(["ticker", "price_date"], keep="last").sort_values("price_date")


def load_dividend_events(settings: CaptureSettings) -> pd.DataFrame:
    conn = sqlite3.connect(DIVIDENDS_DB)
    try:
        df = pd.read_sql_query(
            """
            SELECT ticker, company_name, exchange, sector, asset_type,
                   ex_dividend_date, pay_date, cash_amount, currency, source
            FROM dividend_events
            WHERE ex_dividend_date>=? AND ex_dividend_date<=?
              AND cash_amount IS NOT NULL AND cash_amount>0
            ORDER BY ex_dividend_date, ticker
            """,
            conn,
            params=(settings.start, settings.end),
        )
    finally:
        conn.close()
    if settings.limit_tickers:
        keep = sorted(df["ticker"].dropna().unique().tolist())[: settings.limit_tickers]
        df = df[df["ticker"].isin(keep)].copy()
    return df


def previous_trading_row(prices: pd.DataFrame, ex_date: str, lag_days: int) -> pd.Series | None:
    ex_ts = pd.Timestamp(ex_date)
    candidates = prices[pd.to_datetime(prices["price_date"]) < ex_ts].copy()
    if candidates.empty:
        return None
    candidates = candidates.sort_values("price_date")
    offset = max(1, lag_days)
    if len(candidates) < offset:
        return candidates.iloc[0]
    return candidates.iloc[-offset]


def first_recovery_row(prices: pd.DataFrame, ex_date: str, target_price: float, max_days: int, use_high: bool) -> pd.Series | None:
    ex_ts = pd.Timestamp(ex_date)
    limit_ts = ex_ts + pd.Timedelta(days=max_days)
    window = prices[(pd.to_datetime(prices["price_date"]) >= ex_ts) & (pd.to_datetime(prices["price_date"]) <= limit_ts)].copy()
    if window.empty:
        return None
    price_col = "high" if use_high else "close"
    recovered = window[pd.to_numeric(window[price_col], errors="coerce") >= target_price]
    if recovered.empty:
        return None
    return recovered.sort_values("price_date").iloc[0]


def analyze_event(event: pd.Series, prices: pd.DataFrame, settings: CaptureSettings) -> dict | None:
    entry = previous_trading_row(prices, str(event.ex_dividend_date), settings.entry_lag_days)
    if entry is None or not entry.get("close"):
        return None
    entry_price = float(entry["close"])
    dividend = float(event.cash_amount)
    dividend_yield = dividend / entry_price if entry_price > 0 else 0.0
    if dividend_yield * 100 < settings.min_dividend_yield_pct:
        return None

    recovery = first_recovery_row(
        prices,
        str(event.ex_dividend_date),
        entry_price,
        settings.max_recovery_days,
        settings.use_high_for_recovery,
    )
    entry_date = str(entry["price_date"])
    ex_date = str(event.ex_dividend_date)
    ex_row = prices[pd.to_datetime(prices["price_date"]) >= pd.Timestamp(ex_date)].sort_values("price_date").head(1)
    ex_close = float(ex_row.iloc[0]["close"]) if not ex_row.empty and pd.notna(ex_row.iloc[0]["close"]) else None
    drop_pct = ((ex_close - entry_price) / entry_price * 100) if ex_close else None

    recovered = recovery is not None
    recovery_date = str(recovery["price_date"]) if recovered else ""
    exit_price = float(recovery["close"]) if recovered else None
    holding_days = max(1, (pd.Timestamp(recovery_date) - pd.Timestamp(entry_date)).days) if recovered else None
    price_return = ((exit_price - entry_price) / entry_price) if exit_price else None
    total_return = (price_return + dividend_yield) if price_return is not None else None
    annualized = (total_return * 365 / holding_days) if total_return is not None and holding_days else None

    return {
        "ticker": event.ticker,
        "company_name": event.company_name,
        "asset_type": event.asset_type,
        "exchange": event.exchange,
        "sector": event.sector,
        "ex_dividend_date": ex_date,
        "pay_date": event.pay_date,
        "cash_amount": dividend,
        "currency": event.currency,
        "entry_date": entry_date,
        "entry_price": entry_price,
        "ex_close": ex_close,
        "ex_drop_pct": drop_pct,
        "recovered": recovered,
        "recovery_date": recovery_date,
        "holding_days": holding_days,
        "exit_price": exit_price,
        "dividend_yield_pct": dividend_yield * 100,
        "price_return_pct": price_return * 100 if price_return is not None else None,
        "total_return_pct": total_return * 100 if total_return is not None else None,
        "annualized_return_pct": annualized * 100 if annualized is not None else None,
    }


def run_capture_backtest(
    settings: CaptureSettings,
    refresh_prices: bool = False,
    max_events: int = 0,
    progress: bool = False,
    workers: int = 1,
) -> pd.DataFrame:
    events = load_dividend_events(settings)
    if events.empty:
        if progress:
            print("No hay eventos de dividendos para el rango.", flush=True)
        return pd.DataFrame()
    events["ex_dividend_date"] = pd.to_datetime(events["ex_dividend_date"]).dt.date.astype(str)
    tickers = events["ticker"].dropna().astype(str).unique().tolist()
    if progress:
        print(
            f"Backtest capture: eventos={len(events):,} tickers={len(tickers):,} "
            f"rango={settings.start}..{settings.end} max_recovery={settings.max_recovery_days}d",
            flush=True,
        )
    rows = []
    price_start = (pd.Timestamp(settings.start) - pd.Timedelta(days=14)).date().isoformat()
    price_end = (pd.Timestamp(settings.end) + pd.Timedelta(days=settings.max_recovery_days + 7)).date().isoformat()

    def process_ticker(index: int, ticker: str) -> tuple[int, str, list[dict], str]:
        try:
            prices = fetch_yahoo_prices(ticker, price_start, price_end, refresh=refresh_prices, progress=False)
        except Exception as exc:
            return index, ticker, [], f"ERR prices {ticker}: {exc}"
        if prices.empty:
            return index, ticker, [], f"sin precios {ticker}"
        ticker_rows = []
        sub = events[events["ticker"] == ticker]
        for event in sub.itertuples(index=False):
            row = analyze_event(event, prices, settings)
            if row:
                ticker_rows.append(row)
        return index, ticker, ticker_rows, ""

    worker_count = max(1, int(workers or 1))
    if worker_count > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(process_ticker, i, ticker): (i, ticker)
                for i, ticker in enumerate(tickers, start=1)
            }
            for completed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                i, ticker = futures[future]
                try:
                    _idx, _ticker, ticker_rows, message = future.result()
                except Exception as exc:
                    print(f"ERR worker {ticker}: {exc}", flush=True)
                    continue
                if message:
                    print(message, flush=True)
                rows.extend(ticker_rows)
                if progress:
                    recovered = sum(1 for row in ticker_rows if row.get("recovered"))
                    print(
                        f"[{completed}/{len(tickers)}] {ticker}: eventos={len(events[events['ticker'] == ticker])} "
                        f"recuperados={recovered} acumulados={len(rows)}",
                        flush=True,
                    )
                if max_events and len(rows) >= max_events:
                    if progress:
                        print(f"Limite max_events alcanzado: {max_events}", flush=True)
                    break
        return pd.DataFrame(rows)

    for i, ticker in enumerate(tickers, start=1):
        if progress:
            print(f"[{i}/{len(tickers)}] {ticker}: eventos={len(events[events['ticker'] == ticker])}", flush=True)
        try:
            prices = fetch_yahoo_prices(ticker, price_start, price_end, refresh=refresh_prices, progress=progress)
        except Exception as exc:
            print(f"ERR prices {ticker}: {exc}")
            continue
        if prices.empty:
            if progress:
                print(f"  sin precios {ticker}", flush=True)
            continue
        sub = events[events["ticker"] == ticker]
        for event in sub.itertuples(index=False):
            row = analyze_event(event, prices, settings)
            if row:
                rows.append(row)
                if max_events and len(rows) >= max_events:
                    if progress:
                        print(f"Limite max_events alcanzado: {max_events}", flush=True)
                    return pd.DataFrame(rows)
        if progress:
            recovered = sum(1 for row in rows if row.get("ticker") == ticker and row.get("recovered"))
            print(f"  completado {ticker}: recuperados={recovered} acumulados={len(rows)}", flush=True)
        if i % 25 == 0:
            time.sleep(0.25)
    return pd.DataFrame(rows)


def summarize_by_ticker(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame()
    recovered = results[results["recovered"]].copy()
    grouped = results.groupby("ticker", as_index=False).agg(
        company_name=("company_name", "last"),
        asset_type=("asset_type", "last"),
        exchange=("exchange", "last"),
        sector=("sector", "last"),
        currency=("currency", "last"),
        events=("ticker", "count"),
        recovered_events=("recovered", "sum"),
        recovery_rate_pct=("recovered", lambda x: float(x.mean() * 100)),
        avg_dividend_yield_pct=("dividend_yield_pct", "mean"),
        median_dividend_yield_pct=("dividend_yield_pct", "median"),
        avg_ex_drop_pct=("ex_drop_pct", "mean"),
        latest_ex_dividend_date=("ex_dividend_date", "max"),
    )
    if recovered.empty:
        grouped["median_recovery_days"] = None
        grouped["avg_annualized_return_pct"] = None
        grouped["risk_adjusted_tae_pct"] = None
        return grouped
    rec_stats = recovered.groupby("ticker", as_index=False).agg(
        median_recovery_days=("holding_days", "median"),
        avg_recovery_days=("holding_days", "mean"),
        std_recovery_days=("holding_days", "std"),
        avg_annualized_return_pct=("annualized_return_pct", "mean"),
        median_annualized_return_pct=("annualized_return_pct", "median"),
        best_annualized_return_pct=("annualized_return_pct", "max"),
        std_annualized_return_pct=("annualized_return_pct", "std"),
    )
    out = grouped.merge(rec_stats, on="ticker", how="left")
    out["risk_adjusted_tae_pct"] = out["avg_annualized_return_pct"].fillna(0) * out["recovery_rate_pct"].fillna(0) / 100
    out["expected_tae_pct"] = out["risk_adjusted_tae_pct"]
    out["speed_score"] = (100 - out["median_recovery_days"].clip(lower=0, upper=100)).fillna(0)
    out["security_score"] = out["recovery_rate_pct"].fillna(0)
    out["stability_score"] = (100 - out["std_recovery_days"].fillna(100).clip(lower=0, upper=100)).fillna(0)
    out["capture_score"] = (
        out["security_score"] * 0.45
        + out["speed_score"] * 0.30
        + out["stability_score"] * 0.15
        + out["avg_dividend_yield_pct"].fillna(0).clip(upper=10) * 1.0
    )
    out["speed_cluster"] = out["median_recovery_days"].map(classify_speed)
    out["safety_cluster"] = out["recovery_rate_pct"].map(classify_safety)
    out["stability_cluster"] = out["std_recovery_days"].map(classify_stability)
    out["capture_cluster"] = out.apply(classify_capture_cluster, axis=1)
    return out.sort_values(
        ["recovery_rate_pct", "median_recovery_days", "avg_dividend_yield_pct"],
        ascending=[False, True, False],
    )


def classify_speed(days: object) -> str:
    if pd.isna(days):
        return "Sin recuperacion"
    days = float(days)
    if days <= 7:
        return "Muy rapida"
    if days <= 21:
        return "Rapida"
    if days <= 60:
        return "Media"
    return "Lenta"


def classify_safety(rate: object) -> str:
    if pd.isna(rate):
        return "Sin datos"
    rate = float(rate)
    if rate >= 90:
        return "Alta"
    if rate >= 70:
        return "Media-alta"
    if rate >= 50:
        return "Media"
    return "Baja"


def classify_stability(std_days: object) -> str:
    if pd.isna(std_days):
        return "Sin datos"
    std_days = float(std_days)
    if std_days <= 5:
        return "Muy estable"
    if std_days <= 15:
        return "Estable"
    if std_days <= 35:
        return "Variable"
    return "Muy variable"


def classify_capture_cluster(row: pd.Series) -> str:
    safety = row.get("recovery_rate_pct")
    speed = row.get("median_recovery_days")
    stability = row.get("std_recovery_days")
    tae = row.get("risk_adjusted_tae_pct")
    if pd.isna(safety) or float(safety) < 50:
        return "Especulativo"
    if pd.notna(speed) and float(speed) <= 21 and pd.notna(safety) and float(safety) >= 80:
        if pd.notna(tae) and float(tae) >= 25:
            return "Rapido y rentable"
        return "Rapido defensivo"
    if pd.notna(stability) and float(stability) <= 15 and pd.notna(safety) and float(safety) >= 70:
        return "Estable"
    if pd.notna(tae) and float(tae) >= 25:
        return "Rentable pero lento"
    return "Observacion"


def summarize_by_segment(ticker_signal: pd.DataFrame) -> pd.DataFrame:
    if ticker_signal.empty:
        return pd.DataFrame()
    dimensions = ["asset_type", "exchange", "sector", "speed_cluster", "safety_cluster", "stability_cluster", "capture_cluster"]
    rows = []
    for dimension in dimensions:
        if dimension not in ticker_signal.columns:
            continue
        grouped = ticker_signal.groupby(dimension, dropna=False)
        for value, group in grouped:
            if str(value) in ("", "nan", "None"):
                continue
            rows.append(
                {
                    "dimension": dimension,
                    "segment": value,
                    "tickers": group["ticker"].nunique(),
                    "events": group["events"].sum(),
                    "avg_recovery_rate_pct": group["recovery_rate_pct"].mean(),
                    "median_recovery_days": group["median_recovery_days"].median(),
                    "avg_dividend_yield_pct": group["avg_dividend_yield_pct"].mean(),
                    "avg_expected_tae_pct": group["expected_tae_pct"].mean(),
                    "avg_capture_score": group["capture_score"].mean(),
                    "top_ticker": group.sort_values("capture_score", ascending=False).iloc[0]["ticker"],
                }
            )
    return pd.DataFrame(rows).sort_values(["dimension", "avg_capture_score"], ascending=[True, False])


def save_capture_signal(results: pd.DataFrame, min_events: int = 2) -> tuple[pd.DataFrame, pd.DataFrame]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if results.empty:
        pd.DataFrame().to_csv(CAPTURE_EVENTS_CSV, index=False)
        pd.DataFrame().to_csv(CAPTURE_TICKER_SIGNAL_CSV, index=False)
        pd.DataFrame().to_csv(CAPTURE_SEGMENT_SIGNAL_CSV, index=False)
        return pd.DataFrame(), pd.DataFrame()
    results.to_csv(CAPTURE_EVENTS_CSV, index=False)
    ticker_signal = summarize_by_ticker(results)
    if min_events > 1 and not ticker_signal.empty:
        ticker_signal = ticker_signal[ticker_signal["events"] >= min_events].copy()
    segment_signal = summarize_by_segment(ticker_signal)
    ticker_signal.to_csv(CAPTURE_TICKER_SIGNAL_CSV, index=False)
    segment_signal.to_csv(CAPTURE_SEGMENT_SIGNAL_CSV, index=False)
    return ticker_signal, segment_signal


def simulate_reinvestment(results: pd.DataFrame, capital: float = 1000.0) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame()
    candidates = results[results["recovered"]].copy()
    if candidates.empty:
        return pd.DataFrame()
    candidates["entry_ts"] = pd.to_datetime(candidates["entry_date"])
    candidates["exit_ts"] = pd.to_datetime(candidates["recovery_date"])
    candidates = candidates.sort_values(["entry_ts", "annualized_return_pct"], ascending=[True, False])
    free_at = pd.Timestamp.min
    current_capital = float(capital)
    trades = []
    for row in candidates.itertuples(index=False):
        if row.entry_ts < free_at:
            continue
        shares = current_capital / float(row.entry_price)
        dividend_cash = shares * float(row.cash_amount)
        exit_value = shares * float(row.exit_price)
        pnl = exit_value - current_capital + dividend_cash
        current_capital += pnl
        free_at = row.exit_ts
        trades.append(
            {
                "entry_date": row.entry_date,
                "exit_date": row.recovery_date,
                "ticker": row.ticker,
                "entry_price": row.entry_price,
                "exit_price": row.exit_price,
                "shares": shares,
                "dividend_cash": dividend_cash,
                "holding_days": row.holding_days,
                "trade_return_pct": pnl / (current_capital - pnl) * 100,
                "capital_after": current_capital,
            }
        )
    return pd.DataFrame(trades)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest de compra pre ex-date y venta al recuperar precio.")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--max-recovery-days", type=int, default=90)
    parser.add_argument("--min-dividend-yield-pct", type=float, default=0.0)
    parser.add_argument("--limit-tickers", type=int, default=0)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--capital", type=float, default=1000.0)
    parser.add_argument("--refresh-prices", action="store_true")
    parser.add_argument("--use-high-for-recovery", action="store_true")
    parser.add_argument("--save-signal", action="store_true", help="Guarda senales para la app en data/capture_*_signal.csv")
    parser.add_argument("--min-signal-events", type=int, default=2, help="Eventos minimos por ticker para incluirlo en la senal")
    parser.add_argument("--workers", type=int, default=1, help="Tickers en paralelo para acelerar el backtest")
    args = parser.parse_args()

    settings = CaptureSettings(
        start=args.start,
        end=args.end,
        max_recovery_days=args.max_recovery_days,
        min_dividend_yield_pct=args.min_dividend_yield_pct,
        limit_tickers=args.limit_tickers,
        use_high_for_recovery=args.use_high_for_recovery,
    )
    results = run_capture_backtest(
        settings,
        refresh_prices=args.refresh_prices,
        max_events=args.max_events,
        progress=True,
        workers=args.workers,
    )
    print(f"events_analyzed={len(results)} recovered={int(results['recovered'].sum()) if not results.empty else 0}")
    if not results.empty:
        summary = summarize_by_ticker(results)
        print(summary.head(20).to_string(index=False))
        if args.save_signal:
            ticker_signal, segment_signal = save_capture_signal(results, min_events=args.min_signal_events)
            print("\nSenal guardada")
            print(f"ticker_signal={CAPTURE_TICKER_SIGNAL_CSV} rows={len(ticker_signal)}")
            print(f"segment_signal={CAPTURE_SEGMENT_SIGNAL_CSV} rows={len(segment_signal)}")
            if not segment_signal.empty:
                print("\nTop segmentos")
                print(segment_signal.head(20).to_string(index=False))
        trades = simulate_reinvestment(results, capital=args.capital)
        if not trades.empty:
            print("\nSimulacion reinversion")
            print(trades.tail(10).to_string(index=False))
            print(f"capital_final={trades.iloc[-1]['capital_after']:.2f}")


if __name__ == "__main__":
    main()
