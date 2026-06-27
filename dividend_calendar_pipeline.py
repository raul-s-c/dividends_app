"""
Dividend calendar pipeline.

Builds a local dividends database for USA-listed companies using the existing
SEC project fundamentals database as the ticker universe.

The current extractor stores ex-dividend dates and cash amounts from Yahoo
chart corporate-action events. SEC/EDGAR is still used as the company universe
and metadata source. Future iterations can add a paid corporate-actions
provider for pay dates and commercial-grade validation.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


APP_DIR = Path(__file__).resolve().parent
ROOT = APP_DIR.parent
SOURCE_DATA_DIR = ROOT / "sec_data"
FUNDAMENTALS_DB = SOURCE_DATA_DIR / "fundamentals.db"
DATA_DIR = APP_DIR / "data"
DIVIDENDS_DB = DATA_DIR / "dividends.db"
UNIVERSE_CSV = DATA_DIR / "us_universe.csv"

REQ_TIMEOUT = 16
YAHOO_SLEEP = 0.08
DEFAULT_EXCHANGES = ("NYSE", "Nasdaq", "CBOE")

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 dividend-calendar-research/0.1",
    "Accept": "application/json,text/plain,*/*",
}

_request_lock = threading.Lock()
_last_request = 0.0
_db_lock = threading.Lock()

EVENT_COLS = [
    "ticker",
    "company_name",
    "exchange",
    "sector",
    "state",
    "asset_type",
    "ex_dividend_date",
    "record_date",
    "pay_date",
    "declaration_date",
    "cash_amount",
    "currency",
    "frequency",
    "distribution_type",
    "status",
    "source",
    "source_event_id",
    "updated_at",
]

EVENT_INSERT_SQL = """
    INSERT INTO dividend_events (
        ticker, company_name, exchange, sector, state, asset_type,
        ex_dividend_date, record_date, pay_date, declaration_date, cash_amount,
        currency, frequency, distribution_type, status, source,
        source_event_id, updated_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(ticker, source_event_id) DO UPDATE SET
        company_name=excluded.company_name,
        exchange=excluded.exchange,
        sector=excluded.sector,
        state=excluded.state,
        asset_type=excluded.asset_type,
        ex_dividend_date=excluded.ex_dividend_date,
        record_date=excluded.record_date,
        pay_date=excluded.pay_date,
        declaration_date=excluded.declaration_date,
        cash_amount=excluded.cash_amount,
        currency=excluded.currency,
        frequency=excluded.frequency,
        distribution_type=excluded.distribution_type,
        status=excluded.status,
        source=excluded.source,
        updated_at=excluded.updated_at
"""


@dataclass(frozen=True)
class Company:
    ticker: str
    name: str
    exchange: str
    sector: str
    state: str
    asset_type: str = "Stock"


def yahoo_symbol(ticker: str) -> str:
    return ticker.strip().upper().replace(".", "-")


def to_unix_day(value: str) -> int:
    dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def from_unix_day(value: int | float) -> str:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date().isoformat()


def parse_us_date(value: object) -> str | None:
    if value in (None, "", "N/A"):
        return None
    text = str(value).strip()
    for fmt in ("%m/%d/%Y", "%-m/%-d/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    parts = text.split("/")
    if len(parts) == 3:
        try:
            month, day, year = [int(p) for p in parts]
            return date(year, month, day).isoformat()
        except ValueError:
            return None
    return None


def iter_days(start_date: str, end_date: str) -> Iterable[date]:
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    stop = datetime.strptime(end_date, "%Y-%m-%d").date()
    while current < stop:
        yield current
        current += timedelta(days=1)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def rolling_window_dates(rebuild: bool = False, lookback_days: int = 95, forward_days: int = 550) -> tuple[str, str]:
    today = date.today()
    if rebuild:
        return f"{today.year - 1}-01-01", f"{today.year + 2}-01-01"
    return (today - timedelta(days=lookback_days)).isoformat(), (today + timedelta(days=forward_days)).isoformat()


def classify_unmatched_asset_type(company_name: str) -> str:
    text = company_name.upper()
    if any(token in text for token in (" ETF", " EXCHANGE TRADED FUND", " ETN", " INDEX FUND")):
        return "ETF/Fund"
    if any(token in text for token in (" FUND", " TRUST", " CLOSED END", " CLOSED-END")):
        return "ETF/Fund"
    if "PREFERRED" in text or " PREFERENCE " in text or "DEPOSITARY SHARES" in text:
        return "Preferred"
    if " NOTE" in text or " NOTES " in text or " BOND" in text or " DEBENTURE" in text:
        return "Note/Bond"
    return "Other Unmatched"


def get_conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DIVIDENDS_DB, timeout=60)
    conn.execute("pragma journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dividend_events (
            ticker TEXT NOT NULL,
            company_name TEXT,
            exchange TEXT,
            sector TEXT,
            state TEXT,
            asset_type TEXT,
            ex_dividend_date TEXT NOT NULL,
            record_date TEXT,
            pay_date TEXT,
            declaration_date TEXT,
            cash_amount REAL NOT NULL,
            currency TEXT,
            frequency TEXT,
            distribution_type TEXT,
            status TEXT,
            source TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ticker, source_event_id)
        )
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(dividend_events)").fetchall()}
    if "asset_type" not in cols:
        conn.execute("ALTER TABLE dividend_events ADD COLUMN asset_type TEXT")
    backfill_asset_types(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dividend_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            source TEXT NOT NULL,
            tickers_requested INTEGER NOT NULL DEFAULT 0,
            events_upserted INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dividend_events_ex_date ON dividend_events(ex_dividend_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dividend_events_ticker ON dividend_events(ticker)")
    conn.commit()
    return conn


def backfill_asset_types(conn: sqlite3.Connection) -> None:
    missing = conn.execute(
        "SELECT COUNT(*) FROM dividend_events WHERE asset_type IS NULL OR asset_type=''"
    ).fetchone()[0]
    if not missing:
        return
    if UNIVERSE_CSV.exists():
        with UNIVERSE_CSV.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                ticker = str(row.get("ticker") or "").strip().upper()
                asset_type = str(row.get("asset_type") or "Stock").strip() or "Stock"
                if ticker:
                    conn.execute(
                        """
                        UPDATE dividend_events
                        SET asset_type=?
                        WHERE ticker=? AND (asset_type IS NULL OR asset_type='')
                        """,
                        (asset_type, ticker),
                    )
    conn.execute(
        """
        UPDATE dividend_events
        SET asset_type='Other Unmatched'
        WHERE asset_type IS NULL OR asset_type=''
        """
    )


def load_companies_from_csv(
    ticker: str | None = None,
    exchanges: Iterable[str] = DEFAULT_EXCHANGES,
    limit: int | None = None,
) -> list[Company]:
    if not UNIVERSE_CSV.exists():
        if ticker:
            return [Company(ticker=ticker.strip().upper(), name="", exchange="", sector="", state="", asset_type="Unknown")]
        raise FileNotFoundError(f"Missing source database {FUNDAMENTALS_DB} and universe file {UNIVERSE_CSV}")

    exchange_set = {x.strip() for x in exchanges if x.strip()}
    companies: list[Company] = []
    seen: set[str] = set()
    with UNIVERSE_CSV.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            clean_ticker = str(row.get("ticker") or "").strip().upper()
            if not clean_ticker or clean_ticker in seen:
                continue
            if ticker and clean_ticker != ticker.strip().upper():
                continue
            if not ticker and exchange_set and str(row.get("exchange") or "").strip() not in exchange_set:
                continue
            seen.add(clean_ticker)
            companies.append(
                Company(
                    ticker=clean_ticker,
                    name=str(row.get("name") or ""),
                    exchange=str(row.get("exchange") or ""),
                    sector=str(row.get("sector") or ""),
                    state=str(row.get("state") or ""),
                    asset_type=str(row.get("asset_type") or "Stock"),
                )
            )
            if limit and len(companies) >= limit:
                break
    if ticker and not companies:
        companies.append(Company(ticker=ticker.strip().upper(), name="", exchange="", sector="", state="", asset_type="Unknown"))
    return companies


def load_companies(
    ticker: str | None = None,
    exchanges: Iterable[str] = DEFAULT_EXCHANGES,
    limit: int | None = None,
) -> list[Company]:
    if not FUNDAMENTALS_DB.exists():
        return load_companies_from_csv(ticker=ticker, exchanges=exchanges, limit=limit)

    exchange_list = [x.strip() for x in exchanges if x.strip()]
    params: list[object] = []
    where = ["ticker IS NOT NULL", "ticker <> ''"]
    if ticker:
        where.append("UPPER(ticker)=UPPER(?)")
        params.append(ticker)
    elif exchange_list:
        where.append("exchange IN (" + ",".join("?" for _ in exchange_list) + ")")
        params.extend(exchange_list)

    sql = f"""
        SELECT ticker, name, exchange, sector, state
        FROM companies
        WHERE {" AND ".join(where)}
        ORDER BY ticker
    """
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))

    conn = sqlite3.connect(FUNDAMENTALS_DB)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    companies: list[Company] = []
    seen: set[str] = set()
    for ticker_value, name, exchange, sector, state in rows:
        clean_ticker = str(ticker_value or "").strip().upper()
        if not clean_ticker or clean_ticker in seen:
            continue
        seen.add(clean_ticker)
        companies.append(
            Company(
                ticker=clean_ticker,
                name=str(name or ""),
                exchange=str(exchange or ""),
                sector=str(sector or ""),
                state=str(state or ""),
                asset_type="Stock",
            )
        )
    if ticker and not companies:
        companies.append(Company(ticker=ticker.strip().upper(), name="", exchange="", sector="", state="", asset_type="Unknown"))
    return companies


def throttle() -> None:
    global _last_request
    with _request_lock:
        gap = YAHOO_SLEEP - (time.perf_counter() - _last_request)
        if gap > 0:
            time.sleep(gap + random.uniform(0, 0.03))
        _last_request = time.perf_counter()


def fetch_yahoo_dividends(company: Company, start_date: str, end_date: str) -> list[dict]:
    throttle()
    symbol = yahoo_symbol(company.ticker)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "period1": to_unix_day(start_date),
        "period2": to_unix_day(end_date),
        "interval": "1d",
        "events": "div",
        "includeAdjustedClose": "true",
    }
    request = Request(url + "?" + urlencode(params), headers=HTTP_HEADERS)
    with urlopen(request, timeout=REQ_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"Yahoo status {response.status}")
        payload = json.loads(response.read().decode("utf-8"))
    result = payload.get("chart", {}).get("result", [None])[0]
    if not result:
        return []
    currency = (result.get("meta") or {}).get("currency")
    dividends = ((result.get("events") or {}).get("dividends") or {}).values()
    rows: list[dict] = []
    refreshed_at = now_utc()
    today = date.today().isoformat()
    for item in dividends:
        raw_date = item.get("date")
        amount = item.get("amount")
        if raw_date is None or amount is None:
            continue
        ex_date = from_unix_day(raw_date)
        if ex_date < start_date or ex_date >= end_date:
            continue
        event_id = f"yahoo:{symbol}:{ex_date}:{float(amount):.8f}"
        rows.append(
            {
                "ticker": company.ticker,
                "company_name": company.name,
                "exchange": company.exchange,
                "sector": company.sector,
                "state": company.state,
                "asset_type": company.asset_type,
                "ex_dividend_date": ex_date,
                "record_date": None,
                "pay_date": None,
                "declaration_date": None,
                "cash_amount": float(amount),
                "currency": currency,
                "frequency": None,
                "distribution_type": "cash",
                "status": "historical" if ex_date < today else "announced",
                "source": "yahoo_chart_dividends",
                "source_event_id": event_id,
                "updated_at": refreshed_at,
            }
        )
    return rows


def fetch_nasdaq_calendar(day: date, companies_by_ticker: dict[str, Company], include_unmatched: bool = False) -> list[dict]:
    url = "https://api.nasdaq.com/api/calendar/dividends"
    params = {"date": day.isoformat()}
    headers = {
        **HTTP_HEADERS,
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/dividends",
    }
    request = Request(url + "?" + urlencode(params), headers=headers)
    with urlopen(request, timeout=REQ_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"Nasdaq status {response.status}")
        payload = json.loads(response.read().decode("utf-8"))

    rows = (((payload.get("data") or {}).get("calendar") or {}).get("rows") or [])
    events: list[dict] = []
    refreshed_at = now_utc()
    today = date.today().isoformat()
    for item in rows:
        ticker = str(item.get("symbol") or "").strip().upper().replace(".", "-")
        if not ticker:
            continue
        company = companies_by_ticker.get(ticker)
        if company is None and not include_unmatched:
            continue
        ex_date = parse_us_date(item.get("dividend_Ex_Date")) or day.isoformat()
        amount = item.get("dividend_Rate")
        try:
            amount_value = float(amount)
        except (TypeError, ValueError):
            continue
        company_name = str(item.get("companyName") or "")
        inferred_asset_type = classify_unmatched_asset_type(company_name) if company is None else company.asset_type
        event_id = f"nasdaq:{ticker}:{ex_date}:{amount_value:.8f}:{parse_us_date(item.get('payment_Date')) or ''}"
        events.append(
            {
                "ticker": ticker,
                "company_name": company.name if company and company.name else company_name,
                "exchange": company.exchange if company else None,
                "sector": company.sector if company else None,
                "state": company.state if company else None,
                "asset_type": inferred_asset_type,
                "ex_dividend_date": ex_date,
                "record_date": parse_us_date(item.get("record_Date")),
                "pay_date": parse_us_date(item.get("payment_Date")),
                "declaration_date": parse_us_date(item.get("announcement_Date")),
                "cash_amount": amount_value,
                "currency": "USD",
                "frequency": None,
                "distribution_type": "cash",
                "status": "historical" if ex_date < today else "announced",
                "source": "nasdaq_calendar",
                "source_event_id": event_id,
                "updated_at": refreshed_at,
            }
        )
    return events


def upsert_events(rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [tuple(row.get(col) for col in EVENT_COLS) for row in rows]
    with _db_lock:
        conn = get_conn()
        conn.executemany(EVENT_INSERT_SQL, values)
        conn.commit()
        conn.close()
    return len(rows)


def replace_source_window(rows: list[dict], source: str, start_date: str, end_date: str) -> int:
    values = [tuple(row.get(col) for col in EVENT_COLS) for row in rows]
    with _db_lock:
        conn = get_conn()
        conn.execute("BEGIN")
        conn.execute(
            """
            DELETE FROM dividend_events
            WHERE source=? AND ex_dividend_date >= ? AND ex_dividend_date < ?
            """,
            (source, start_date, end_date),
        )
        if values:
            conn.executemany(EVENT_INSERT_SQL, values)
        conn.commit()
        conn.close()
    return len(rows)


def run_yahoo(
    start_date: str,
    end_date: str,
    ticker: str | None = None,
    workers: int = 8,
    limit: int | None = None,
    exchanges: Iterable[str] = DEFAULT_EXCHANGES,
) -> dict:
    companies = load_companies(ticker=ticker, exchanges=exchanges, limit=limit)
    conn = get_conn()
    run_id = now_utc()
    conn.execute(
        """
        INSERT INTO dividend_runs(run_id, started_at, start_date, end_date, source, tickers_requested, notes)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            run_id,
            run_id,
            start_date,
            end_date,
            "yahoo_chart_dividends",
            len(companies),
            "SEC fundamentals.db universe; Yahoo chart dividend events for ex-date and amount.",
        ),
    )
    conn.commit()
    conn.close()

    errors = 0
    events = 0
    buffer: list[dict] = []
    print(f"Dividend extraction: tickers={len(companies):,} range={start_date}..{end_date}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(fetch_yahoo_dividends, company, start_date, end_date): company for company in companies}
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            company = futures[future]
            completed += 1
            try:
                rows = future.result()
            except Exception as exc:
                errors += 1
                if errors <= 20:
                    print(f"ERR {company.ticker}: {exc}")
                rows = []
            if rows:
                buffer.extend(rows)
            if len(buffer) >= 500:
                events += upsert_events(buffer)
                buffer.clear()
            if completed % 250 == 0 or completed == len(companies):
                print(f"  processed={completed:,}/{len(companies):,} events={events + len(buffer):,} errors={errors:,}")
    events += upsert_events(buffer)

    conn = get_conn()
    conn.execute(
        """
        UPDATE dividend_runs
        SET finished_at=?, events_upserted=?, errors=?
        WHERE run_id=?
        """,
        (now_utc(), events, errors, run_id),
    )
    conn.commit()
    conn.close()
    return {"tickers": len(companies), "events": events, "errors": errors, "run_id": run_id}


def run_nasdaq_calendar(
    start_date: str,
    end_date: str,
    ticker: str | None = None,
    workers: int = 8,
    include_unmatched: bool = False,
    exchanges: Iterable[str] = DEFAULT_EXCHANGES,
    reconcile_window: bool = False,
) -> dict:
    companies = load_companies(ticker=ticker, exchanges=exchanges)
    companies_by_ticker = {company.ticker: company for company in companies}
    days = list(iter_days(start_date, end_date))
    conn = get_conn()
    run_id = now_utc()
    conn.execute(
        """
        INSERT INTO dividend_runs(run_id, started_at, start_date, end_date, source, tickers_requested, notes)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            run_id,
            run_id,
            start_date,
            end_date,
            "nasdaq_calendar",
            len(companies),
            "Nasdaq daily dividend calendar; incremental window replacement when enabled.",
        ),
    )
    conn.commit()
    conn.close()

    errors = 0
    events = 0
    buffer: list[dict] = []
    all_rows: list[dict] = []
    print(f"Nasdaq dividend calendar: days={len(days):,} universe={len(companies):,} range={start_date}..{end_date}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(fetch_nasdaq_calendar, day, companies_by_ticker, include_unmatched): day
            for day in days
        }
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            day = futures[future]
            completed += 1
            try:
                rows = future.result()
            except Exception as exc:
                errors += 1
                if errors <= 20:
                    print(f"ERR {day.isoformat()}: {exc}")
                rows = []
            if ticker and rows:
                rows = [row for row in rows if row["ticker"].upper() == ticker.upper()]
            if reconcile_window and rows:
                all_rows.extend(rows)
            elif rows:
                buffer.extend(rows)
            if not reconcile_window and len(buffer) >= 500:
                events += upsert_events(buffer)
                buffer.clear()
            if completed % 50 == 0 or completed == len(days):
                pending_events = len(all_rows) if reconcile_window else len(buffer)
                print(f"  processed={completed:,}/{len(days):,} events={events + pending_events:,} errors={errors:,}")
    if reconcile_window and errors == 0:
        events = replace_source_window(all_rows, "nasdaq_calendar", start_date, end_date)
    else:
        if reconcile_window and errors:
            print("Window replacement skipped because at least one calendar day failed; upserting successful rows only.")
            buffer.extend(all_rows)
        events += upsert_events(buffer)

    conn = get_conn()
    conn.execute(
        """
        UPDATE dividend_runs
        SET finished_at=?, events_upserted=?, errors=?
        WHERE run_id=?
        """,
        (now_utc(), events, errors, run_id),
    )
    conn.commit()
    conn.close()
    return {
        "days": len(days),
        "tickers": len(companies),
        "events": events,
        "errors": errors,
        "run_id": run_id,
        "reconciled_window": bool(reconcile_window and errors == 0),
    }


def load_events(start_date: str, end_date: str, ticker: str | None = None) -> list[dict]:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    params: list[object] = [start_date, end_date]
    where = "ex_dividend_date >= ? AND ex_dividend_date <= ?"
    if ticker:
        where += " AND UPPER(ticker)=UPPER(?)"
        params.append(ticker)
    rows = conn.execute(
        f"""
        SELECT *
        FROM dividend_events
        WHERE {where}
        ORDER BY ex_dividend_date, ticker
        """,
        params,
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", help="Exclusive end date YYYY-MM-DD")
    parser.add_argument("--ticker", help="Single ticker")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int, help="Limit ticker count for tests")
    parser.add_argument("--exchanges", default="NYSE,Nasdaq,CBOE", help="Comma-separated source exchanges")
    parser.add_argument("--source", choices=["yahoo", "nasdaq", "both"], default="nasdaq")
    parser.add_argument("--include-unmatched", action="store_true", help="Keep Nasdaq rows not found in local SEC universe")
    parser.add_argument("--incremental", action="store_true", help="Use a rolling window and replace only that source/date range")
    parser.add_argument("--rebuild", action="store_true", help="Use a broad moving rebuild range instead of the incremental window")
    parser.add_argument("--lookback-days", type=int, default=95, help="Incremental lookback window; default is roughly one quarter")
    parser.add_argument("--forward-days", type=int, default=550, help="Incremental forward window; default is about 18 months")
    args = parser.parse_args()
    exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()]
    default_start, default_end = rolling_window_dates(
        rebuild=args.rebuild,
        lookback_days=args.lookback_days,
        forward_days=args.forward_days,
    )
    start_date = args.start or default_start
    end_date = args.end or default_end
    results = []
    if args.source in ("yahoo", "both"):
        results.append(
            run_yahoo(
                start_date=start_date,
                end_date=end_date,
                ticker=args.ticker,
                workers=args.workers,
                limit=args.limit,
                exchanges=exchanges,
            )
        )
    if args.source in ("nasdaq", "both"):
        if args.limit:
            print("--limit only applies to the Yahoo ticker extractor; Nasdaq is date-based.")
        results.append(
            run_nasdaq_calendar(
                start_date=start_date,
                end_date=end_date,
                ticker=args.ticker,
                workers=args.workers,
                include_unmatched=args.include_unmatched,
                exchanges=exchanges,
                reconcile_window=args.incremental,
            )
        )
    result = results[-1] if len(results) == 1 else results
    print(f"Done: {result}")


if __name__ == "__main__":
    main()
