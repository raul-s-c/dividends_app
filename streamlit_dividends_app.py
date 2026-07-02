from __future__ import annotations

import csv
import calendar as month_calendar
import html
import json
import sqlite3
import subprocess
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

import dividend_capture_strategy as capture
import dividend_calendar_pipeline as pipeline


APP_DIR = Path(__file__).resolve().parent
PORTFOLIO_CSV = APP_DIR / "data" / "portfolio.csv"
US_UNIVERSE_CSV = APP_DIR / "data" / "us_universe.csv"
EUROPE_UNIVERSE_CSV = APP_DIR / "data" / "europe_etf_universe.csv"
CAPTURE_TICKER_SIGNAL_CSV = APP_DIR / "data" / "capture_ticker_signal.csv"
CAPTURE_SEGMENT_SIGNAL_CSV = APP_DIR / "data" / "capture_segment_signal.csv"
SEC_FUNDAMENTALS_DB = APP_DIR.parent / "sec_data" / "fundamentals.db"

st.set_page_config(page_title="Dividend Calendar USA", page_icon="Div", layout="wide")


def fmt_money(value, currency: str | None = "USD") -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "-"
    symbol = "$" if not currency or currency == "USD" else f"{currency} "
    return f"{symbol}{amount:,.2f}"


def load_portfolio() -> pd.DataFrame:
    if not PORTFOLIO_CSV.exists():
        return pd.DataFrame(columns=["ticker", "shares", "avg_cost", "notes"])
    df = pd.read_csv(PORTFOLIO_CSV).fillna("")
    df["ticker"] = df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
    df["shares"] = pd.to_numeric(df.get("shares", 0), errors="coerce").fillna(0.0)
    df["avg_cost"] = pd.to_numeric(df.get("avg_cost", 0), errors="coerce").fillna(0.0)
    if "notes" not in df.columns:
        df["notes"] = ""
    return df


def save_portfolio(df: pd.DataFrame) -> None:
    pipeline.DATA_DIR.mkdir(parents=True, exist_ok=True)
    clean = df.copy()
    clean["ticker"] = clean["ticker"].astype(str).str.upper().str.strip()
    clean = clean[clean["ticker"] != ""]
    clean["shares"] = pd.to_numeric(clean.get("shares", 0), errors="coerce").fillna(0.0)
    clean["avg_cost"] = pd.to_numeric(clean.get("avg_cost", 0), errors="coerce").fillna(0.0)
    clean.to_csv(PORTFOLIO_CSV, index=False)


@st.cache_data(ttl=300)
def load_universe() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if US_UNIVERSE_CSV.exists():
        us = pd.read_csv(US_UNIVERSE_CSV).fillna("")
        us["market_region"] = "USA"
        frames.append(us)
    if EUROPE_UNIVERSE_CSV.exists():
        europe = pd.read_csv(EUROPE_UNIVERSE_CSV).fillna("")
        europe = europe.rename(columns={"country": "state"})
        if "sector" not in europe.columns:
            europe["sector"] = ""
        europe["market_region"] = "Europa"
        frames.append(europe)
    if not frames:
        return pd.DataFrame(columns=["ticker", "isin", "name", "exchange", "sector", "state", "asset_type", "market_region"])
    universe = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    universe["ticker"] = universe["ticker"].astype(str).str.upper().str.strip()
    if "isin" not in universe.columns:
        universe["isin"] = ""
    universe["isin"] = universe["isin"].astype(str).str.upper().str.strip()
    universe["ticker_base"] = universe["ticker"].str.split(".").str[0]
    return universe.drop_duplicates(["ticker"], keep="first")


@st.cache_data(ttl=300)
def load_capture_ticker_signal() -> pd.DataFrame:
    if not CAPTURE_TICKER_SIGNAL_CSV.exists():
        return pd.DataFrame()
    signal = pd.read_csv(CAPTURE_TICKER_SIGNAL_CSV).fillna("")
    if "ticker" not in signal.columns:
        return pd.DataFrame()
    signal["ticker"] = signal["ticker"].astype(str).str.upper().str.strip()
    numeric_cols = [
        "events",
        "recovered_events",
        "latest_entry_price",
        "recovery_rate_pct",
        "avg_dividend_yield_pct",
        "median_recovery_days",
        "avg_recovery_days",
        "avg_annualized_return_pct",
        "risk_adjusted_tae_pct",
        "expected_tae_pct",
        "capture_score",
        "trend_adjusted_capture_score",
        "trend_adjusted_expected_tae_pct",
        "trend_risk_score",
        "trend_score_multiplier",
        "trend_return_3m_pct",
        "trend_return_6m_pct",
        "trend_vs_sma200_pct",
        "trend_drawdown_6m_pct",
    ]
    for col in numeric_cols:
        if col in signal.columns:
            signal[col] = pd.to_numeric(signal[col], errors="coerce")
    return signal.drop_duplicates("ticker", keep="first")


@st.cache_data(ttl=300)
def load_capture_segment_signal() -> pd.DataFrame:
    if not CAPTURE_SEGMENT_SIGNAL_CSV.exists():
        return pd.DataFrame()
    return pd.read_csv(CAPTURE_SEGMENT_SIGNAL_CSV).fillna("")


def sector_display(value, asset_type: str | None = "") -> str:
    label = sic_to_sector(value)
    if label:
        return label
    if asset_type and str(asset_type).strip():
        return str(asset_type).strip()
    return "Sin sector"


def sic_to_sector(sic_val) -> str:
    try:
        sic = int(float(str(sic_val)))
    except (ValueError, TypeError):
        return str(sic_val) if sic_val else ""
    if 100 <= sic <= 999:
        return "Agriculture"
    if 1000 <= sic <= 1499:
        return "Mining"
    if 1500 <= sic <= 1799:
        return "Construction"
    if 2000 <= sic <= 2099:
        return "Food & Beverage"
    if 2100 <= sic <= 2199:
        return "Tobacco"
    if 2200 <= sic <= 2399:
        return "Textiles & Apparel"
    if 2400 <= sic <= 2799:
        return "Paper & Publishing"
    if 2800 <= sic <= 2999:
        return "Chemicals"
    if 3000 <= sic <= 3399:
        return "Metals & Machinery"
    if 3400 <= sic <= 3499:
        return "Fabricated Metals"
    if 3500 <= sic <= 3599:
        return "Industrial Machinery"
    if 3600 <= sic <= 3699:
        return "Electronics"
    if 3700 <= sic <= 3799:
        return "Transportation Equipment"
    if 3800 <= sic <= 3999:
        return "Instruments & Misc Mfg"
    if 4000 <= sic <= 4499:
        return "Transportation"
    if 4500 <= sic <= 4899:
        return "Communications"
    if 4900 <= sic <= 4999:
        return "Utilities"
    if 5000 <= sic <= 5199:
        return "Wholesale Trade"
    if 5200 <= sic <= 5999:
        return "Retail Trade"
    if 6000 <= sic <= 6199:
        return "Banking"
    if 6200 <= sic <= 6299:
        return "Securities"
    if 6300 <= sic <= 6411:
        return "Insurance"
    if 6500 <= sic <= 6599:
        return "Real Estate"
    if 7000 <= sic <= 7299:
        return "Hotels & Personal Services"
    if 7370 <= sic <= 7379:
        return "Technology Services"
    if 7300 <= sic <= 7399:
        return "Business Services"
    if 7500 <= sic <= 7999:
        return "Entertainment & Recreation"
    if 8000 <= sic <= 8099:
        return "Healthcare"
    if 8100 <= sic <= 8999:
        return "Professional Services"
    return "Other"


@st.cache_data(ttl=600)
def load_sec_profiles_table() -> pd.DataFrame:
    if not SEC_FUNDAMENTALS_DB.exists():
        return pd.DataFrame(columns=["ticker_base", "sic_code", "sic_industry", "sic_sector"])
    conn = sqlite3.connect(SEC_FUNDAMENTALS_DB)
    try:
        df = pd.read_sql_query(
            """
            SELECT UPPER(ticker) AS ticker_base,
                   sector AS sic_code,
                   sic_description AS sic_industry
            FROM companies
            WHERE ticker IS NOT NULL AND ticker<>''
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame(columns=["ticker_base", "sic_code", "sic_industry", "sic_sector"])
    df["sic_sector"] = df["sic_code"].map(sic_to_sector)
    return df.drop_duplicates("ticker_base", keep="first")


def enrich_events(events_df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return events_df
    enriched = events_df.copy()
    for col in ["ticker", "isin", "company_name", "exchange", "sector", "asset_type", "state", "pay_date"]:
        if col not in enriched.columns:
            enriched[col] = ""
    if not universe_df.empty:
        meta_cols = ["ticker", "isin", "name", "exchange", "sector", "asset_type", "state", "market_region"]
        meta = universe_df[[c for c in meta_cols if c in universe_df.columns]].drop_duplicates("ticker")
        enriched = enriched.merge(meta, on="ticker", how="left", suffixes=("", "_universe"))
        enriched["company_name"] = enriched["company_name"].replace("", pd.NA).fillna(enriched.get("name", ""))
        for col in ["exchange", "sector", "asset_type", "state"]:
            ucol = f"{col}_universe"
            if ucol in enriched.columns:
                enriched[col] = enriched[col].replace("", pd.NA).fillna(enriched[ucol]).fillna("")
        enriched["market_region"] = enriched.get("market_region", "").fillna("")
        drop_cols = [c for c in ["name", "exchange_universe", "sector_universe", "asset_type_universe", "state_universe"] if c in enriched.columns]
        enriched = enriched.drop(columns=drop_cols)
    else:
        enriched["market_region"] = ""
    sec_profiles = load_sec_profiles_table()
    if not sec_profiles.empty:
        enriched["ticker_base"] = enriched["ticker"].astype(str).str.upper().str.split(".").str[0]
        enriched = enriched.merge(sec_profiles, on="ticker_base", how="left")
        enriched["sector"] = enriched["sector"].replace("", pd.NA).fillna(enriched.get("sic_code", "")).fillna("")
        enriched = enriched.drop(columns=["ticker_base"])
    else:
        enriched["sic_code"] = ""
        enriched["sic_industry"] = ""
        enriched["sic_sector"] = ""
    enriched["sector_label"] = enriched.apply(lambda row: sector_display(row.get("sector"), row.get("asset_type")), axis=1)
    enriched["pay_date_display"] = enriched["pay_date"].replace("", pd.NA).fillna("Pendiente")
    capture_signal = load_capture_ticker_signal()
    if not capture_signal.empty:
        signal_cols = [
            "ticker",
            "events",
            "recovered_events",
            "recovery_rate_pct",
            "median_recovery_days",
            "latest_entry_price",
            "avg_dividend_yield_pct",
            "expected_tae_pct",
            "risk_adjusted_tae_pct",
            "capture_score",
            "trend_adjusted_capture_score",
            "trend_adjusted_expected_tae_pct",
            "trend_risk_score",
            "trend_score_multiplier",
            "trend_return_3m_pct",
            "trend_return_6m_pct",
            "trend_vs_sma200_pct",
            "trend_drawdown_6m_pct",
            "speed_cluster",
            "safety_cluster",
            "stability_cluster",
            "trend_cluster",
            "capture_cluster",
        ]
        signal = capture_signal[[c for c in signal_cols if c in capture_signal.columns]].copy()
        signal = signal.rename(
            columns={
                "events": "capture_events",
                "recovered_events": "capture_recovered_events",
                "avg_dividend_yield_pct": "capture_avg_dividend_yield_pct",
            }
        )
        enriched = enriched.merge(signal, on="ticker", how="left")
    if "latest_entry_price" in enriched.columns:
        reference_price = pd.to_numeric(enriched["latest_entry_price"], errors="coerce")
        cash_amount = pd.to_numeric(enriched["cash_amount"], errors="coerce")
        enriched["event_yield_real_pct"] = (cash_amount / reference_price * 100).where(reference_price > 0)
    if "event_yield_real_pct" not in enriched.columns:
        enriched["event_yield_real_pct"] = pd.NA
    recovery_days = pd.to_numeric(enriched.get("median_recovery_days", pd.Series(dtype=float)), errors="coerce").replace(0, pd.NA)
    recovery_rate = pd.to_numeric(enriched.get("recovery_rate_pct", pd.Series(dtype=float)), errors="coerce").fillna(0)
    event_yield = pd.to_numeric(enriched["event_yield_real_pct"], errors="coerce")
    enriched["event_expected_tae_pct"] = event_yield * 365 / recovery_days * recovery_rate / 100
    trend_multiplier = pd.to_numeric(enriched.get("trend_score_multiplier", pd.Series(1.0, index=enriched.index)), errors="coerce").fillna(1.0)
    enriched["event_trend_adjusted_tae_pct"] = enriched["event_expected_tae_pct"] * trend_multiplier
    for col in ["expected_tae_pct", "capture_score", "trend_adjusted_capture_score", "event_trend_adjusted_tae_pct", "trend_risk_score", "trend_score_multiplier", "recovery_rate_pct", "median_recovery_days"]:
        if col not in enriched.columns:
            enriched[col] = pd.NA
    for col in ["speed_cluster", "safety_cluster", "stability_cluster", "trend_cluster", "capture_cluster"]:
        if col not in enriched.columns:
            enriched[col] = ""
    return enriched


@st.cache_data(ttl=300)
def load_sec_profile(ticker: str) -> dict:
    if not SEC_FUNDAMENTALS_DB.exists():
        return {}
    clean = str(ticker or "").upper().split(".")[0]
    conn = sqlite3.connect(SEC_FUNDAMENTALS_DB)
    try:
        row = conn.execute(
            """
            SELECT ticker, name, sector, exchange, state, sic_description,
                   entity_type, description, n_years, min_year, max_year
            FROM companies
            WHERE UPPER(ticker)=UPPER(?)
            LIMIT 1
            """,
            (clean,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    keys = [
        "ticker",
        "name",
        "sector",
        "exchange",
        "state",
        "sic_description",
        "entity_type",
        "description",
        "n_years",
        "min_year",
        "max_year",
    ]
    profile = dict(zip(keys, row))
    profile["sector_name"] = sic_to_sector(profile.get("sector"))
    return profile


def search_universe(universe_df: pd.DataFrame, query: str) -> pd.DataFrame:
    q = query.strip().upper()
    if not q or universe_df.empty:
        return pd.DataFrame()
    return universe_df[
        universe_df["ticker"].astype(str).str.upper().str.contains(q, regex=False)
        | universe_df["ticker_base"].astype(str).str.upper().str.contains(q, regex=False)
        | universe_df.get("isin", pd.Series("", index=universe_df.index)).astype(str).str.upper().str.contains(q, regex=False)
        | universe_df["name"].astype(str).str.upper().str.contains(q, regex=False)
    ].copy()


def resolve_unique_ticker(value: str, universe_df: pd.DataFrame) -> str:
    ticker = str(value or "").upper().strip()
    if not ticker or universe_df.empty:
        return ticker
    exact = universe_df[universe_df["ticker"].astype(str).str.upper() == ticker]
    if not exact.empty:
        return str(exact.iloc[0]["ticker"])
    base = universe_df[universe_df["ticker_base"].astype(str).str.upper() == ticker]
    options = sorted(base["ticker"].dropna().astype(str).unique().tolist())
    return options[0] if len(options) == 1 else ticker


def apply_portfolio_ticker_resolution(df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "ticker" not in df.columns:
        return df
    out = df.copy()
    out["ticker"] = out["ticker"].map(lambda value: resolve_unique_ticker(value, universe_df))
    return out


def selectable_ticker_table(df: pd.DataFrame, key: str, **kwargs) -> str:
    if df.empty or "ticker" not in df.columns:
        st.dataframe(df, **kwargs)
        return ""
    try:
        selection = st.dataframe(
            df,
            on_select="rerun",
            selection_mode="single-row",
            key=key,
            **kwargs,
        )
        rows = getattr(getattr(selection, "selection", None), "rows", [])
        if rows:
            return str(df.reset_index(drop=True).iloc[rows[0]]["ticker"])
    except TypeError:
        st.dataframe(df, **kwargs)
    return ""


@st.cache_data(ttl=3600)
def fetch_yahoo_dividend_snapshot(ticker: str) -> dict:
    today_value = date.today()
    end_day = today_value + timedelta(days=366)
    start_day = today_value - timedelta(days=365 * 5 + 2)
    symbol = pipeline.yahoo_symbol(ticker)
    params = {
        "period1": pipeline.to_unix_day(start_day.isoformat()),
        "period2": pipeline.to_unix_day(end_day.isoformat()),
        "interval": "1d",
        "events": "div",
        "includeAdjustedClose": "true",
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{urlencode(params)}"
    try:
        with urlopen(Request(url, headers=pipeline.HTTP_HEADERS), timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"events": [], "price": None, "currency": "", "error": str(exc)}

    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return {"events": [], "price": None, "currency": "", "error": "Sin respuesta Yahoo"}
    meta = result.get("meta") or {}
    currency = meta.get("currency") or ""
    price = meta.get("regularMarketPrice") or meta.get("previousClose")
    dividends = ((result.get("events") or {}).get("dividends") or {}).values()
    rows = []
    for item in dividends:
        raw_date = item.get("date")
        amount = item.get("amount")
        if raw_date is None or amount is None:
            continue
        rows.append(
            {
                "ex_dividend_date": pipeline.from_unix_day(raw_date),
                "cash_amount": float(amount),
                "currency": currency,
                "pay_date": None,
                "status": "historical",
                "source": "yahoo_chart_on_demand",
            }
        )
    return {"events": rows, "price": price, "currency": currency, "error": ""}


def dividend_history_for_ticker(ticker: str, local_events: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    cols = ["ex_dividend_date", "cash_amount", "currency", "pay_date", "status", "source"]
    local = pd.DataFrame(columns=cols)
    if not local_events.empty:
        local = local_events[local_events["ticker"].astype(str).str.upper() == ticker.upper()].copy()
        local = local[[c for c in cols if c in local.columns]]
    snapshot = fetch_yahoo_dividend_snapshot(ticker)
    remote = pd.DataFrame(snapshot.get("events") or [], columns=cols)
    combined = pd.concat([local, remote], ignore_index=True, sort=False).fillna("")
    if combined.empty:
        return combined, snapshot
    combined["ex_dividend_date"] = pd.to_datetime(combined["ex_dividend_date"], errors="coerce").dt.date
    combined["cash_amount"] = pd.to_numeric(combined["cash_amount"], errors="coerce").fillna(0)
    combined["pay_date_display"] = combined["pay_date"].replace("", pd.NA).fillna("Pendiente")
    combined = (
        combined.dropna(subset=["ex_dividend_date"])
        .sort_values(["ex_dividend_date", "cash_amount", "source"], ascending=[False, False, True])
        .drop_duplicates(["ex_dividend_date", "cash_amount"], keep="first")
    )
    return combined, snapshot


def render_dividend_analytics(ticker: str, events_df: pd.DataFrame) -> None:
    history, snapshot = dividend_history_for_ticker(ticker, events_df)
    price = snapshot.get("price")
    currency = snapshot.get("currency") or (history["currency"].replace("", pd.NA).dropna().iloc[0] if not history.empty and history["currency"].replace("", pd.NA).dropna().any() else "")

    st.markdown("**Dividendos**")
    if history.empty:
        error = snapshot.get("error")
        if error:
            st.info(f"No hay dividendos cargados y Yahoo no devolvio historico ahora: {error}")
        else:
            st.info("No hay dividendos cargados ni historico Yahoo para este instrumento.")
        return

    hist = history.copy()
    hist["date_ts"] = pd.to_datetime(hist["ex_dividend_date"])
    today_ts = pd.Timestamp(date.today())
    trailing_12m = hist[(hist["date_ts"] > today_ts - pd.DateOffset(months=12)) & (hist["date_ts"] <= today_ts)]["cash_amount"].sum()
    current_yield = (trailing_12m / float(price) * 100) if price else None

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rentabilidad actual", f"{current_yield:.2f}%" if current_yield is not None else "-")
    m2.metric("Dividendos 12 meses", fmt_money(trailing_12m, currency))
    m3.metric("Precio referencia", fmt_money(price, currency) if price else "-")
    m4.metric("Eventos historicos", f"{len(hist):,}")

    hist["year"] = hist["date_ts"].dt.year
    hist["month"] = hist["date_ts"].dt.month
    annual = hist.groupby("year", as_index=False)["cash_amount"].sum().sort_values("year", ascending=False)
    current_year = date.today().year
    annual["yield_base_amount"] = annual.apply(
        lambda row: trailing_12m if int(row["year"]) == current_year else row["cash_amount"],
        axis=1,
    )
    annual["yield_on_current_price"] = annual["yield_base_amount"].map(lambda x: (x / float(price) * 100) if price else None)
    annual_show = annual.rename(
        columns={
            "year": "Periodo",
            "cash_amount": f"Dividendo en {currency or 'moneda'}",
            "yield_on_current_price": "Rentabilidad sobre precio actual %",
        }
    )
    annual_show = annual_show[["Periodo", f"Dividendo en {currency or 'moneda'}", "Rentabilidad sobre precio actual %"]]
    if "Rentabilidad sobre precio actual %" in annual_show:
        annual_show["Rentabilidad sobre precio actual %"] = annual_show["Rentabilidad sobre precio actual %"].map(
            lambda x: f"{x:.2f}%" if pd.notna(x) else "-"
        )

    left, right = st.columns([1.05, 1])
    with left:
        st.markdown("**Rentabilidad historica de los dividendos**")
        st.dataframe(annual_show, use_container_width=True, hide_index=True)
    with right:
        chart_df = annual.sort_values("year").rename(columns={"year": "Ano", "cash_amount": "Dividendos"})
        st.markdown("**Contribucion anual**")
        st.bar_chart(chart_df, x="Ano", y="Dividendos")

    monthly = (
        hist.groupby(["year", "month"], as_index=False)["cash_amount"].sum()
        .pivot(index="year", columns="month", values="cash_amount")
        .sort_index(ascending=False)
    )
    month_names = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sept", "oct", "nov", "dic"]
    monthly = monthly.reindex(columns=range(1, 13))
    monthly.columns = month_names
    st.markdown("**Dividendos mensuales**")
    st.dataframe(monthly.fillna(""), use_container_width=True)

    st.markdown("**Eventos de dividendo**")
    st.dataframe(
        hist[["ex_dividend_date", "cash_amount", "currency", "pay_date_display", "status", "source"]].rename(columns={"pay_date_display": "pay_date"}),
        use_container_width=True,
        hide_index=True,
    )


@st.cache_data(ttl=300)
def load_local_price_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    clean = str(ticker or "").upper().strip()
    if not clean:
        return pd.DataFrame()
    frames = []
    sec_prices = capture.read_sec_prices(clean, start, end)
    if not sec_prices.empty:
        frames.append(sec_prices)
    cached_prices = capture.read_cached_prices(clean, start, end)
    if not cached_prices.empty:
        frames.append(cached_prices)
    if not frames:
        return pd.DataFrame()
    prices = pd.concat(frames, ignore_index=True, sort=False)
    prices["price_date"] = pd.to_datetime(prices["price_date"], errors="coerce")
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["price_date", "close"])
    if prices.empty:
        return prices
    return prices.sort_values("price_date").drop_duplicates("price_date", keep="last")


def mini_price_svg(prices: pd.DataFrame, ex_dates: list[pd.Timestamp], width: int = 300, height: int = 130) -> str:
    if prices.empty:
        return "<div class='ticker-hover-empty'>Sin precios locales cacheados</div>"
    chart = prices.copy().sort_values("price_date")
    if len(chart) < 2:
        return "<div class='ticker-hover-empty'>Historico insuficiente</div>"
    min_date = chart["price_date"].min()
    max_date = chart["price_date"].max()
    min_price = float(chart["close"].min())
    max_price = float(chart["close"].max())
    if max_price <= min_price:
        max_price = min_price + 1
    total_seconds = max(1, (max_date - min_date).total_seconds())
    left, right, top, bottom = 12, width - 10, 10, height - 22
    x_span = right - left
    y_span = bottom - top

    def x_for(ts: pd.Timestamp) -> float:
        return left + ((ts - min_date).total_seconds() / total_seconds) * x_span

    def y_for(price: float) -> float:
        return bottom - ((price - min_price) / (max_price - min_price)) * y_span

    points = " ".join(f"{x_for(row.price_date):.1f},{y_for(float(row.close)):.1f}" for row in chart.itertuples())
    lines = []
    for ex_date in ex_dates:
        if pd.isna(ex_date) or ex_date < min_date or ex_date > max_date:
            continue
        x = x_for(ex_date)
        lines.append(f"<line x1='{x:.1f}' y1='{top}' x2='{x:.1f}' y2='{bottom}' class='ex-line' />")
    first = chart.iloc[0]
    last = chart.iloc[-1]
    return (
        f"<svg viewBox='0 0 {width} {height}' class='ticker-hover-svg' role='img'>"
        f"<rect x='0' y='0' width='{width}' height='{height}' rx='6' class='svg-bg' />"
        f"<line x1='{left}' y1='{bottom}' x2='{right}' y2='{bottom}' class='axis' />"
        f"<line x1='{left}' y1='{top}' x2='{left}' y2='{bottom}' class='axis' />"
        f"{''.join(lines)}"
        f"<polyline points='{points}' class='price-line' />"
        f"<text x='{left}' y='{height - 6}' class='axis-label'>{html.escape(str(first['price_date'].date()))}</text>"
        f"<text x='{right}' y='{height - 6}' text-anchor='end' class='axis-label'>{html.escape(str(last['price_date'].date()))}</text>"
        f"<text x='{right}' y='{top + 10}' text-anchor='end' class='price-label'>{last['close']:.2f}</text>"
        f"</svg>"
    )


def ticker_hover_card_html(ticker: str, row, events_df: pd.DataFrame, chart_end: date) -> str:
    clean = str(ticker or "").upper().strip()
    if not clean:
        return ""
    start = (pd.Timestamp(chart_end) - pd.DateOffset(months=12)).date().isoformat()
    end = pd.Timestamp(chart_end).date().isoformat()
    prices = load_local_price_history(clean, start, end)
    ticker_events = events_df[events_df["ticker"].astype(str).str.upper() == clean].copy() if not events_df.empty else pd.DataFrame()
    if not ticker_events.empty:
        ticker_events["ex_ts"] = pd.to_datetime(ticker_events["ex_dividend_date"], errors="coerce")
        ex_dates = ticker_events[(ticker_events["ex_ts"] <= pd.Timestamp(chart_end))]["ex_ts"].dropna().tolist()
    else:
        ex_dates = []
    svg = mini_price_svg(prices, ex_dates)
    company = html.escape(str(getattr(row, "company_name", "") or clean))
    try:
        amount_label = f"{float(getattr(row, 'cash_amount', 0)):.4g}"
    except (TypeError, ValueError):
        amount_label = "-"
    currency = html.escape(str(getattr(row, "currency", "") or ""))
    pay_label_raw = getattr(row, "payment_day", "") or getattr(row, "pay_date_display", "") or "Pendiente"
    pay_label = html.escape(str(pay_label_raw))
    tae = getattr(row, "event_expected_tae_pct", None)
    tae_label = f"{float(tae):.1f}%" if pd.notna(tae) else "-"
    trend_tae = getattr(row, "event_trend_adjusted_tae_pct", None)
    trend_tae_label = f"{float(trend_tae):.1f}%" if pd.notna(trend_tae) else "-"
    return f"""
    <span class="ticker-hover-wrap">
      <span class="ticker-hover-symbol">{html.escape(clean)}</span>
      <span class="ticker-hover-card">
        <strong>{html.escape(clean)} - {company}</strong>
        <span class="ticker-hover-meta">Div: {html.escape(amount_label)} {currency} | pago {pay_label} | TAE {html.escape(tae_label)} | TAE aj. {html.escape(trend_tae_label)}</span>
        {svg}
        <span class="ticker-hover-meta">Lineas verticales: ex-dividend dates pasados en el rango.</span>
      </span>
    </span>
    """


def inject_ticker_hover_css() -> None:
    st.markdown(
        """
        <style>
        .ticker-hover-wrap { position: relative; display: inline-block; margin: 2px 0; }
        .ticker-hover-symbol {
            display: inline-flex; align-items: center; justify-content: center;
            min-width: 56px; padding: 3px 7px; border-radius: 5px;
            background: #eef2f7; color: #0f172a; font-weight: 700; font-size: 0.82rem;
            border: 1px solid #d8dee8; cursor: default;
        }
        .ticker-hover-card {
            visibility: hidden; opacity: 0; pointer-events: none;
            position: absolute; z-index: 50; left: 0; top: 26px; width: 330px;
            padding: 10px; border: 1px solid #d6dde8; border-radius: 7px;
            background: #ffffff; color: #0f172a; box-shadow: 0 12px 32px rgba(15, 23, 42, 0.18);
            transition: opacity 120ms ease;
        }
        .ticker-hover-wrap:hover .ticker-hover-card { visibility: visible; opacity: 1; }
        .ticker-hover-meta { display: block; color: #64748b; font-size: 0.72rem; margin: 4px 0 6px; }
        .ticker-hover-svg { width: 100%; height: auto; display: block; }
        .svg-bg { fill: #f8fafc; }
        .axis { stroke: #cbd5e1; stroke-width: 1; }
        .price-line { fill: none; stroke: #0f6fc6; stroke-width: 2; }
        .ex-line { stroke: #ef4444; stroke-width: 1.4; stroke-dasharray: 3 3; opacity: 0.85; }
        .axis-label { fill: #64748b; font-size: 9px; }
        .price-label { fill: #0f172a; font-size: 10px; font-weight: 700; }
        .ticker-hover-empty { padding: 28px 8px; background: #f8fafc; border-radius: 6px; color: #64748b; text-align: center; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_global_monthly_calendar(events_df: pd.DataFrame, universe_df: pd.DataFrame) -> str:
    st.markdown("**Calendario mensual global**")
    if events_df.empty:
        st.info("No hay eventos cargados para construir el calendario mensual.")
        return ""

    calendar = events_df.copy()
    calendar["ex_dividend_date"] = pd.to_datetime(calendar["ex_dividend_date"], errors="coerce")
    calendar["pay_date_dt"] = pd.to_datetime(calendar.get("pay_date", ""), errors="coerce")
    calendar = calendar.dropna(subset=["ex_dividend_date"])
    if calendar.empty:
        st.info("No hay fechas de ex-dividend validas en el rango.")
        return ""

    month_options = calendar["ex_dividend_date"].dt.to_period("M").astype(str).sort_values().unique().tolist()
    current_month = date.today().strftime("%Y-%m")
    default_index = month_options.index(current_month) if current_month in month_options else 0

    def options_for(column: str) -> list[str]:
        if column not in calendar.columns:
            return []
        values = calendar[column].dropna().astype(str)
        return sorted([value for value in values.unique().tolist() if value and value != "nan"])

    with st.container(border=True):
        f1, f2, f3, f4 = st.columns([1.2, 1.4, 1.4, 1.4])
        selected_month = f1.selectbox("Mes", month_options, index=default_index, key="global_calendar_month")
        selected_regions = f2.multiselect("Region", options_for("market_region"), key="global_calendar_regions")
        selected_types = f3.multiselect("Asset type", options_for("asset_type"), key="global_calendar_asset_types")
        selected_exchanges = f4.multiselect("Mercado", options_for("exchange"), key="global_calendar_exchanges")

        f5, f6, f7, f8 = st.columns(4)
        selected_sectors = f5.multiselect("Sector", options_for("sector_label"), key="global_calendar_sectors")
        selected_sources = f6.multiselect("Fuente", options_for("source"), key="global_calendar_sources")
        selected_statuses = f7.multiselect("Estado", options_for("status"), key="global_calendar_statuses")
        selected_currencies = f8.multiselect("Moneda", options_for("currency"), key="global_calendar_currencies")

        f9, f10, f11 = st.columns([1.2, 1.2, 2.4])
        selected_sic = f9.multiselect("SIC", options_for("sic_code"), key="global_calendar_sic")
        pay_date_filter = f10.selectbox("Fecha pago", ["Todos", "Con fecha", "Pendiente"], key="global_calendar_pay_date_filter")
        text_filter = f11.text_input(
            "Buscar",
            "",
            placeholder="Ticker, ISIN, nombre, sector, mercado...",
            key="global_calendar_text",
        )
        f12, f13, f14, f15 = st.columns(4)
        selected_capture_clusters = f12.multiselect("Cluster captura", options_for("capture_cluster"), key="global_calendar_capture_clusters")
        selected_speed_clusters = f13.multiselect("Rapidez", options_for("speed_cluster"), key="global_calendar_speed_clusters")
        selected_safety_clusters = f14.multiselect("Seguridad", options_for("safety_cluster"), key="global_calendar_safety_clusters")
        min_expected_tae = f15.number_input("TAE evento min %", min_value=0.0, value=0.0, step=1.0, key="global_calendar_min_tae")
        f16, f17, f18, f19 = st.columns(4)
        min_dividend_amount = f16.number_input("Dividendo min", min_value=0.0, value=0.0, step=0.01, key="global_calendar_min_dividend_amount")
        min_event_yield = f17.number_input("Yield real min %", min_value=0.0, value=0.0, step=0.05, key="global_calendar_min_event_yield")
        max_recovery_days_filter = f18.number_input("Dias recuperacion max", min_value=0, value=0, step=1, key="global_calendar_max_recovery_days")
        max_trend_risk_filter = f19.number_input("Riesgo tendencia max", min_value=0.0, max_value=100.0, value=100.0, step=5.0, key="global_calendar_max_trend_risk")

    monthly_view = calendar[calendar["ex_dividend_date"].dt.to_period("M").astype(str) == selected_month].copy()
    filter_map = {
        "market_region": selected_regions,
        "asset_type": selected_types,
        "exchange": selected_exchanges,
        "sector_label": selected_sectors,
        "source": selected_sources,
        "status": selected_statuses,
        "currency": selected_currencies,
        "sic_code": selected_sic,
        "capture_cluster": selected_capture_clusters,
        "speed_cluster": selected_speed_clusters,
        "safety_cluster": selected_safety_clusters,
    }
    for column, selected_values in filter_map.items():
        if selected_values and column in monthly_view.columns:
            monthly_view = monthly_view[monthly_view[column].astype(str).isin(selected_values)]

    if pay_date_filter == "Con fecha":
        monthly_view = monthly_view[monthly_view["pay_date_dt"].notna()]
    elif pay_date_filter == "Pendiente":
        monthly_view = monthly_view[monthly_view["pay_date_dt"].isna()]
    if min_dividend_amount > 0:
        monthly_view = monthly_view[pd.to_numeric(monthly_view["cash_amount"], errors="coerce").fillna(-1) >= float(min_dividend_amount)]
    if min_event_yield > 0 and "event_yield_real_pct" in monthly_view.columns:
        monthly_view = monthly_view[pd.to_numeric(monthly_view["event_yield_real_pct"], errors="coerce").fillna(-1) >= float(min_event_yield)]
    if max_recovery_days_filter > 0 and "median_recovery_days" in monthly_view.columns:
        monthly_view = monthly_view[pd.to_numeric(monthly_view["median_recovery_days"], errors="coerce").fillna(10_000) <= float(max_recovery_days_filter)]
    if min_expected_tae > 0 and "event_expected_tae_pct" in monthly_view.columns:
        monthly_view = monthly_view[pd.to_numeric(monthly_view["event_expected_tae_pct"], errors="coerce").fillna(-1) >= float(min_expected_tae)]
    if max_trend_risk_filter < 100 and "trend_risk_score" in monthly_view.columns:
        monthly_view = monthly_view[pd.to_numeric(monthly_view["trend_risk_score"], errors="coerce").fillna(100) <= float(max_trend_risk_filter)]

    if text_filter.strip():
        q = text_filter.strip().upper()
        matched_instruments = search_universe(universe_df, text_filter)
        matched_tickers = set(matched_instruments["ticker"].astype(str).str.upper().tolist()) if not matched_instruments.empty else set()
        searchable_cols = [
            "ticker",
            "isin",
            "company_name",
            "exchange",
            "asset_type",
            "market_region",
            "sector_label",
            "sic_code",
            "sic_industry",
            "capture_cluster",
            "speed_cluster",
            "safety_cluster",
            "event_yield_real_pct",
        ]
        text_mask = pd.Series(False, index=monthly_view.index)
        for col in searchable_cols:
            if col in monthly_view.columns:
                text_mask = text_mask | monthly_view[col].astype(str).str.upper().str.contains(q, regex=False)
        monthly_view = monthly_view[monthly_view["ticker"].astype(str).str.upper().isin(matched_tickers) | text_mask]

    if monthly_view.empty:
        if text_filter.strip():
            matches = search_universe(universe_df, text_filter)
            if not matches.empty:
                st.info("No hay eventos en este mes para esos filtros, pero estos instrumentos existen en el universo.")
                show = matches[["ticker", "isin", "name", "exchange", "asset_type", "market_region"]].head(50).reset_index(drop=True)
                clicked = selectable_ticker_table(show, "global_calendar_instrument_matches", use_container_width=True, hide_index=True)
                if clicked:
                    return clicked
                options = show["ticker"].astype(str).tolist()
                return st.selectbox("Abrir ficha desde calendario", options, key="global_calendar_open_match") if options else ""
        st.info("No hay eventos para esos filtros.")
        return ""

    monthly_view["ex_date"] = monthly_view["ex_dividend_date"].dt.date
    monthly_view["payment_day"] = monthly_view["pay_date_dt"].dt.date.astype(str).replace("NaT", "Pendiente")
    monthly_view["payment_day"] = monthly_view["payment_day"].replace("NaT", "Pendiente")

    selected_year, selected_month_number = [int(part) for part in selected_month.split("-")]
    month_label = date(selected_year, selected_month_number, 1).strftime("%B %Y")
    month_last_day = month_calendar.monthrange(selected_year, selected_month_number)[1]
    chart_end = date(selected_year, selected_month_number, month_last_day)
    total_amount = monthly_view["cash_amount"].sum()
    known_pay_dates = int(monthly_view["pay_date_dt"].notna().sum())
    avg_tae = pd.to_numeric(monthly_view.get("event_expected_tae_pct", pd.Series(dtype=float)), errors="coerce").mean()
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Eventos filtrados", f"{len(monthly_view):,}")
    m2.metric("Instrumentos", f"{monthly_view['ticker'].nunique():,}")
    m3.metric("Con payment date", f"{known_pay_dates:,}")
    m4.metric("Importe bruto", f"{total_amount:,.2f}")
    m5.metric("TAE esperado medio", f"{avg_tae:.1f}%" if pd.notna(avg_tae) else "-")

    st.markdown(f"**{month_label}**")
    st.caption("Cada dia se puede desplegar para ver ex-dates, fecha de pago e importe. Pulsa un ticker para abrir su ficha.")
    inject_ticker_hover_css()

    weekdays = ["Lu", "Ma", "Mi", "Ju", "Vi", "Sa", "Do"]
    header_cols = st.columns(7)
    for col, label in zip(header_cols, weekdays):
        col.markdown(f"**{label}**")

    selected_from_calendar = ""
    grouped_by_day = {day: day_rows for day, day_rows in monthly_view.groupby(monthly_view["ex_dividend_date"].dt.day)}
    for week in month_calendar.Calendar(firstweekday=0).monthdayscalendar(selected_year, selected_month_number):
        cols = st.columns(7)
        for col, day_number in zip(cols, week):
            if day_number == 0:
                col.container(border=True)
                continue
            day_rows = grouped_by_day.get(day_number, pd.DataFrame())
            with col.container(border=True):
                if day_rows.empty:
                    st.markdown(f"**{day_number}**")
                    st.caption("Sin eventos")
                    continue
                count = len(day_rows)
                amount = day_rows["cash_amount"].sum()
                with st.expander(f"{day_number} · {count} eventos · {amount:,.2f}", expanded=False):
                    for pos, row in enumerate(day_rows.sort_values(["ticker", "cash_amount"]).itertuples(), start=1):
                        pay_label = getattr(row, "payment_day", "Pendiente") or "Pendiente"
                        amount_label = f"{getattr(row, 'cash_amount', 0):.4g} {getattr(row, 'currency', '')}".strip()
                        tae = getattr(row, "event_expected_tae_pct", None)
                        tae_label = f" | TAE {float(tae):.1f}%" if pd.notna(tae) else ""
                        ticker = str(getattr(row, "ticker", ""))
                        st.caption(f"{amount_label} | pago {pay_label}{tae_label}")
                        st.markdown(ticker_hover_card_html(ticker, row, events_df, chart_end), unsafe_allow_html=True)
                        if st.button("Abrir", key=f"calendar_day_{selected_month}_{day_number}_{pos}_{ticker}_{getattr(row, 'source_event_id', '')}"):
                            selected_from_calendar = ticker

    show_cols = [
        "ex_date",
        "payment_day",
        "ticker",
        "isin",
        "company_name",
        "cash_amount",
        "currency",
        "capture_avg_dividend_yield_pct",
        "event_yield_real_pct",
        "recovery_rate_pct",
        "median_recovery_days",
        "event_expected_tae_pct",
        "event_trend_adjusted_tae_pct",
        "expected_tae_pct",
        "capture_score",
        "trend_adjusted_capture_score",
        "trend_risk_score",
        "trend_cluster",
        "capture_cluster",
        "speed_cluster",
        "safety_cluster",
        "asset_type",
        "market_region",
        "sector_label",
        "sic_code",
        "sic_industry",
        "source",
    ]
    display = monthly_view.sort_values(["ex_dividend_date", "ticker"])[show_cols].rename(
            columns={
                "ex_date": "ex-date",
                "payment_day": "payment day",
                "company_name": "nombre",
                "cash_amount": "cantidad",
                "capture_avg_dividend_yield_pct": "yield hist %",
                "event_yield_real_pct": "yield real evento %",
                "recovery_rate_pct": "recuperacion %",
                "median_recovery_days": "dias rec mediana",
                "event_expected_tae_pct": "TAE evento %",
                "event_trend_adjusted_tae_pct": "TAE ajust tendencia %",
                "expected_tae_pct": "TAE hist ticker %",
                "capture_score": "score captura",
                "trend_adjusted_capture_score": "score ajust tendencia",
                "trend_risk_score": "riesgo tendencia",
                "trend_cluster": "cluster tendencia",
                "capture_cluster": "cluster captura",
                "speed_cluster": "rapidez",
                "safety_cluster": "seguridad",
                "asset_type": "asset type",
                "market_region": "region",
                "sector_label": "sector",
                "sic_code": "SIC",
                "sic_industry": "industria SEC",
            }
        )
    st.markdown("**Detalle filtrado**")
    clicked = selectable_ticker_table(
        display,
        "global_calendar_events",
        use_container_width=True,
        hide_index=True,
    )
    return selected_from_calendar or clicked


def render_instrument_detail(ticker: str, universe_df: pd.DataFrame, events_df: pd.DataFrame) -> None:
    row = universe_df[universe_df["ticker"] == ticker]
    info = row.iloc[0].to_dict() if not row.empty else {"ticker": ticker}
    profile = load_sec_profile(ticker)
    name = profile.get("name") or info.get("name") or ticker
    sector = profile.get("sector_name") or sector_display(info.get("sector"), info.get("asset_type"))
    description = profile.get("description") or ""
    asset_type = info.get("asset_type") or profile.get("entity_type") or ""
    sic_code = profile.get("sector") or info.get("sic_code") or ""
    sic_industry = profile.get("sic_description") or info.get("sic_industry") or ""
    if not sic_code and str(asset_type).lower().find("etf") >= 0:
        sic_code = "No aplica"
        sic_industry = "Fondo/ETF sin clasificacion SIC SEC"

    st.markdown(f"**{ticker} - {name}**")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Tipo", asset_type or "-")
    d2.metric("Mercado", info.get("exchange") or profile.get("exchange") or "-")
    d3.metric("Region", info.get("market_region") or info.get("region") or "-")
    d4.metric("Sector", sector or "-")

    meta = {
        "Ticker": ticker,
        "ISIN": info.get("isin") or "",
        "Nombre": name,
        "Exchange": info.get("exchange") or profile.get("exchange") or "",
        "Sector": sector,
        "SIC": sic_code,
        "Industria SEC": sic_industry,
        "Estado/Pais": profile.get("state") or info.get("state") or "",
        "Entidad": profile.get("entity_type") or "",
        "Anios SEC": f"{profile.get('min_year', '')}-{profile.get('max_year', '')}".strip("-"),
        "Fuente universo": info.get("provider") or "",
        "Fuente perfil": info.get("profile_provider") or "",
    }
    st.dataframe(
        pd.DataFrame([meta]).replace("", "-"),
        use_container_width=True,
        hide_index=True,
    )
    if description:
        st.caption(description)

    signal = load_capture_ticker_signal()
    if not signal.empty:
        capture_row = signal[signal["ticker"] == ticker]
        if not capture_row.empty:
            cap = capture_row.iloc[0]
            st.markdown("**Senal historica de captura**")
            s1, s2, s3, s4, s5, s6 = st.columns(6)
            s1.metric("TAE esperado", f"{cap.get('expected_tae_pct'):.1f}%" if pd.notna(cap.get("expected_tae_pct")) else "-")
            s2.metric("Recuperacion", f"{cap.get('recovery_rate_pct'):.1f}%" if pd.notna(cap.get("recovery_rate_pct")) else "-")
            s3.metric("Dias mediana", f"{cap.get('median_recovery_days'):.0f}" if pd.notna(cap.get("median_recovery_days")) else "-")
            s4.metric("Yield medio", f"{cap.get('avg_dividend_yield_pct'):.2f}%" if pd.notna(cap.get("avg_dividend_yield_pct")) else "-")
            s5.metric("Score", f"{cap.get('capture_score'):.1f}" if pd.notna(cap.get("capture_score")) else "-")
            s6.metric("Score tendencia", f"{cap.get('trend_adjusted_capture_score'):.1f}" if pd.notna(cap.get("trend_adjusted_capture_score")) else "-")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Cluster captura": cap.get("capture_cluster", ""),
                            "Rapidez": cap.get("speed_cluster", ""),
                            "Seguridad": cap.get("safety_cluster", ""),
                            "Estabilidad": cap.get("stability_cluster", ""),
                            "Cluster tendencia": cap.get("trend_cluster", ""),
                            "Riesgo tendencia": cap.get("trend_risk_score", ""),
                            "Multiplicador tendencia": cap.get("trend_score_multiplier", ""),
                            "Retorno 3m %": cap.get("trend_return_3m_pct", ""),
                            "Retorno 6m %": cap.get("trend_return_6m_pct", ""),
                            "Vs SMA200 %": cap.get("trend_vs_sma200_pct", ""),
                            "Drawdown 6m %": cap.get("trend_drawdown_6m_pct", ""),
                            "Eventos analizados": cap.get("events", ""),
                            "Eventos recuperados": cap.get("recovered_events", ""),
                        }
                    ]
                ).replace("", "-"),
                use_container_width=True,
                hide_index=True,
            )

    render_dividend_analytics(ticker, events_df)


def file_mtime(path: Path) -> str:
    if not path.exists():
        return "-"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def git_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=APP_DIR,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        return completed.stdout.strip() or "-"
    except Exception:
        return "-"


def csv_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        return max(0, sum(1 for _ in fh) - 1)


@st.cache_data(ttl=120)
def data_status() -> dict:
    status = {
        "db_path": str(pipeline.DIVIDENDS_DB),
        "db_updated": file_mtime(pipeline.DIVIDENDS_DB),
        "code_commit": git_commit(),
        "us_universe_rows": csv_count(US_UNIVERSE_CSV),
        "europe_universe_rows": csv_count(EUROPE_UNIVERSE_CSV),
        "total_events": 0,
        "min_ex_date": "-",
        "max_ex_date": "-",
        "runs": [],
        "sources": [],
        "asset_types": [],
    }
    if not pipeline.DIVIDENDS_DB.exists():
        return status
    conn = sqlite3.connect(pipeline.DIVIDENDS_DB)
    try:
        row = conn.execute(
            "SELECT COUNT(*), MIN(ex_dividend_date), MAX(ex_dividend_date) FROM dividend_events"
        ).fetchone()
        status["total_events"] = int(row[0] or 0)
        status["min_ex_date"] = row[1] or "-"
        status["max_ex_date"] = row[2] or "-"
        status["sources"] = pd.read_sql_query(
            """
            SELECT source, COUNT(*) AS events, COUNT(DISTINCT ticker) AS tickers,
                   MIN(ex_dividend_date) AS first_ex_date,
                   MAX(ex_dividend_date) AS last_ex_date
            FROM dividend_events
            GROUP BY source
            ORDER BY events DESC
            """,
            conn,
        ).to_dict("records")
        status["asset_types"] = pd.read_sql_query(
            """
            SELECT COALESCE(asset_type, '') AS asset_type,
                   COUNT(*) AS events,
                   COUNT(DISTINCT ticker) AS tickers
            FROM dividend_events
            GROUP BY COALESCE(asset_type, '')
            ORDER BY events DESC
            """,
            conn,
        ).to_dict("records")
        status["runs"] = pd.read_sql_query(
            """
            SELECT started_at, finished_at, source, start_date, end_date,
                   tickers_requested, events_upserted, errors
            FROM dividend_runs
            ORDER BY started_at DESC
            LIMIT 12
            """,
            conn,
        ).to_dict("records")
    finally:
        conn.close()
    return status


@st.cache_data(ttl=120)
def events_between(start: str, end: str) -> pd.DataFrame:
    return pd.DataFrame(pipeline.load_events(start, end))


def csv_download(df: pd.DataFrame) -> str:
    out = StringIO()
    writer = csv.writer(out)
    writer.writerow(df.columns)
    for row in df.itertuples(index=False):
        writer.writerow(row)
    return out.getvalue()


@st.cache_data(ttl=1800)
def run_capture_lab(
    start: str,
    end: str,
    max_recovery_days: int,
    min_dividend_yield_pct: float,
    limit_tickers: int,
    max_events: int,
    use_high_for_recovery: bool,
    workers: int,
) -> pd.DataFrame:
    settings = capture.CaptureSettings(
        start=start,
        end=end,
        max_recovery_days=max_recovery_days,
        min_dividend_yield_pct=min_dividend_yield_pct,
        limit_tickers=limit_tickers,
        use_high_for_recovery=use_high_for_recovery,
    )
    return capture.run_capture_backtest(settings, max_events=max_events, workers=workers)


def build_portfolio_equity_curve(trades: pd.DataFrame, initial_capital: float, rank_by: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["step", "date", "rank_by", "capital", "return_pct"])
    curve = trades.copy()
    curve["date"] = pd.to_datetime(curve["exit_date"], errors="coerce")
    curve = curve.sort_values(["date", "ticker"]).reset_index(drop=True)
    curve["step"] = range(1, len(curve) + 1)
    curve["rank_by"] = rank_by
    curve["capital"] = pd.to_numeric(curve["capital_after"], errors="coerce")
    curve["return_pct"] = (curve["capital"] / float(initial_capital) - 1) * 100
    start = pd.DataFrame(
        [
            {
                "step": 0,
                "date": pd.to_datetime(curve["entry_date"], errors="coerce").min(),
                "rank_by": rank_by,
                "capital": float(initial_capital),
                "return_pct": 0.0,
            }
        ]
    )
    return pd.concat([start, curve[["step", "date", "rank_by", "capital", "return_pct"]]], ignore_index=True)


def render_portfolio_strategy_charts(
    portfolio_by_rank: dict[str, pd.DataFrame],
    comparison: pd.DataFrame,
    initial_capital: float,
    universe: pd.DataFrame,
    events: pd.DataFrame,
) -> None:
    if comparison.empty:
        return

    st.markdown("**Comparativa visual de estrategias**")
    chart_comparison = comparison.copy()
    chart_comparison["capital_final"] = pd.to_numeric(chart_comparison["capital_final"], errors="coerce")
    chart_comparison["return_pct"] = pd.to_numeric(chart_comparison["return_pct"], errors="coerce")
    chart_comparison["avg_trade_return_pct"] = pd.to_numeric(chart_comparison["avg_trade_return_pct"], errors="coerce")
    chart_comparison["median_holding_days"] = pd.to_numeric(chart_comparison["median_holding_days"], errors="coerce")

    c1, c2 = st.columns(2)
    c1.bar_chart(chart_comparison.set_index("rank_by")[["capital_final"]])
    c2.bar_chart(chart_comparison.set_index("rank_by")[["return_pct"]])

    c3, c4 = st.columns(2)
    c3.bar_chart(chart_comparison.set_index("rank_by")[["avg_trade_return_pct"]])
    c4.bar_chart(chart_comparison.set_index("rank_by")[["median_holding_days"]])

    curves = []
    for rank_by, trades in portfolio_by_rank.items():
        curves.append(build_portfolio_equity_curve(trades, initial_capital, rank_by))
    curve_df = pd.concat([c for c in curves if not c.empty], ignore_index=True) if curves else pd.DataFrame()
    if not curve_df.empty:
        capital_curve = curve_df.pivot_table(index="step", columns="rank_by", values="capital", aggfunc="last").sort_index()
        st.markdown("**Evolucion del capital por estrategia**")
        st.line_chart(capital_curve)

    best_rank = str(comparison.iloc[0]["rank_by"])
    selected_rank = st.selectbox(
        "Estrategia a inspeccionar",
        chart_comparison["rank_by"].tolist(),
        index=chart_comparison["rank_by"].tolist().index(best_rank),
        key="portfolio_strategy_rank_selector",
    )
    selected_trades = portfolio_by_rank.get(selected_rank, pd.DataFrame())
    if selected_trades.empty:
        st.info("Esta estrategia no genero operaciones.")
        return

    selected_summary = capture.portfolio_backtest_summary(selected_trades, initial_capital)
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Operaciones", f"{selected_summary['trades']:,}")
    s2.metric("Capital final", fmt_money(selected_summary["capital_final"], "EUR"))
    s3.metric("Retorno total", f"{selected_summary['return_pct']:.2f}%")
    s4.metric("Mediana dias", f"{selected_summary['median_holding_days']:.0f}" if pd.notna(selected_summary["median_holding_days"]) else "-")

    selected_curve = build_portfolio_equity_curve(selected_trades, initial_capital, selected_rank)
    if not selected_curve.empty:
        st.markdown("**Camino de capital de la estrategia seleccionada**")
        st.line_chart(selected_curve.set_index("step")[["capital", "return_pct"]])

    trade_chart = selected_trades.copy().sort_values(["entry_date", "ticker"]).reset_index(drop=True)
    trade_chart["operacion"] = trade_chart.index + 1
    trade_chart["pnl"] = pd.to_numeric(trade_chart["pnl"], errors="coerce")
    trade_chart["trade_return_pct"] = pd.to_numeric(trade_chart["trade_return_pct"], errors="coerce")
    trade_chart["holding_days"] = pd.to_numeric(trade_chart["holding_days"], errors="coerce")
    st.markdown("**Acciones ejecutadas por la estrategia seleccionada**")
    t1, t2 = st.columns(2)
    t1.bar_chart(trade_chart.set_index("operacion")[["pnl"]])
    t2.bar_chart(trade_chart.set_index("operacion")[["holding_days"]])

    detail_cols = [
        "entry_date",
        "exit_date",
        "ticker",
        "company_name",
        "entry_cash",
        "exit_cash",
        "pnl",
        "trade_return_pct",
        "holding_days",
        "event_yield_real_pct",
        "event_expected_tae_pct",
        "event_trend_adjusted_tae_pct",
        "capture_score",
        "trend_adjusted_capture_score",
        "trend_risk_score",
        "trend_cluster",
        "capture_cluster",
    ]
    visible_cols = [c for c in detail_cols if c in selected_trades.columns]
    clicked = selectable_ticker_table(
        selected_trades[visible_cols].sort_values(["entry_date", "ticker"]),
        "strategy_portfolio_selected_trades_table",
        use_container_width=True,
        hide_index=True,
    )
    if clicked:
        render_instrument_detail(clicked, universe, events)

    st.markdown("**Detalle por cada opcion estudiada**")
    tabs = st.tabs(chart_comparison["rank_by"].tolist())
    for tab, rank_by in zip(tabs, chart_comparison["rank_by"].tolist()):
        trades = portfolio_by_rank.get(str(rank_by), pd.DataFrame())
        with tab:
            if trades.empty:
                st.info("Sin operaciones para esta opcion.")
                continue
            summary = capture.portfolio_backtest_summary(trades, initial_capital)
            a1, a2, a3, a4 = st.columns(4)
            a1.metric("Capital final", fmt_money(summary["capital_final"], "EUR"))
            a2.metric("Retorno", f"{summary['return_pct']:.2f}%")
            a3.metric("Operaciones", f"{summary['trades']:,}")
            a4.metric("Mediana dias", f"{summary['median_holding_days']:.0f}" if pd.notna(summary["median_holding_days"]) else "-")
            curve = build_portfolio_equity_curve(trades, initial_capital, str(rank_by))
            if not curve.empty:
                st.line_chart(curve.set_index("step")[["capital"]])
            sample = trades.copy().sort_values(["entry_date", "ticker"]).reset_index(drop=True)
            sample["operacion"] = sample.index + 1
            st.bar_chart(sample.set_index("operacion")[["trade_return_pct"]])
            clicked = selectable_ticker_table(
                sample[[c for c in detail_cols if c in sample.columns]],
                f"strategy_portfolio_trades_table_{rank_by}",
                use_container_width=True,
                hide_index=True,
            )
            if clicked:
                render_instrument_detail(clicked, universe, events)


def strategy_criterion_label(criterion: str) -> str:
    labels = {
        "trend_adjusted_capture_score": "Score ajustado por tendencia",
        "capture_score": "Score captura",
        "event_trend_adjusted_tae_pct": "TAE evento ajustada por tendencia",
        "event_expected_tae_pct": "TAE esperada del evento",
        "event_yield_real_pct": "Yield real del reparto",
        "recovery_rate_pct": "Seguridad de recuperacion",
        "median_recovery_days": "Rapidez de recuperacion",
    }
    return labels.get(criterion, criterion)


def build_capture_recommendations(
    events_df: pd.DataFrame,
    criterion: str,
    max_positions: int = 2,
    horizon_days: int = 45,
    min_event_yield_pct: float = 0.0,
    min_recovery_rate_pct: float = 0.0,
    max_recovery_days: int = 0,
    max_trend_risk: float = 100.0,
) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()
    signals = events_df.copy()
    signals["ex_dividend_dt"] = pd.to_datetime(signals["ex_dividend_date"], errors="coerce")
    signals = signals.dropna(subset=["ex_dividend_dt"])
    today_ts = pd.Timestamp(date.today())
    max_ts = today_ts + pd.Timedelta(days=int(horizon_days))
    signals = signals[(signals["ex_dividend_dt"] >= today_ts) & (signals["ex_dividend_dt"] <= max_ts)].copy()
    if signals.empty:
        return signals
    signals["entry_dt"] = signals["ex_dividend_dt"] - pd.Timedelta(days=1)
    signals["entry_date"] = signals["entry_dt"].dt.date
    signals["ex_date"] = signals["ex_dividend_dt"].dt.date
    signals["days_to_entry"] = (signals["entry_dt"].dt.normalize() - today_ts.normalize()).dt.days
    signals["accion"] = signals["days_to_entry"].map(
        lambda days: "Comprar hoy" if days == 0 else ("Vigilar entrada pasada" if days < 0 else f"Esperar {days} dias")
    )
    for col in [
        "event_yield_real_pct",
        "event_expected_tae_pct",
        "event_trend_adjusted_tae_pct",
        "capture_score",
        "trend_adjusted_capture_score",
        "trend_risk_score",
        "trend_score_multiplier",
        "trend_return_3m_pct",
        "trend_return_6m_pct",
        "trend_vs_sma200_pct",
        "trend_drawdown_6m_pct",
        "recovery_rate_pct",
        "median_recovery_days",
        "cash_amount",
    ]:
        if col in signals.columns:
            signals[col] = pd.to_numeric(signals[col], errors="coerce")
    if min_event_yield_pct > 0 and "event_yield_real_pct" in signals.columns:
        signals = signals[signals["event_yield_real_pct"].fillna(-1) >= float(min_event_yield_pct)]
    if min_recovery_rate_pct > 0 and "recovery_rate_pct" in signals.columns:
        signals = signals[signals["recovery_rate_pct"].fillna(-1) >= float(min_recovery_rate_pct)]
    if max_recovery_days > 0 and "median_recovery_days" in signals.columns:
        signals = signals[signals["median_recovery_days"].fillna(10_000) <= float(max_recovery_days)]
    if max_trend_risk < 100 and "trend_risk_score" in signals.columns:
        signals = signals[signals["trend_risk_score"].fillna(100) <= float(max_trend_risk)]
    if signals.empty:
        return signals
    sort_ascending = criterion == "median_recovery_days"
    if criterion not in signals.columns:
        criterion = "capture_score"
    signals["_criterion_value"] = pd.to_numeric(signals[criterion], errors="coerce")
    signals = signals.dropna(subset=["_criterion_value"])
    if signals.empty:
        return signals
    signals = signals.sort_values(
        ["entry_dt", "_criterion_value", "event_yield_real_pct"],
        ascending=[True, sort_ascending, False],
    )
    signals["rank_dia"] = signals.groupby("entry_date")["_criterion_value"].rank(method="first", ascending=sort_ascending)
    signals = signals[signals["rank_dia"] <= int(max_positions)].copy()
    signals["criterio"] = criterion
    signals["criterio_valor"] = signals["_criterion_value"]

    def fmt_signal_number(value, suffix: str = "", decimals: int = 1) -> str:
        try:
            if pd.isna(value):
                return "-"
            return f"{float(value):.{decimals}f}{suffix}"
        except (TypeError, ValueError):
            return "-"

    signals["justificacion"] = signals.apply(
        lambda row: (
            f"{strategy_criterion_label(criterion)} {fmt_signal_number(row.get('criterio_valor'), decimals=2)}; "
            f"yield real {fmt_signal_number(row.get('event_yield_real_pct'), '%', 2)} del reparto; "
            f"recuperacion historica {fmt_signal_number(row.get('recovery_rate_pct'), '%', 1)} en mediana "
            f"{fmt_signal_number(row.get('median_recovery_days'), '', 0)} dias; "
            f"TAE evento {fmt_signal_number(row.get('event_expected_tae_pct'), '%', 1)}; "
            f"TAE ajustada tendencia {fmt_signal_number(row.get('event_trend_adjusted_tae_pct'), '%', 1)}; "
            f"riesgo tendencia {fmt_signal_number(row.get('trend_risk_score'), '/100', 0)} "
            f"({row.get('trend_cluster') or '-'}), multiplicador {fmt_signal_number(row.get('trend_score_multiplier'), 'x', 2)}."
        )
        if pd.notna(row.get("criterio_valor"))
        else "",
        axis=1,
    )
    return signals.sort_values(["entry_dt", "rank_dia", "_criterion_value"], ascending=[True, True, sort_ascending])


def render_capture_recommendation_calendar(events_df: pd.DataFrame, universe_df: pd.DataFrame) -> None:
    st.markdown("**Senales operativas: que comprar segun el criterio**")
    st.caption("Selecciona un criterio y la app genera un calendario de entradas estimadas. La entrada se calcula como el dia anterior al ex-date.")
    if events_df.empty:
        st.info("No hay eventos cargados para generar senales.")
        return

    c1, c2, c3, c4 = st.columns([1.6, 1, 1, 1])
    criterion = c1.selectbox(
        "Criterio",
        ["trend_adjusted_capture_score", "event_trend_adjusted_tae_pct", "capture_score", "event_expected_tae_pct", "event_yield_real_pct", "recovery_rate_pct", "median_recovery_days"],
        format_func=strategy_criterion_label,
        key="capture_signal_criterion",
    )
    max_positions = c2.number_input("Max posiciones", min_value=1, max_value=10, value=2, step=1, key="capture_signal_max_positions")
    horizon_days = c3.number_input("Horizonte dias", min_value=7, max_value=365, value=45, step=7, key="capture_signal_horizon")
    signal_month = c4.selectbox(
        "Vista",
        ["Proximas senales", "Calendario mensual"],
        key="capture_signal_view",
    )
    f1, f2, f3, f4 = st.columns(4)
    min_event_yield = f1.number_input("Yield real min %", min_value=0.0, value=0.0, step=0.05, key="capture_signal_min_yield")
    min_recovery = f2.number_input("Recuperacion min %", min_value=0.0, max_value=100.0, value=0.0, step=5.0, key="capture_signal_min_recovery")
    max_days = f3.number_input("Dias recuperacion max", min_value=0, value=0, step=1, key="capture_signal_max_days")
    max_trend_risk = f4.number_input("Riesgo tendencia max", min_value=0.0, max_value=100.0, value=70.0, step=5.0, key="capture_signal_max_trend_risk")

    recommendations = build_capture_recommendations(
        events_df,
        criterion=criterion,
        max_positions=int(max_positions),
        horizon_days=int(horizon_days),
        min_event_yield_pct=float(min_event_yield),
        min_recovery_rate_pct=float(min_recovery),
        max_recovery_days=int(max_days),
        max_trend_risk=float(max_trend_risk),
    )
    if recommendations.empty:
        st.info("No hay senales con esos filtros.")
        return

    top_today = recommendations[recommendations["accion"] == "Comprar hoy"]
    n1, n2, n3, n4 = st.columns(4)
    n1.metric("Senales", f"{len(recommendations):,}")
    n2.metric("Comprar hoy", f"{len(top_today):,}")
    n3.metric("Tickers", f"{recommendations['ticker'].nunique():,}")
    n4.metric("Siguiente valor criterio", f"{recommendations['criterio_valor'].iloc[0]:.2f}")

    explanation = {
        "trend_adjusted_capture_score": "Score historico de captura penalizado por tendencia bajista reciente, distancia a media 200 y drawdown.",
        "capture_score": "Mejor equilibrio entre yield del reparto, rapidez, seguridad y consistencia historica.",
        "event_expected_tae_pct": "Prioriza el retorno anualizado esperado de este reparto concreto, usando yield real del evento y recuperacion historica.",
        "event_yield_real_pct": "Prioriza cobrar el dividendo mas grande respecto al precio de referencia, aunque pueda tardar mas en recuperar.",
        "recovery_rate_pct": "Prioriza activos que historicamente recuperaron mas veces el precio previo.",
        "median_recovery_days": "Prioriza activos que historicamente recuperaron antes el precio previo.",
    }
    st.info(explanation.get(criterion, "Criterio seleccionado."))

    show_cols = [
        "accion",
        "entry_date",
        "ex_date",
        "pay_date_display",
        "rank_dia",
        "ticker",
        "company_name",
        "cash_amount",
        "currency",
        "criterio_valor",
        "event_yield_real_pct",
        "event_expected_tae_pct",
        "event_trend_adjusted_tae_pct",
        "capture_score",
        "trend_adjusted_capture_score",
        "trend_risk_score",
        "trend_score_multiplier",
        "trend_return_3m_pct",
        "trend_return_6m_pct",
        "trend_vs_sma200_pct",
        "trend_drawdown_6m_pct",
        "recovery_rate_pct",
        "median_recovery_days",
        "capture_cluster",
        "trend_cluster",
        "justificacion",
    ]
    visible_cols = [col for col in show_cols if col in recommendations.columns]

    if signal_month == "Calendario mensual":
        month_options = recommendations["entry_dt"].dt.to_period("M").astype(str).sort_values().unique().tolist()
        selected_month = st.selectbox("Mes de entrada", month_options, key="capture_signal_month")
        month_rows = recommendations[recommendations["entry_dt"].dt.to_period("M").astype(str) == selected_month]
        year, month_num = [int(part) for part in selected_month.split("-")]
        chart_end = date(year, month_num, month_calendar.monthrange(year, month_num)[1])
        st.markdown(f"**Entradas estimadas {selected_month}**")
        inject_ticker_hover_css()
        weekdays = ["Lu", "Ma", "Mi", "Ju", "Vi", "Sa", "Do"]
        for col, label in zip(st.columns(7), weekdays):
            col.markdown(f"**{label}**")
        grouped = {day: rows for day, rows in month_rows.groupby(month_rows["entry_dt"].dt.day)}
        selected_ticker = ""
        for week in month_calendar.Calendar(firstweekday=0).monthdayscalendar(year, month_num):
            cols = st.columns(7)
            for col, day_number in zip(cols, week):
                with col.container(border=True):
                    if day_number == 0:
                        continue
                    rows = grouped.get(day_number, pd.DataFrame())
                    st.markdown(f"**{day_number}**")
                    if rows.empty:
                        st.caption("Sin senal")
                        continue
                    for pos, row in enumerate(rows.sort_values("rank_dia").itertuples(), start=1):
                        ticker = str(getattr(row, "ticker", ""))
                        st.caption(f"#{int(getattr(row, 'rank_dia', pos))} ex {getattr(row, 'ex_date', '')} | {getattr(row, 'cash_amount', 0):.4g}")
                        st.markdown(ticker_hover_card_html(ticker, row, events_df, chart_end), unsafe_allow_html=True)
                        if st.button("Abrir", key=f"capture_signal_{selected_month}_{day_number}_{pos}_{ticker}"):
                            selected_ticker = ticker
        if selected_ticker:
            render_instrument_detail(selected_ticker, universe_df, events_df)

    clicked = selectable_ticker_table(
        recommendations[visible_cols],
        "capture_recommendations_table",
        use_container_width=True,
        hide_index=True,
    )
    if clicked:
        render_instrument_detail(clicked, universe_df, events_df)


def render_capture_strategy_tab() -> None:
    st.subheader("Estrategia compra pre ex-date")
    st.caption(
        "Backtest experimental: compra al cierre previo al ex-date, cobra dividendo "
        "y vende cuando el cierre recupera el precio de entrada."
    )
    render_capture_recommendation_calendar(events, universe)
    st.divider()
    c1, c2, c3, c4 = st.columns(4)
    strategy_start = c1.date_input("Desde backtest", value=date(2024, 1, 1), key="capture_start")
    strategy_end = c2.date_input("Hasta backtest", value=date.today(), key="capture_end")
    max_recovery_days = c3.number_input("Max dias recuperacion", min_value=5, max_value=365, value=90, step=5)
    capital = c4.number_input("Capital inicial", min_value=100.0, value=1000.0, step=100.0)

    f1, f2, f3, f4 = st.columns(4)
    min_yield = f1.number_input("Yield minimo evento %", min_value=0.0, value=0.0, step=0.1)
    limit_tickers = f2.number_input("Limite tickers", min_value=0, value=40, step=10)
    max_events = f3.number_input("Limite eventos", min_value=0, value=250, step=50)
    use_high = f4.checkbox("Recuperacion intradia high", value=False)
    workers = st.slider("Workers", min_value=1, max_value=12, value=4, step=1)

    if st.button("Ejecutar backtest", type="primary"):
        st.session_state["capture_run_requested"] = True
        st.cache_data.clear()

    if not st.session_state.get("capture_run_requested"):
        st.info("Configura el experimento y pulsa Ejecutar backtest para descargar precios y calcular recuperaciones.")
        return

    with st.spinner("Calculando recuperaciones y cacheando precios..."):
        results = run_capture_lab(
            strategy_start.isoformat(),
            strategy_end.isoformat(),
            int(max_recovery_days),
            float(min_yield),
            int(limit_tickers),
            int(max_events),
            bool(use_high),
            int(workers),
        )

    if results.empty:
        st.info("No hay resultados para esos parametros. Prueba ampliar fechas o bajar el yield minimo.")
        return

    recovered = results[results["recovered"]].copy()
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Eventos analizados", f"{len(results):,}")
    r2.metric("Recuperados", f"{len(recovered):,}")
    r3.metric("Tasa recuperacion", f"{results['recovered'].mean() * 100:.1f}%")
    r4.metric("Mediana dias", f"{recovered['holding_days'].median():.0f}" if not recovered.empty else "-")

    summary = capture.summarize_by_ticker(results)
    if not summary.empty:
        st.markdown("**Ranking por ticker**")
        clicked = selectable_ticker_table(
            summary[
                [
                    "ticker",
                    "company_name",
                    "asset_type",
                    "exchange",
                    "events",
                    "recovery_rate_pct",
                    "median_recovery_days",
                    "avg_dividend_yield_pct",
                    "expected_tae_pct",
                    "capture_score",
                    "trend_adjusted_capture_score",
                    "trend_risk_score",
                    "trend_cluster",
                    "capture_cluster",
                    "speed_cluster",
                    "safety_cluster",
                    "avg_annualized_return_pct",
                ]
            ],
            "strategy_summary_table",
            use_container_width=True,
            hide_index=True,
        )
        if clicked:
            render_instrument_detail(clicked, universe, events)

        segment_signal = capture.summarize_by_segment(summary)
        if not segment_signal.empty:
            st.markdown("**Segmentos y clusters**")
            st.dataframe(
                segment_signal[
                    [
                        "dimension",
                        "segment",
                        "tickers",
                        "events",
                        "avg_recovery_rate_pct",
                        "median_recovery_days",
                        "avg_expected_tae_pct",
                        "avg_capture_score",
                        "top_ticker",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
        if st.button("Guardar senal en calendario", key="save_capture_signal"):
            ticker_signal, saved_segments = capture.save_capture_signal(results)
            st.success(f"Senal guardada: {len(ticker_signal):,} tickers y {len(saved_segments):,} segmentos.")
            st.cache_data.clear()

    st.markdown("**Eventos historicos**")
    event_cols = [
        "ex_dividend_date",
        "ticker",
        "company_name",
        "cash_amount",
        "currency",
        "entry_date",
        "entry_price",
        "ex_close",
        "ex_drop_pct",
        "recovered",
        "recovery_date",
        "holding_days",
        "dividend_yield_pct",
        "total_return_pct",
        "annualized_return_pct",
    ]
    clicked = selectable_ticker_table(
        results[event_cols].sort_values(["recovered", "annualized_return_pct"], ascending=[False, False]),
        "strategy_events_table",
        use_container_width=True,
        hide_index=True,
    )
    if clicked:
        render_instrument_detail(clicked, universe, events)

    trades = capture.simulate_reinvestment(results, capital=float(capital))
    st.markdown("**Simulacion secuencial reinvirtiendo**")
    if trades.empty:
        st.info("No hay operaciones recuperadas para simular reinversion.")
    else:
        final_capital = trades.iloc[-1]["capital_after"]
        s1, s2, s3 = st.columns(3)
        s1.metric("Operaciones", f"{len(trades):,}")
        s2.metric("Capital final", fmt_money(final_capital, "EUR"))
        s3.metric("Retorno total", f"{(final_capital / float(capital) - 1) * 100:.2f}%")
        clicked = selectable_ticker_table(trades, "strategy_trades_table", use_container_width=True, hide_index=True)
        if clicked:
            render_instrument_detail(clicked, universe, events)

    st.markdown("**Simulacion cartera max 2 posiciones**")
    signal_for_run = capture.summarize_by_ticker(results)
    rank_options = ["trend_adjusted_capture_score", "event_trend_adjusted_tae_pct", "event_expected_tae_pct", "event_yield_real_pct", "capture_score", "recovery_rate_pct"]
    portfolio_summaries = []
    portfolio_by_rank = {}
    for rank_by in rank_options:
        portfolio_run = capture.simulate_portfolio_capture(
            results,
            capital=float(capital),
            max_positions=2,
            rank_by=rank_by,
            ticker_signal=signal_for_run,
        )
        portfolio_by_rank[rank_by] = portfolio_run
        summary = capture.portfolio_backtest_summary(portfolio_run, float(capital))
        summary["rank_by"] = rank_by
        portfolio_summaries.append(summary)
    comparison = pd.DataFrame(portfolio_summaries).sort_values("capital_final", ascending=False)
    st.dataframe(comparison, use_container_width=True, hide_index=True)
    render_portfolio_strategy_charts(portfolio_by_rank, comparison, float(capital), universe, events)


st.title("Dividend Calendar USA")
st.caption("Calendario personal de ex-dividend dates e importes para acciones y ETFs.")

with st.sidebar:
    st.header("Rango")
    today = date.today()
    default_start = date(2025, 1, 1)
    default_end = date(2026, 12, 31)
    start_date = st.date_input("Desde", value=default_start)
    end_date = st.date_input("Hasta", value=default_end)
    if end_date < start_date:
        st.error("La fecha final debe ser posterior a la inicial.")
    st.divider()
    st.header("Actualizar")
    st.caption("Comando unico recomendado para refrescar datos diarios.")
    st.code("python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8")
    if st.button("Recargar vista"):
        st.cache_data.clear()
        st.rerun()

universe = load_universe()
portfolio = load_portfolio()

tab_calendar, tab_portfolio, tab_strategy, tab_data, tab_status = st.tabs(["Calendario", "Mi cartera", "Estrategia", "Datos", "Estado"])

with tab_portfolio:
    st.subheader("Cartera")
    st.caption("Anade tickers y acciones para estimar cobros proximos. Los datos se guardan localmente.")
    edited = st.data_editor(
        portfolio,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker", required=True),
            "shares": st.column_config.NumberColumn("Acciones", min_value=0.0, step=1.0),
            "avg_cost": st.column_config.NumberColumn("Coste medio", min_value=0.0, step=0.01),
            "notes": st.column_config.TextColumn("Notas"),
        },
    )
    if st.button("Guardar cartera", type="primary"):
        edited = apply_portfolio_ticker_resolution(edited, universe)
        save_portfolio(edited)
        st.success("Cartera guardada.")
        st.cache_data.clear()

start_text = start_date.isoformat()
end_text = end_date.isoformat()
events = events_between(start_text, end_text)
portfolio = load_portfolio()
portfolio["ticker"] = portfolio.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
portfolio["shares"] = pd.to_numeric(portfolio.get("shares", 0), errors="coerce").fillna(0)
portfolio["input_ticker"] = portfolio["ticker"]
portfolio["ticker"] = portfolio["ticker"].map(lambda value: resolve_unique_ticker(value, universe))

if not events.empty:
    events["_source_rank"] = events["source"].map({"nasdaq_calendar": 0, "yahoo_chart_dividends": 1}).fillna(9)
    events = (
        events.sort_values(["ticker", "ex_dividend_date", "cash_amount", "_source_rank"])
        .drop_duplicates(["ticker", "ex_dividend_date", "cash_amount"], keep="first")
        .drop(columns=["_source_rank"])
    )
    events["ex_dividend_date"] = pd.to_datetime(events["ex_dividend_date"]).dt.date
    events["cash_amount"] = pd.to_numeric(events["cash_amount"], errors="coerce").fillna(0)
    events = enrich_events(events, universe)

portfolio_events = events.merge(portfolio[["ticker", "shares"]], on="ticker", how="inner") if not events.empty and not portfolio.empty else pd.DataFrame()
if not portfolio_events.empty:
    portfolio_events["estimated_cash"] = portfolio_events["cash_amount"] * portfolio_events["shares"]

with tab_calendar:
    st.subheader("Proximos dividendos")
    c1, c2, c3, c4 = st.columns(4)
    total_events = len(events)
    companies = events["ticker"].nunique() if not events.empty else 0
    upcoming = events[events["ex_dividend_date"] >= today] if not events.empty else events
    portfolio_cash = portfolio_events["estimated_cash"].sum() if not portfolio_events.empty else 0
    c1.metric("Eventos", f"{total_events:,}")
    c2.metric("Empresas", f"{companies:,}")
    c3.metric("Pendientes", f"{len(upcoming):,}" if upcoming is not None else "0")
    c4.metric("Cartera estimada", fmt_money(portfolio_cash))

    selected_ticker = render_global_monthly_calendar(events, universe)

    sectors = ["Todos"] + sorted([x for x in events["sector_label"].dropna().unique().tolist() if x]) if not events.empty else ["Todos"]
    asset_types = ["Todos"] + sorted([x for x in events["asset_type"].dropna().unique().tolist() if x]) if not events.empty else ["Todos"]
    selected_asset_type = st.selectbox("Tipo de activo", asset_types, key="instrument_asset_type")
    selected_sector = st.selectbox("Sector", sectors, key="instrument_sector")
    ticker_search = st.text_input("Buscar instrumento", "", placeholder="Ticker o nombre: JGPI, Apple, JPM...", key="instrument_search")

    matched_instruments = search_universe(universe, ticker_search) if ticker_search.strip() else pd.DataFrame()
    if ticker_search.strip():
        if matched_instruments.empty:
            st.warning("No encuentro instrumentos en el universo local con ese texto.")
        else:
            st.markdown("**Instrumentos encontrados**")
            instrument_cols = ["ticker", "isin", "name", "exchange", "asset_type", "market_region"]
            instrument_view = matched_instruments[instrument_cols].head(100).reset_index(drop=True)
            clicked = selectable_ticker_table(
                instrument_view,
                "instrument_search_results",
                use_container_width=True,
                hide_index=True,
            )
            if clicked:
                selected_ticker = clicked
            options = instrument_view["ticker"].astype(str).tolist()
            if options:
                selected_index = options.index(selected_ticker) if selected_ticker in options else 0
                selected_ticker = st.selectbox("Abrir ficha", options, index=selected_index)

    view = events.copy()
    if not view.empty:
        if selected_asset_type != "Todos":
            view = view[view["asset_type"] == selected_asset_type]
        if selected_sector != "Todos":
            view = view[view["sector_label"] == selected_sector]
        if ticker_search.strip():
            q = ticker_search.strip().upper()
            matched_tickers = set(matched_instruments["ticker"].astype(str).str.upper().tolist()) if not matched_instruments.empty else set()
            view = view[
                view["ticker"].astype(str).str.upper().isin(matched_tickers)
                | view["ticker"].astype(str).str.upper().str.contains(q, regex=False)
                | view["company_name"].astype(str).str.upper().str.contains(q, regex=False)
            ]

    if selected_ticker:
        render_instrument_detail(selected_ticker, universe, events)

    if view.empty:
        st.info("No hay dividendos cargados para esta busqueda y rango.")
    else:
        show_cols = [
            "ex_dividend_date",
            "ticker",
            "isin",
            "company_name",
            "asset_type",
            "exchange",
            "sector_label",
            "sic_code",
            "sic_industry",
            "capture_avg_dividend_yield_pct",
            "event_yield_real_pct",
            "recovery_rate_pct",
            "median_recovery_days",
            "event_expected_tae_pct",
            "expected_tae_pct",
            "capture_score",
            "capture_cluster",
            "speed_cluster",
            "safety_cluster",
            "cash_amount",
            "currency",
            "status",
            "pay_date_display",
            "source",
        ]
        clicked = selectable_ticker_table(
            view[show_cols].rename(columns={"sector_label": "sector", "pay_date_display": "pay_date"}),
            "calendar_events_table",
            use_container_width=True,
            hide_index=True,
        )
        if clicked:
            render_instrument_detail(clicked, universe, events)

with tab_portfolio:
    st.subheader("Cobros estimados")
    if portfolio_events.empty:
        st.info("Guarda una cartera con tickers que tengan dividendos cargados en el rango.")
    else:
        show = portfolio_events.sort_values(["ex_dividend_date", "ticker"])
        cols = [
            "ex_dividend_date",
            "ticker",
            "company_name",
            "asset_type",
            "shares",
            "cash_amount",
            "currency",
            "estimated_cash",
            "pay_date_display",
            "status",
        ]
        clicked = selectable_ticker_table(
            show[cols].rename(columns={"pay_date_display": "pay_date"}),
            "portfolio_events_table",
            use_container_width=True,
            hide_index=True,
        )
        if clicked:
            render_instrument_detail(clicked, universe, events)
        monthly = show.copy()
        monthly["month"] = pd.to_datetime(monthly["ex_dividend_date"]).dt.to_period("M").astype(str)
        grouped = monthly.groupby("month", as_index=False)["estimated_cash"].sum()
        st.bar_chart(grouped, x="month", y="estimated_cash")

with tab_strategy:
    render_capture_strategy_tab()

with tab_data:
    st.subheader("Base local")
    st.write(f"Base: `{pipeline.DIVIDENDS_DB}`")
    st.write(f"Cartera: `{PORTFOLIO_CSV}`")
    if not events.empty:
        st.download_button(
            "Descargar CSV del rango",
            data=csv_download(events),
            file_name=f"dividend_events_{start_text}_{end_text}.csv",
            mime="text/csv",
        )
        clicked = selectable_ticker_table(events, "data_events_table", use_container_width=True, hide_index=True)
        if clicked:
            render_instrument_detail(clicked, universe, events)
    st.warning(
        "Primera version: ex-date e importe vienen de eventos de mercado Yahoo/Nasdaq; "
        "SEC/EDGAR se usa para universo y metadatos. Pay date y record date quedan "
        "preparados en el esquema para incorporar una fuente corporate-actions validada."
    )

with tab_status:
    st.subheader("Estado de actualizacion")
    status = data_status()
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Eventos totales", f"{status['total_events']:,}")
    s2.metric("Universo USA", f"{status['us_universe_rows']:,}")
    s3.metric("Universo Europa", f"{status['europe_universe_rows']:,}")
    s4.metric("Commit codigo", status["code_commit"])

    s5, s6, s7 = st.columns(3)
    s5.metric("Primera ex-date", status["min_ex_date"])
    s6.metric("Ultima ex-date", status["max_ex_date"])
    s7.metric("DB modificada", status["db_updated"])

    st.code("python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8")
    st.caption(f"Base: {status['db_path']}")

    if status["runs"]:
        st.markdown("**Ultimas ejecuciones**")
        st.dataframe(pd.DataFrame(status["runs"]), use_container_width=True, hide_index=True)
    if status["sources"]:
        st.markdown("**Cobertura por fuente**")
        st.dataframe(pd.DataFrame(status["sources"]), use_container_width=True, hide_index=True)
    if status["asset_types"]:
        st.markdown("**Cobertura por tipo de activo**")
        st.dataframe(pd.DataFrame(status["asset_types"]), use_container_width=True, hide_index=True)

