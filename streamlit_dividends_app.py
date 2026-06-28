from __future__ import annotations

import csv
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
        europe["sector"] = ""
        europe["market_region"] = "Europa"
        frames.append(europe)
    if not frames:
        return pd.DataFrame(columns=["ticker", "name", "exchange", "sector", "state", "asset_type", "market_region"])
    universe = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    universe["ticker"] = universe["ticker"].astype(str).str.upper().str.strip()
    universe["ticker_base"] = universe["ticker"].str.split(".").str[0]
    return universe.drop_duplicates(["ticker"], keep="first")


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
    for col in ["ticker", "company_name", "exchange", "sector", "asset_type", "state", "pay_date"]:
        if col not in enriched.columns:
            enriched[col] = ""
    if not universe_df.empty:
        meta_cols = ["ticker", "name", "exchange", "sector", "asset_type", "state", "market_region"]
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
        return

    month_options = calendar["ex_dividend_date"].dt.to_period("M").astype(str).sort_values().unique().tolist()
    current_month = date.today().strftime("%Y-%m")
    default_index = month_options.index(current_month) if current_month in month_options else 0

    f1, f2, f3, f4 = st.columns(4)
    selected_month = f1.selectbox("Mes", month_options, index=default_index, key="global_calendar_month")
    regions = ["Todos"] + sorted([x for x in calendar.get("market_region", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
    types = ["Todos"] + sorted([x for x in calendar.get("asset_type", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
    sectors = ["Todos"] + sorted([x for x in calendar.get("sector_label", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
    selected_region = f2.selectbox("Region", regions, key="global_calendar_region")
    selected_type = f3.selectbox("Asset type", types, key="global_calendar_asset_type")
    selected_sector = f4.selectbox("Sector", sectors, key="global_calendar_sector")
    text_filter = st.text_input("Filtrar calendario", "", placeholder="Ticker o nombre", key="global_calendar_text")

    monthly_view = calendar[calendar["ex_dividend_date"].dt.to_period("M").astype(str) == selected_month].copy()
    if selected_region != "Todos":
        monthly_view = monthly_view[monthly_view["market_region"] == selected_region]
    if selected_type != "Todos":
        monthly_view = monthly_view[monthly_view["asset_type"] == selected_type]
    if selected_sector != "Todos":
        monthly_view = monthly_view[monthly_view["sector_label"] == selected_sector]
    if text_filter.strip():
        q = text_filter.strip().upper()
        matched_instruments = search_universe(universe_df, text_filter)
        matched_tickers = set(matched_instruments["ticker"].astype(str).str.upper().tolist()) if not matched_instruments.empty else set()
        monthly_view = monthly_view[
            monthly_view["ticker"].astype(str).str.upper().isin(matched_tickers)
            | monthly_view["ticker"].astype(str).str.upper().str.contains(q, regex=False)
            | monthly_view["company_name"].astype(str).str.upper().str.contains(q, regex=False)
        ]

    if monthly_view.empty:
        if text_filter.strip():
            matches = search_universe(universe_df, text_filter)
            if not matches.empty:
                st.info("No hay eventos en este mes para esos filtros, pero estos instrumentos existen en el universo.")
                show = matches[["ticker", "name", "exchange", "asset_type", "market_region"]].head(50).reset_index(drop=True)
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
    show_cols = [
        "ex_date",
        "payment_day",
        "ticker",
        "company_name",
        "cash_amount",
        "currency",
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
                "asset_type": "asset type",
                "market_region": "region",
                "sector_label": "sector",
                "sic_code": "SIC",
                "sic_industry": "industria SEC",
            }
        )
    clicked = selectable_ticker_table(
        display,
        "global_calendar_events",
        use_container_width=True,
        hide_index=True,
    )
    return clicked


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
        "Nombre": name,
        "Exchange": info.get("exchange") or profile.get("exchange") or "",
        "Sector": sector,
        "SIC": sic_code,
        "Industria SEC": sic_industry,
        "Estado/Pais": profile.get("state") or info.get("state") or "",
        "Entidad": profile.get("entity_type") or "",
        "Anios SEC": f"{profile.get('min_year', '')}-{profile.get('max_year', '')}".strip("-"),
        "Fuente universo": info.get("provider") or "",
    }
    st.dataframe(
        pd.DataFrame([meta]).replace("", "-"),
        use_container_width=True,
        hide_index=True,
    )
    if description:
        st.caption(description)

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
) -> pd.DataFrame:
    settings = capture.CaptureSettings(
        start=start,
        end=end,
        max_recovery_days=max_recovery_days,
        min_dividend_yield_pct=min_dividend_yield_pct,
        limit_tickers=limit_tickers,
        use_high_for_recovery=use_high_for_recovery,
    )
    return capture.run_capture_backtest(settings, max_events=max_events)


def render_capture_strategy_tab() -> None:
    st.subheader("Estrategia compra pre ex-date")
    st.caption(
        "Backtest experimental: compra al cierre previo al ex-date, cobra dividendo "
        "y vende cuando el cierre recupera el precio de entrada."
    )
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
                    "events",
                    "recovery_rate_pct",
                    "median_recovery_days",
                    "avg_dividend_yield_pct",
                    "avg_annualized_return_pct",
                ]
            ],
            "strategy_summary_table",
            use_container_width=True,
            hide_index=True,
        )
        if clicked:
            render_instrument_detail(clicked, universe, events)

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
            instrument_cols = ["ticker", "name", "exchange", "asset_type", "market_region"]
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
            "company_name",
            "asset_type",
            "exchange",
            "sector_label",
            "sic_code",
            "sic_industry",
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

