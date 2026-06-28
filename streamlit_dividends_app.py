from __future__ import annotations

import csv
import sqlite3
import subprocess
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import streamlit as st

import dividend_calendar_pipeline as pipeline


APP_DIR = Path(__file__).resolve().parent
PORTFOLIO_CSV = APP_DIR / "data" / "portfolio.csv"
US_UNIVERSE_CSV = APP_DIR / "data" / "us_universe.csv"
EUROPE_UNIVERSE_CSV = APP_DIR / "data" / "europe_etf_universe.csv"

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
    return pd.read_csv(PORTFOLIO_CSV).fillna("")


def save_portfolio(df: pd.DataFrame) -> None:
    pipeline.DATA_DIR.mkdir(parents=True, exist_ok=True)
    clean = df.copy()
    clean["ticker"] = clean["ticker"].astype(str).str.upper().str.strip()
    clean = clean[clean["ticker"] != ""]
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


def resolve_portfolio_tickers(portfolio_df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_df.empty:
        portfolio_df["resolved_ticker"] = ""
        return portfolio_df
    resolved = portfolio_df.copy()
    resolved["resolved_ticker"] = resolved["ticker"]
    if universe_df.empty:
        return resolved
    lookup: dict[str, str] = {}
    for base, group in universe_df.groupby("ticker_base"):
        tickers = sorted(group["ticker"].dropna().astype(str).unique().tolist())
        if len(tickers) == 1:
            lookup[base] = tickers[0]
    resolved["resolved_ticker"] = resolved["ticker"].map(lambda value: lookup.get(value, value))
    return resolved


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
    st.caption("Comando único recomendado para refrescar datos diarios.")
    st.code("python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8")
    if st.button("Recargar vista"):
        st.cache_data.clear()
        st.rerun()

universe = load_universe()
portfolio = load_portfolio()

tab_calendar, tab_portfolio, tab_data, tab_status = st.tabs(["Calendario", "Mi cartera", "Datos", "Estado"])

with tab_portfolio:
    st.subheader("Cartera")
    st.markdown("**Buscar instrumento**")
    instrument_search = st.text_input("Ticker o nombre", "", placeholder="Ej: JGPI, JGPI.DE, JEPI, VUSA")
    if instrument_search.strip():
        q = instrument_search.strip().upper()
        matches = universe[
            universe["ticker"].astype(str).str.upper().str.contains(q, regex=False)
            | universe["ticker_base"].astype(str).str.upper().eq(q)
            | universe["name"].astype(str).str.upper().str.contains(q, regex=False)
        ].copy()
        if matches.empty:
            st.warning("No encuentro ese ticker en el universo local. Prueba con el sufijo de mercado, por ejemplo .DE, .L, .MI o .AS.")
        else:
            st.dataframe(
                matches[["ticker", "name", "exchange", "asset_type", "market_region"]].head(25),
                use_container_width=True,
                hide_index=True,
            )
    st.caption("Añade tickers y acciones para estimar cobros próximos. Los datos se guardan localmente.")
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
        save_portfolio(edited)
        st.success("Cartera guardada.")
        st.cache_data.clear()

start_text = start_date.isoformat()
end_text = end_date.isoformat()
events = events_between(start_text, end_text)
portfolio = load_portfolio()
portfolio["ticker"] = portfolio.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
portfolio["shares"] = pd.to_numeric(portfolio.get("shares", 0), errors="coerce").fillna(0)
portfolio = resolve_portfolio_tickers(portfolio, universe)

if not events.empty:
    events["_source_rank"] = events["source"].map({"nasdaq_calendar": 0, "yahoo_chart_dividends": 1}).fillna(9)
    events = (
        events.sort_values(["ticker", "ex_dividend_date", "cash_amount", "_source_rank"])
        .drop_duplicates(["ticker", "ex_dividend_date", "cash_amount"], keep="first")
        .drop(columns=["_source_rank"])
    )
    events["ex_dividend_date"] = pd.to_datetime(events["ex_dividend_date"]).dt.date
    events["cash_amount"] = pd.to_numeric(events["cash_amount"], errors="coerce").fillna(0)

if not events.empty and not portfolio.empty:
    portfolio_events = events.merge(
        portfolio[["ticker", "resolved_ticker", "shares"]].rename(columns={"ticker": "portfolio_ticker"}),
        left_on="ticker",
        right_on="resolved_ticker",
        how="inner",
    ).drop(columns=["resolved_ticker"])
else:
    portfolio_events = pd.DataFrame()
if not portfolio_events.empty:
    portfolio_events["estimated_cash"] = portfolio_events["cash_amount"] * portfolio_events["shares"]

with tab_calendar:
    st.subheader("Próximos dividendos")
    c1, c2, c3, c4 = st.columns(4)
    total_events = len(events)
    companies = events["ticker"].nunique() if not events.empty else 0
    upcoming = events[events["ex_dividend_date"] >= today] if not events.empty else events
    portfolio_cash = portfolio_events["estimated_cash"].sum() if not portfolio_events.empty else 0
    c1.metric("Eventos", f"{total_events:,}")
    c2.metric("Empresas", f"{companies:,}")
    c3.metric("Pendientes", f"{len(upcoming):,}" if upcoming is not None else "0")
    c4.metric("Cartera estimada", fmt_money(portfolio_cash))

    if events.empty:
        st.info("No hay dividendos cargados para este rango. Ejecuta primero el pipeline.")
    else:
        sectors = ["Todos"] + sorted([x for x in events["sector"].dropna().unique().tolist() if x])
        asset_types = ["Todos"] + sorted([x for x in events["asset_type"].dropna().unique().tolist() if x])
        selected_asset_type = st.selectbox("Tipo de activo", asset_types)
        selected_sector = st.selectbox("Sector", sectors)
        ticker_search = st.text_input("Buscar ticker o empresa", "")
        view = events.copy()
        if selected_asset_type != "Todos":
            view = view[view["asset_type"] == selected_asset_type]
        if selected_sector != "Todos":
            view = view[view["sector"] == selected_sector]
        if ticker_search.strip():
            q = ticker_search.strip().upper()
            view = view[
                view["ticker"].astype(str).str.upper().str.contains(q, regex=False)
                | view["company_name"].astype(str).str.upper().str.contains(q, regex=False)
            ]
        show_cols = [
            "ex_dividend_date",
            "ticker",
            "company_name",
            "asset_type",
            "exchange",
            "sector",
            "cash_amount",
            "currency",
            "status",
            "pay_date",
            "source",
        ]
        st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

with tab_portfolio:
    st.subheader("Cobros estimados")
    resolved_view = portfolio[portfolio["ticker"] != portfolio["resolved_ticker"]] if "resolved_ticker" in portfolio.columns else pd.DataFrame()
    if not resolved_view.empty:
        st.markdown("**Tickers resueltos**")
        st.dataframe(
            resolved_view[["ticker", "resolved_ticker", "shares"]].rename(
                columns={"ticker": "ticker introducido", "resolved_ticker": "ticker usado"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    if portfolio_events.empty:
        st.info("Guarda una cartera con tickers que tengan dividendos cargados en el rango.")
    else:
        show = portfolio_events.sort_values(["ex_dividend_date", "ticker"])
        cols = [
            "ex_dividend_date",
            "portfolio_ticker",
            "ticker",
            "company_name",
            "asset_type",
            "shares",
            "cash_amount",
            "currency",
            "estimated_cash",
            "pay_date",
            "status",
        ]
        st.dataframe(show[cols], use_container_width=True, hide_index=True)
        monthly = show.copy()
        monthly["month"] = pd.to_datetime(monthly["ex_dividend_date"]).dt.to_period("M").astype(str)
        grouped = monthly.groupby("month", as_index=False)["estimated_cash"].sum()
        st.bar_chart(grouped, x="month", y="estimated_cash")

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
        st.dataframe(events, use_container_width=True, hide_index=True)
    st.warning(
        "Primera versión: ex-date e importe vienen de eventos de mercado Yahoo/Nasdaq; "
        "SEC/EDGAR se usa para universo y metadatos. Pay date y record date quedan "
        "preparados en el esquema para incorporar una fuente corporate-actions validada."
    )

with tab_status:
    st.subheader("Estado de actualización")
    status = data_status()
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Eventos totales", f"{status['total_events']:,}")
    s2.metric("Universo USA", f"{status['us_universe_rows']:,}")
    s3.metric("Universo Europa", f"{status['europe_universe_rows']:,}")
    s4.metric("Commit código", status["code_commit"])

    s5, s6, s7 = st.columns(3)
    s5.metric("Primera ex-date", status["min_ex_date"])
    s6.metric("Última ex-date", status["max_ex_date"])
    s7.metric("DB modificada", status["db_updated"])

    st.code("python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8")
    st.caption(f"Base: {status['db_path']}")

    if status["runs"]:
        st.markdown("**Últimas ejecuciones**")
        st.dataframe(pd.DataFrame(status["runs"]), use_container_width=True, hide_index=True)
    if status["sources"]:
        st.markdown("**Cobertura por fuente**")
        st.dataframe(pd.DataFrame(status["sources"]), use_container_width=True, hide_index=True)
    if status["asset_types"]:
        st.markdown("**Cobertura por tipo de activo**")
        st.dataframe(pd.DataFrame(status["asset_types"]), use_container_width=True, hide_index=True)
