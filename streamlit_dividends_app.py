from __future__ import annotations

import csv
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import streamlit as st

import dividend_calendar_pipeline as pipeline


APP_DIR = Path(__file__).resolve().parent
PORTFOLIO_CSV = APP_DIR / "data" / "portfolio.csv"

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
st.caption("Calendario personal de ex-dividend dates e importes para empresas cotizadas en USA.")

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
    st.caption("Ejecuta el pipeline desde terminal para refrescar datos diarios.")
    st.code("python dividends_app/dividend_calendar_pipeline.py --start 2025-01-01 --end 2027-01-01 --workers 8")

portfolio = load_portfolio()

tab_calendar, tab_portfolio, tab_data = st.tabs(["Calendario", "Mi cartera", "Datos"])

with tab_portfolio:
    st.subheader("Cartera")
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

if not events.empty:
    events["_source_rank"] = events["source"].map({"nasdaq_calendar": 0, "yahoo_chart_dividends": 1}).fillna(9)
    events = (
        events.sort_values(["ticker", "ex_dividend_date", "cash_amount", "_source_rank"])
        .drop_duplicates(["ticker", "ex_dividend_date", "cash_amount"], keep="first")
        .drop(columns=["_source_rank"])
    )
    events["ex_dividend_date"] = pd.to_datetime(events["ex_dividend_date"]).dt.date
    events["cash_amount"] = pd.to_numeric(events["cash_amount"], errors="coerce").fillna(0)

portfolio_events = events.merge(portfolio[["ticker", "shares"]], on="ticker", how="inner") if not events.empty and not portfolio.empty else pd.DataFrame()
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
        "Primera version: ex-date e importe vienen de eventos de mercado Yahoo; "
        "SEC/EDGAR se usa para universo y metadatos. Pay date y record date quedan "
        "preparados en el esquema para incorporar una fuente corporate-actions validada."
    )
