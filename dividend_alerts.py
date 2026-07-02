from __future__ import annotations

import argparse
import os
import smtplib
import sqlite3
import ssl
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DIVIDENDS_DB = DATA_DIR / "dividends.db"
CAPTURE_TICKER_SIGNAL_CSV = DATA_DIR / "capture_ticker_signal.csv"


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "")
    if not value:
        return default
    return int(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Envia alertas de candidatos dividend capture por email.")
    parser.add_argument("--horizon-days", type=int, default=7, help="Dias futuros a incluir en el informe.")
    parser.add_argument("--capital", type=float, default=1000.0, help="Capital simulado por compra.")
    parser.add_argument("--min-success-30d-pct", type=float, default=80.0)
    parser.add_argument("--max-trend-risk", type=float, default=70.0)
    parser.add_argument("--min-yield-real-pct", type=float, default=2.0)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--send-email", action="store_true")
    parser.add_argument("--send-empty", action="store_true", help="Envia email aunque no haya candidatos.")
    parser.add_argument("--email-to", default=os.getenv("ALERT_EMAIL_TO", ""))
    parser.add_argument("--email-from", default=os.getenv("ALERT_EMAIL_FROM", ""))
    parser.add_argument("--smtp-host", default=os.getenv("SMTP_HOST", "smtp.gmail.com"))
    parser.add_argument("--smtp-port", type=int, default=env_int("SMTP_PORT", 587))
    parser.add_argument("--smtp-user", default=os.getenv("SMTP_USER", ""))
    parser.add_argument("--smtp-password", default=os.getenv("SMTP_PASSWORD", ""))
    return parser.parse_args()


def previous_buy_day(ex_date: pd.Timestamp) -> date:
    buy_day = ex_date.date() - timedelta(days=1)
    while buy_day.weekday() >= 5:
        buy_day -= timedelta(days=1)
    return buy_day


def load_upcoming_events(as_of: str, horizon_days: int) -> pd.DataFrame:
    if not DIVIDENDS_DB.exists():
        raise FileNotFoundError(f"No existe {DIVIDENDS_DB}")
    start = pd.Timestamp(as_of).date()
    end = start + timedelta(days=horizon_days)
    conn = sqlite3.connect(DIVIDENDS_DB)
    try:
        events = pd.read_sql_query(
            """
            SELECT ticker, company_name, exchange, sector, asset_type,
                   ex_dividend_date, pay_date, cash_amount, currency, source
            FROM dividend_events
            WHERE ex_dividend_date>=? AND ex_dividend_date<=?
              AND cash_amount IS NOT NULL AND cash_amount>0
            ORDER BY ex_dividend_date, ticker
            """,
            conn,
            params=(start.isoformat(), end.isoformat()),
        )
    finally:
        conn.close()
    if events.empty:
        return events
    events["ticker"] = events["ticker"].astype(str).str.upper().str.strip()
    events["ex_dividend_date"] = pd.to_datetime(events["ex_dividend_date"], errors="coerce")
    events["cash_amount"] = pd.to_numeric(events["cash_amount"], errors="coerce")
    events = events.dropna(subset=["ticker", "ex_dividend_date", "cash_amount"])
    events["buy_date"] = events["ex_dividend_date"].map(previous_buy_day)
    events["ex_date"] = events["ex_dividend_date"].dt.date
    events = events[events["buy_date"] >= start].copy()
    return events


def load_signal() -> pd.DataFrame:
    if not CAPTURE_TICKER_SIGNAL_CSV.exists():
        raise FileNotFoundError(f"No existe {CAPTURE_TICKER_SIGNAL_CSV}")
    signal = pd.read_csv(CAPTURE_TICKER_SIGNAL_CSV)
    if signal.empty or "ticker" not in signal.columns:
        return pd.DataFrame()
    signal["ticker"] = signal["ticker"].astype(str).str.upper().str.strip()
    numeric_cols = [
        "latest_entry_price",
        "success_30d_pct",
        "median_recovery_30d_days",
        "avg_forced_30d_total_return_pct",
        "worst_forced_30d_total_return_pct",
        "trend_risk_score",
        "trend_score_multiplier",
        "trend_adjusted_capture_score",
        "capture_score",
        "recovery_rate_pct",
        "median_recovery_days",
    ]
    for col in numeric_cols:
        if col in signal.columns:
            signal[col] = pd.to_numeric(signal[col], errors="coerce")
    return signal.drop_duplicates("ticker", keep="first")


def build_candidates(args: argparse.Namespace) -> pd.DataFrame:
    events = load_upcoming_events(args.as_of, args.horizon_days)
    signal = load_signal()
    if events.empty or signal.empty:
        return pd.DataFrame()
    candidates = events.merge(signal, on="ticker", how="inner", suffixes=("", "_signal"))
    if candidates.empty:
        return candidates
    reference_price = pd.to_numeric(candidates.get("latest_entry_price"), errors="coerce")
    candidates["yield_real_pct"] = candidates["cash_amount"] / reference_price * 100
    candidates["expected_dividend_cash"] = float(args.capital) * candidates["yield_real_pct"] / 100
    recovery_days = pd.to_numeric(candidates.get("median_recovery_days"), errors="coerce").replace(0, pd.NA)
    recovery_rate = pd.to_numeric(candidates.get("recovery_rate_pct"), errors="coerce").fillna(0)
    trend_multiplier = pd.to_numeric(candidates.get("trend_score_multiplier"), errors="coerce").fillna(1.0)
    candidates["event_expected_tae_pct"] = candidates["yield_real_pct"] * 365 / recovery_days * recovery_rate / 100
    candidates["event_trend_adjusted_tae_pct"] = candidates["event_expected_tae_pct"] * trend_multiplier
    candidates = candidates[
        (pd.to_numeric(candidates["success_30d_pct"], errors="coerce").fillna(-1) >= float(args.min_success_30d_pct))
        & (pd.to_numeric(candidates["trend_risk_score"], errors="coerce").fillna(100) <= float(args.max_trend_risk))
        & (pd.to_numeric(candidates["yield_real_pct"], errors="coerce").fillna(-1) >= float(args.min_yield_real_pct))
    ].copy()
    if candidates.empty:
        return candidates
    candidates = candidates.sort_values(
        ["buy_date", "success_30d_pct", "trend_risk_score", "event_trend_adjusted_tae_pct"],
        ascending=[True, False, True, False],
    )
    return candidates.head(int(args.top))


def money(value: object, currency: str = "") -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{amount:,.2f} {currency}".strip()


def pct(value: object, decimals: int = 1) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):.{decimals}f}%"
    except (TypeError, ValueError):
        return "-"


def compose_report(candidates: pd.DataFrame, args: argparse.Namespace) -> tuple[str, str, str]:
    start = pd.Timestamp(args.as_of).date()
    end = start + timedelta(days=int(args.horizon_days))
    subject = f"Dividend capture: {len(candidates)} candidatos {start}..{end}"
    if candidates.empty:
        text = (
            f"No hay candidatos esta semana con exito 30d >= {args.min_success_30d_pct:.0f}% "
            f"y riesgo tendencia <= {args.max_trend_risk:.0f}.\n"
        )
        html = f"<p>{text}</p>"
        return subject, text, html

    lines = [
        f"Candidatos dividend capture para {start}..{end}",
        f"Filtros: compra {money(args.capital, 'EUR')} | exito 30d >= {args.min_success_30d_pct:.0f}% | riesgo tendencia <= {args.max_trend_risk:.0f}",
        "",
    ]
    html_rows = []
    for row in candidates.itertuples(index=False):
        currency = getattr(row, "currency", "") or ""
        line = (
            f"- Comprar {row.ticker} el {row.buy_date}: ex-date {row.ex_date}; "
            f"dividendo {money(row.cash_amount, currency)}; ganancia esperada "
            f"{money(row.expected_dividend_cash, currency)} por {money(args.capital, 'EUR')}; "
            f"exito 30d {pct(row.success_30d_pct)}; recuperacion mediana 30d "
            f"{getattr(row, 'median_recovery_30d_days', '-') or '-'} dias; "
            f"riesgo tendencia {pct(row.trend_risk_score, 0).replace('%', '/100')}; "
            f"TAE aj. {pct(row.event_trend_adjusted_tae_pct)}."
        )
        lines.append(line)
        html_rows.append(
            "<tr>"
            f"<td>{row.buy_date}</td>"
            f"<td>{row.ex_date}</td>"
            f"<td><strong>{row.ticker}</strong><br>{getattr(row, 'company_name', '') or ''}</td>"
            f"<td>{money(row.cash_amount, currency)}</td>"
            f"<td>{money(row.expected_dividend_cash, currency)}</td>"
            f"<td>{pct(row.yield_real_pct, 2)}</td>"
            f"<td>{pct(row.success_30d_pct)}</td>"
            f"<td>{getattr(row, 'median_recovery_30d_days', '-') or '-'}</td>"
            f"<td>{pct(row.trend_risk_score, 0).replace('%', '/100')}</td>"
            f"<td>{pct(row.event_trend_adjusted_tae_pct)}</td>"
            f"<td>{pct(row.avg_forced_30d_total_return_pct, 2)}</td>"
            "</tr>"
        )
    text = "\n".join(lines)
    html = f"""
    <h2>Dividend capture: candidatos de la semana</h2>
    <p><strong>Filtros:</strong> compra {money(args.capital, 'EUR')};
    exito 30d >= {args.min_success_30d_pct:.0f}%;
    riesgo tendencia <= {args.max_trend_risk:.0f};
    yield real >= {args.min_yield_real_pct:.2f}%.</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px">
      <thead>
        <tr>
          <th>Comprar</th><th>Ex-date</th><th>Ticker</th><th>Dividendo</th>
          <th>Ganancia 1000</th><th>Yield real</th><th>Exito 30d</th>
          <th>Mediana dias</th><th>Riesgo tendencia</th><th>TAE aj.</th><th>Ret venta 30d</th>
        </tr>
      </thead>
      <tbody>{''.join(html_rows)}</tbody>
    </table>
    <p>Nota: si no recupera antes de 30 dias, la regla analizada asume venta forzada al precio disponible el dia 30.</p>
    """
    return subject, text, html


def send_email(subject: str, text: str, html: str, args: argparse.Namespace) -> None:
    missing = [
        name
        for name, value in {
            "SMTP_HOST": args.smtp_host,
            "SMTP_USER": args.smtp_user,
            "SMTP_PASSWORD": args.smtp_password,
            "ALERT_EMAIL_TO": args.email_to,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Faltan variables/secrets para enviar email: {', '.join(missing)}")
    sender = args.email_from or args.smtp_user
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = args.email_to
    message.set_content(text)
    message.add_alternative(html, subtype="html")
    context = ssl.create_default_context()
    with smtplib.SMTP(args.smtp_host, args.smtp_port, timeout=30) as server:
        server.starttls(context=context)
        server.login(args.smtp_user, args.smtp_password)
        server.send_message(message)


def main() -> None:
    args = parse_args()
    candidates = build_candidates(args)
    subject, text, html = compose_report(candidates, args)
    print(text)
    if args.send_email and (not candidates.empty or args.send_empty):
        send_email(subject, text, html, args)
        print(f"Email enviado a {args.email_to}")
    elif args.send_email:
        print("Sin candidatos: no se envia email porque --send-empty no esta activo.")


if __name__ == "__main__":
    main()
