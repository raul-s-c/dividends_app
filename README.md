# Dividend Calendar USA

App Streamlit y pipeline diario para mantener un calendario de dividendos USA.
Permite cargar una cartera local y estimar cobros por `ex-dividend date`,
importe por accion y, cuando esta disponible, `payment date`.

## Datos incluidos

- `data/us_universe.csv`: universo base de 3.904 tickers USA (`NYSE`,
  `Nasdaq`, `CBOE`) exportado desde el proyecto SEC original.
- `data/dividends.db`: base SQLite con los eventos de dividendos.
- `nasdaq_calendar`: fuente principal para `ex_dividend_date`, `pay_date`,
  `record_date`, `declaration_date` e importe.
- `yahoo_chart_dividends`: extractor historico complementario para ex-date e
  importe.

## Actualizacion diaria

El repo incluye GitHub Actions:

```text
.github/workflows/update-dividends.yml
```

Se ejecuta cada dia a las `07:20 UTC` y tambien manualmente desde
`Actions -> Update dividend calendar -> Run workflow`.

El workflow usa actualizacion incremental:

- Recalcula solo una ventana movil.
- Por defecto rehace los ultimos `95` dias, aproximadamente un trimestre.
- Tambien refresca los proximos `550` dias, para mantener anuncios futuros.
- El historico anterior a la ventana no se borra ni se recalcula.
- Si la fuente falla en algun dia, no reemplaza la ventana completa; conserva la
  base previa y solo intenta upsert de lo que si haya respondido.

Comando diario:

```powershell
python dividend_calendar_pipeline.py --source nasdaq --incremental --lookback-days 95 --forward-days 550 --workers 8 --include-unmatched
```

Despues valida que haya eventos y commitea `data/dividends.db` si cambia.

## Ejecutar en local

Actualizar datos:

```powershell
python dividend_calendar_pipeline.py --source nasdaq --incremental --lookback-days 95 --forward-days 550 --workers 8 --include-unmatched
```

O en Windows:

```powershell
actualizar_dividendos.bat
```

Rebuild amplio manual:

```powershell
python dividend_calendar_pipeline.py --source nasdaq --rebuild --incremental --workers 8 --include-unmatched
```

Abrir app:

```powershell
streamlit run streamlit_dividends_app.py
```

O en Windows:

```powershell
abrir_dividends_streamlit.bat
```

## ETFs

Si. El pipeline ya soporta ETFs de forma practica:

- `--include-unmatched` conserva eventos de Nasdaq que no estan en el universo
  SEC local.
- Los no emparejados se clasifican como `ETF/Fund`, `Preferred`,
  `Note/Bond` u `Other Unmatched` segun el nombre publicado por Nasdaq.
- La cartera permite introducir cualquier ticker.
- Si quieres consultar un ticker concreto por Yahoo, aunque no exista en el
  universo SEC, puedes usar:

```powershell
python dividend_calendar_pipeline.py --source yahoo --ticker SPY --start 2025-01-01 --end 2027-01-01
```

## Notas para producto publico

Para una app publica en Play Store conviene validar terminos/licencia de la
fuente definitiva o contratar una API corporate-actions comercial. Esta version
deja la arquitectura lista para cambiar la fuente sin rehacer la app.

## Estado de la primera base

La base local actual contiene:

- 21.746 eventos brutos.
- 13.248 eventos desde `nasdaq_calendar`, todos con `pay_date`.
- 8.498 eventos desde `yahoo_chart_dividends`.
- 8.369 eventos clasificados como `ETF/Fund`.
