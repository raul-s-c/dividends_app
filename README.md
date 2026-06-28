# Dividend Calendar USA

App Streamlit y pipeline diario para mantener un calendario de dividendos USA.
Permite cargar una cartera local y estimar cobros por `ex-dividend date`,
importe por accion y, cuando esta disponible, `payment date`.

## Datos incluidos

- `data/us_universe.csv`: universo base de 3.904 tickers USA (`NYSE`,
  `Nasdaq`, `CBOE`) exportado desde el proyecto SEC original y ampliado con
  ETFs/ETNs/fondos desde el screener ETF de Nasdaq.
- `data/europe_etf_universe.csv`: universo europeo inicial para ETFs UCITS en
  Alemania/Xetra, London Stock Exchange, Borsa Italiana y Euronext Amsterdam.
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
python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8
```

Este es el comando para actualizar TODO: universo USA, universo europeo,
dividendos USA y dividendos europeos en ventana incremental.

Actualizar solo el universo USA/ETF sin tocar dividendos:

```powershell
python dividend_calendar_pipeline.py --universe-only --workers 8
```

Actualizar solo el universo europeo sin tocar dividendos:

```powershell
python dividend_calendar_pipeline.py --europe-universe-only --workers 8
```

Despues valida que haya eventos y commitea `data/dividends.db` si cambia.

## Ejecutar en local

Actualizar datos:

```powershell
python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8
```

O en Windows:

```powershell
actualizar_dividendos.bat
```

Rebuild amplio manual:

```powershell
python dividend_calendar_pipeline.py --daily-update --rebuild --workers 8
```

Abrir app:

```powershell
streamlit run streamlit_dividends_app.py
```

La app web incluye una pestana `Estado` con:

- commit de codigo local;
- fecha de modificacion de `data/dividends.db`;
- recuentos de universo USA y Europa;
- cobertura por fuente y tipo de activo;
- ultimas ejecuciones registradas en `dividend_runs`.

O en Windows:

```powershell
abrir_dividends_streamlit.bat
```

## APK Android

El repo incluye un primer proyecto Android nativo en `android/`.

Para construir/publicar un APK sin pasarlo manualmente:

1. En GitHub, abre `Actions`.
2. Ejecuta `Build Android APK`.
3. Indica `versionName`, `versionCode` y notas.
4. El workflow compila el APK y actualiza:

```text
releases/DividendCalendar-<version>-debug.apk
releases/update.json
```

La app Android consulta:

```text
https://raw.githubusercontent.com/raul-s-c/dividends_app/main/releases/update.json
```

Ese manifest sigue el mismo patron que `nubeplay-releases`: contiene
`versionCode`, `versionName`, `apkUrl` y `notes`. Si hay una version superior,
la app ofrece abrir la descarga del APK.

Para que una APK pueda actualizar encima de la anterior, configura en GitHub
estos secretos con un keystore estable:

```text
ANDROID_KEYSTORE_BASE64
ANDROID_KEYSTORE_PASSWORD
ANDROID_KEY_ALIAS
ANDROID_KEY_PASSWORD
```

Ahora el workflow publica APK debug para que el primer ciclo sea simple. Para
Play Store o distribucion estable hay que firmar APK/AAB release con keystore
fijo y cambiar el workflow a `assembleRelease`.

## ETFs

Si. El pipeline ya soporta ETFs de forma practica:

- `--daily-update` refresca primero `data/us_universe.csv` con ETFs/ETNs/fondos
  del screener ETF de Nasdaq, no solo con valores presentes en SEC.
- Tambien refresca `data/europe_etf_universe.csv` para ETFs europeos con
  simbolos Yahoo como `JGPI.DE`, `JEPG.L`, `JEPG.MI`, `VUSA.AS` o `IWDA.AS`.
- `--universe-only` permite actualizar solo ese universo de tickers.
- `--europe-universe-only` permite actualizar solo el universo europeo.
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

Ejemplo europeo validado:

```powershell
python dividend_calendar_pipeline.py --source yahoo --ticker JGPI.DE --start 2025-01-01 --end 2027-01-01
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
- 7.179 instrumentos en `data/us_universe.csv`.
- 3.207 ETFs/ETNs/fondos unicos cargados desde el screener ETF.
