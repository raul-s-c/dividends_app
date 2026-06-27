# Android APK

Proyecto Android nativo para Dividend Calendar.

La app:

- Descarga `data/dividends.db` desde GitHub.
- Guarda una cartera local sencilla.
- Consulta proximos dividendos de los tickers de la cartera.
- Comprueba `releases/update.json` para avisar de APKs nuevas.

## Build en GitHub

Usa `Actions -> Build Android APK -> Run workflow`.

El workflow genera:

```text
releases/DividendCalendar-<version>-debug.apk
releases/update.json
```

Este flujo replica el patron de `nubeplay-releases`: la app mira un
`update.json` publicado en GitHub y descarga el APK desde `raw.githubusercontent`.

Para que las actualizaciones se instalen encima de la version anterior, configura
estos secretos del repo:

```text
ANDROID_KEYSTORE_BASE64
ANDROID_KEYSTORE_PASSWORD
ANDROID_KEY_ALIAS
ANDROID_KEY_PASSWORD
```

Ahora el workflow publica APK debug para pruebas. Para Play Store o distribucion
estable hay que firmar release con un keystore fijo y cambiar el workflow a
`assembleRelease`.
