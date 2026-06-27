package com.raulsc.dividendcalendar;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Intent;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.graphics.Color;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.provider.Settings;
import android.view.Gravity;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.core.content.FileProvider;

import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainActivity extends Activity {
    private static final String DATA_URL = "https://raw.githubusercontent.com/raul-s-c/dividends_app/main/data/dividends.db";
    private static final String UPDATE_URL = "https://raw.githubusercontent.com/raul-s-c/dividends_app/main/releases/update.json";

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Handler main = new Handler(Looper.getMainLooper());
    private final NumberFormat money = NumberFormat.getCurrencyInstance(Locale.US);

    private File dbFile;
    private SharedPreferences prefs;
    private LinearLayout root;
    private TextView status;
    private TextView portfolioText;
    private LinearLayout eventsList;
    private EditText tickerInput;
    private EditText sharesInput;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        dbFile = new File(getFilesDir(), "dividends.db");
        prefs = getSharedPreferences("portfolio", MODE_PRIVATE);
        buildUi();
        renderPortfolio();
        if (!dbFile.exists()) {
            refreshDatabase();
        } else {
            loadEvents();
            checkForUpdates(false);
        }
    }

    private void buildUi() {
        ScrollView scroll = new ScrollView(this);
        root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(32, 36, 32, 36);
        root.setBackgroundColor(Color.rgb(247, 248, 250));
        scroll.addView(root);

        TextView title = text("Dividend Calendar USA", 28, true);
        root.addView(title);
        status = text("Preparando calendario...", 14, false);
        status.setTextColor(Color.rgb(85, 93, 104));
        root.addView(status);

        LinearLayout buttons = new LinearLayout(this);
        buttons.setOrientation(LinearLayout.HORIZONTAL);
        buttons.setGravity(Gravity.CENTER_VERTICAL);
        buttons.setPadding(0, 24, 0, 16);
        Button refresh = button("Actualizar datos");
        refresh.setOnClickListener(v -> refreshDatabase());
        Button update = button("Buscar APK");
        update.setOnClickListener(v -> checkForUpdates(true));
        buttons.addView(refresh, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        buttons.addView(update, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        root.addView(buttons);

        root.addView(text("Mi cartera", 20, true));
        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.HORIZONTAL);
        tickerInput = input("Ticker");
        sharesInput = input("Acciones");
        sharesInput.setInputType(android.text.InputType.TYPE_CLASS_NUMBER | android.text.InputType.TYPE_NUMBER_FLAG_DECIMAL);
        Button add = button("Añadir");
        add.setOnClickListener(v -> addHolding());
        form.addView(tickerInput, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        form.addView(sharesInput, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        form.addView(add);
        root.addView(form);

        portfolioText = text("", 14, false);
        portfolioText.setPadding(0, 8, 0, 24);
        root.addView(portfolioText);

        root.addView(text("Próximos cobros", 20, true));
        eventsList = new LinearLayout(this);
        eventsList.setOrientation(LinearLayout.VERTICAL);
        root.addView(eventsList);

        setContentView(scroll);
    }

    private TextView text(String value, int sp, boolean bold) {
        TextView tv = new TextView(this);
        tv.setText(value);
        tv.setTextSize(sp);
        tv.setTextColor(Color.rgb(24, 31, 42));
        tv.setPadding(0, 6, 0, 6);
        if (bold) tv.setTypeface(android.graphics.Typeface.DEFAULT_BOLD);
        return tv;
    }

    private Button button(String value) {
        Button b = new Button(this);
        b.setText(value);
        b.setAllCaps(false);
        return b;
    }

    private EditText input(String hint) {
        EditText e = new EditText(this);
        e.setHint(hint);
        e.setSingleLine(true);
        return e;
    }

    private void addHolding() {
        String ticker = tickerInput.getText().toString().trim().toUpperCase(Locale.US);
        String sharesRaw = sharesInput.getText().toString().trim();
        if (ticker.isEmpty() || sharesRaw.isEmpty()) {
            toast("Introduce ticker y acciones.");
            return;
        }
        try {
            double shares = Double.parseDouble(sharesRaw);
            Map<String, Double> portfolio = getPortfolio();
            portfolio.put(ticker, shares);
            savePortfolio(portfolio);
            tickerInput.setText("");
            sharesInput.setText("");
            renderPortfolio();
            loadEvents();
        } catch (NumberFormatException ex) {
            toast("Acciones no válidas.");
        }
    }

    private Map<String, Double> getPortfolio() {
        Map<String, Double> map = new LinkedHashMap<>();
        String raw = prefs.getString("holdings", "");
        if (raw == null || raw.isEmpty()) return map;
        for (String part : raw.split(";")) {
            String[] bits = part.split(",");
            if (bits.length == 2) {
                try {
                    map.put(bits[0], Double.parseDouble(bits[1]));
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return map;
    }

    private void savePortfolio(Map<String, Double> portfolio) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Double> entry : portfolio.entrySet()) {
            if (sb.length() > 0) sb.append(';');
            sb.append(entry.getKey()).append(',').append(entry.getValue());
        }
        prefs.edit().putString("holdings", sb.toString()).apply();
    }

    private void renderPortfolio() {
        Map<String, Double> portfolio = getPortfolio();
        if (portfolio.isEmpty()) {
            portfolioText.setText("Añade tus tickers para estimar cobros.");
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Double> entry : portfolio.entrySet()) {
            if (sb.length() > 0) sb.append("  ·  ");
            sb.append(entry.getKey()).append(": ").append(trim(entry.getValue()));
        }
        portfolioText.setText(sb.toString());
    }

    private void refreshDatabase() {
        status.setText("Descargando base de dividendos...");
        executor.execute(() -> {
            try {
                download(DATA_URL, dbFile);
                main.post(() -> {
                    status.setText("Datos actualizados.");
                    loadEvents();
                    checkForUpdates(false);
                });
            } catch (Exception ex) {
                main.post(() -> {
                    status.setText("No se pudo actualizar la base.");
                    toast(ex.getMessage());
                    if (dbFile.exists()) loadEvents();
                });
            }
        });
    }

    private void loadEvents() {
        eventsList.removeAllViews();
        if (!dbFile.exists()) {
            eventsList.addView(text("Todavía no hay base local.", 15, false));
            return;
        }
        Map<String, Double> portfolio = getPortfolio();
        if (portfolio.isEmpty()) {
            eventsList.addView(text("Añade valores a tu cartera para ver cobros estimados.", 15, false));
            return;
        }
        executor.execute(() -> {
            List<String> rows = new ArrayList<>();
            try (SQLiteDatabase db = SQLiteDatabase.openDatabase(dbFile.getAbsolutePath(), null, SQLiteDatabase.OPEN_READONLY)) {
                String today = new SimpleDateFormat("yyyy-MM-dd", Locale.US).format(new Date());
                String placeholders = placeholders(portfolio.size());
                String sql = "SELECT ex_dividend_date,ticker,company_name,asset_type,cash_amount,currency,pay_date " +
                        "FROM dividend_events WHERE ex_dividend_date >= ? AND ticker IN (" + placeholders + ") " +
                        "ORDER BY ex_dividend_date,ticker LIMIT 120";
                String[] args = new String[portfolio.size() + 1];
                args[0] = today;
                int i = 1;
                for (String ticker : portfolio.keySet()) args[i++] = ticker;
                try (Cursor c = db.rawQuery(sql, args)) {
                    while (c.moveToNext()) {
                        String ex = c.getString(0);
                        String ticker = c.getString(1);
                        String name = c.getString(2);
                        String type = c.getString(3);
                        double amount = c.getDouble(4);
                        String pay = c.getString(6);
                        double shares = portfolio.get(ticker);
                        rows.add(ex + "  " + ticker + "  " + money.format(amount * shares) +
                                "\n" + safe(name) + " · " + safe(type) +
                                "\nDPS " + money.format(amount) + " · Pago " + safe(pay));
                    }
                }
            } catch (Exception ex) {
                rows.add("Error leyendo la base: " + ex.getMessage());
            }
            main.post(() -> renderRows(rows));
        });
    }

    private void renderRows(List<String> rows) {
        eventsList.removeAllViews();
        if (rows.isEmpty()) {
            eventsList.addView(text("No hay próximos dividendos para tu cartera.", 15, false));
            return;
        }
        for (String row : rows) {
            TextView tv = text(row, 15, false);
            tv.setBackgroundColor(Color.WHITE);
            tv.setPadding(20, 18, 20, 18);
            LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT
            );
            lp.setMargins(0, 0, 0, 16);
            eventsList.addView(tv, lp);
        }
    }

    private void checkForUpdates(boolean manual) {
        executor.execute(() -> {
            try {
                String raw = readUrl(UPDATE_URL);
                JSONObject obj = new JSONObject(raw);
                int latest = obj.getInt("versionCode");
                int current = getPackageManager().getPackageInfo(getPackageName(), 0).versionCode;
                if (latest > current) {
                    String name = obj.optString("versionName", "");
                    String notes = obj.optString("notes", "");
                    String apkUrl = obj.getString("apkUrl");
                    main.post(() -> showUpdateDialog(name, notes, apkUrl));
                } else if (manual) {
                    main.post(() -> toast("Ya tienes la última versión."));
                }
            } catch (Exception ex) {
                if (manual) main.post(() -> toast("No se pudo comprobar actualización."));
            }
        });
    }

    private void showUpdateDialog(String versionName, String notes, String apkUrl) {
        new AlertDialog.Builder(this)
                .setTitle("Nueva versión " + versionName)
                .setMessage(notes)
                .setPositiveButton("Descargar", (d, which) -> downloadAndInstall(apkUrl))
                .setNegativeButton("Luego", null)
                .show();
    }

    private void downloadAndInstall(String apkUrl) {
        status.setText("Descargando APK...");
        executor.execute(() -> {
            try {
                File apk = new File(getCacheDir(), "DividendCalendar-update.apk");
                download(apkUrl, apk);
                main.post(() -> installApk(apk));
            } catch (Exception ex) {
                main.post(() -> toast("No se pudo descargar el APK."));
            }
        });
    }

    private void installApk(File apk) {
        if (android.os.Build.VERSION.SDK_INT >= 26 && !getPackageManager().canRequestPackageInstalls()) {
            startActivity(new Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:" + getPackageName())));
            toast("Autoriza instalaciones de esta app y vuelve a pulsar actualizar.");
            return;
        }
        Uri uri = FileProvider.getUriForFile(this, getPackageName() + ".fileprovider", apk);
        Intent intent = new Intent(Intent.ACTION_VIEW);
        intent.setDataAndType(uri, "application/vnd.android.package-archive");
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION | Intent.FLAG_ACTIVITY_NEW_TASK);
        startActivity(intent);
    }

    private String readUrl(String urlValue) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlValue).openConnection();
        conn.setRequestProperty("User-Agent", "DividendCalendarAndroid/0.1");
        try (InputStream in = new BufferedInputStream(conn.getInputStream())) {
            byte[] data = new byte[8192];
            StringBuilder sb = new StringBuilder();
            int n;
            while ((n = in.read(data)) >= 0) {
                sb.append(new String(data, 0, n));
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    private void download(String urlValue, File out) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlValue).openConnection();
        conn.setRequestProperty("User-Agent", "DividendCalendarAndroid/0.1");
        File tmp = new File(out.getAbsolutePath() + ".tmp");
        try (InputStream in = new BufferedInputStream(conn.getInputStream());
             FileOutputStream fos = new FileOutputStream(tmp)) {
            byte[] data = new byte[1024 * 64];
            int n;
            while ((n = in.read(data)) >= 0) {
                fos.write(data, 0, n);
            }
        } finally {
            conn.disconnect();
        }
        if (out.exists() && !out.delete()) throw new IllegalStateException("No se pudo reemplazar " + out.getName());
        if (!tmp.renameTo(out)) throw new IllegalStateException("No se pudo guardar " + out.getName());
    }

    private String placeholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(',');
            sb.append('?');
        }
        return sb.toString();
    }

    private String safe(String value) {
        return value == null || value.isEmpty() ? "-" : value;
    }

    private String trim(double value) {
        if (Math.floor(value) == value) return String.valueOf((long) value);
        return String.valueOf(value);
    }

    private void toast(String value) {
        Toast.makeText(this, value, Toast.LENGTH_LONG).show();
    }
}
