import sys, time
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
 
BASE = "https://finance.yahoo.com/markets/stocks/gainers"
 
# Paths adapted to project structure
ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT_DIR / "output"

#Configuración del driver de Selenium
def build_driver(headless=False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--ignore-ssl-errors=yes")
    opts.add_argument("--allow-running-insecure-content")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame,PrivacySandboxAdsAPIs")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-quic")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(90)
    d.implicitly_wait(0)
    return d
 
# Esperar hasta que haya al menos `min_rows` filas en la tabla
def wait_rows(d, min_rows, timeout=60):
    WebDriverWait(d, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, "section table")))
    WebDriverWait(d, timeout).until(lambda x: len(x.find_elements(By.CSS_SELECTOR, "section table tbody tr")) >= min_rows)
 
# Aceptar cookies si aparece el botón
def accept_cookies(d):
    for by, sel in [
        (By.CSS_SELECTOR, "button[aria-label*='Accept']"),
        (By.XPATH, "//button[contains(., 'Accept') or contains(., 'Aceptar') or contains(.,'Agree')]"),
    ]:
        btns = d.find_elements(by, sel)
        if btns:
            try:
                d.execute_script("arguments[0].scrollIntoView({block:'center'});", btns[0])
                time.sleep(0.2)
                btns[0].click()
                time.sleep(0.5)
                break
            except Exception:
                pass
 
# Extraer (símbolo, nombre) de las filas de la tabla
def extract_rows(d):
    out = []
    rows = d.find_elements(By.CSS_SELECTOR, "section table tbody tr")
    for r in rows:
        try:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) >= 2:
                sym = tds[0].text.strip()
                name = tds[1].text.strip()
                if sym and name:
                    out.append((sym, name))
        except StaleElementReferenceException:
            continue
    return out
 
# Cargar la URL con reintentos y esperar filas
def load_with_retry(d, url, min_rows, tries=3):
    last_err = None
    for _ in range(tries):
        try:
            d.get(url)
            accept_cookies(d)
            time.sleep(2)
            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_rows(d, min_rows=min_rows, timeout=60)
            return True
        except Exception as e:
            last_err = e
            time.sleep(3)
    if last_err:
        print("Carga fallida:", last_err, file=sys.stderr)
    return False
 
# Función principal
def main():
    d = build_driver(headless=False)
    try:
        # --- SCRAPING 50 TOP GAINERS ---
        data = []
 
        ok1 = load_with_retry(d, f"{BASE}?count=25&offset=0", min_rows=25)
        if not ok1:
            print("No se pudo cargar la página 1 de ganadores.", file=sys.stderr)
            sys.exit(1)
        page1 = extract_rows(d)[:25]
        data.extend(page1)
 
        ok2 = load_with_retry(d, f"{BASE}?count=25&offset=25", min_rows=25)
        if not ok2:
            print("No se pudo cargar la página 2 de ganadores.", file=sys.stderr)
            sys.exit(1)
        page2 = extract_rows(d)[:25]
        seen = set(data)
        for tup in page2:
            if tup not in seen and len(data) < 50:
                data.append(tup)
                seen.add(tup)
 
        if len(data) < 50:
            if load_with_retry(d, f"{BASE}?count=100", min_rows=50):
                allrows = extract_rows(d)
                unique, s = [], set()
                for tup in allrows:
                    if tup not in s:
                        unique.append(tup); s.add(tup)
                    if len(unique) == 50:
                        break
                if len(unique) >= len(data):
                    data = unique
 
        df = pd.DataFrame(data, columns=["symbol", "name"]).drop_duplicates().head(50)
 
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", 0)
        print("\nTOP GAINERS (50):")
        print(df.to_string(index=False))
 
        out_csv = OUTPUT_DIR / "yahoo_top_gainers_50.csv"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False, sep=";", encoding="utf-8-sig")
        print(f"\nFilas obtenidas: {len(df)}")
        print(f"Guardado en: {out_csv}")
 
        # --- HISTÓRICOS (Adj Close mensual 1y) → 1 sola hoja con fechas como headers ---
        import yfinance as yf
        import numpy as np
 
        syms = df["symbol"].astype(str).str.upper().str.strip().tolist()
 
        def fetch_adj(symbols):
            all_adj = pd.DataFrame()
            for i in range(0, len(symbols), 20):
                batch = symbols[i:i+20]
                tries = 0
                while tries < 2:
                    try:
                        raw = yf.download(
                            tickers=batch,
                            period="1y",
                            interval="1mo",
                            auto_adjust=False,
                            group_by="column",
                            threads=True,
                            progress=False,
                        )
                        break
                    except Exception:
                        tries += 1
                        time.sleep(1.5)
                else:
                    continue
 
                if isinstance(raw.columns, pd.MultiIndex):
                    if "Adj Close" not in raw.columns.levels[0]:
                        continue
                    adj_b = raw["Adj Close"].copy()
                else:
                    if "Adj Close" not in raw.columns:
                        continue
                    adj_b = raw[["Adj Close"]].copy()
                    if len(batch) == 1:
                        adj_b.columns = [batch[0]]
 
                if isinstance(adj_b.columns, pd.MultiIndex):
                    adj_b.columns = adj_b.columns.get_level_values(-1)
                if getattr(adj_b.index, "tz", None) is not None:
                    adj_b.index = adj_b.index.tz_localize(None)
 
                adj_b.index = adj_b.index.to_period("M").to_timestamp("M")
                adj_b = adj_b.groupby(adj_b.index).last().sort_index().tail(12)
 
                all_adj = adj_b if all_adj.empty else all_adj.join(adj_b, how="outer")
 
            all_adj.index.name = "Date"
            return all_adj
 
        adj = fetch_adj(syms)
 
        # ===== Segundo Excel: matriz "ancha" por símbolo (1 hoja) =====
        hist_wide = adj.T.copy()
        hist_wide.index.name = "symbol"
        hist_wide = hist_wide.reset_index()
 
        # renombrar headers de fecha a 'YYYY-MM'
        hist_cols = []
        for c in hist_wide.columns:
            if isinstance(c, pd.Timestamp):
                hist_cols.append(c.strftime("%Y-%m"))
            else:
                hist_cols.append(c)
        hist_wide.columns = hist_cols
 
        # agregar nombre y ordenar columnas
        hist_wide = df[["symbol", "name"]].merge(hist_wide, on="symbol", how="left")
        order_cols = ["symbol", "name"] + [c for c in hist_wide.columns if c not in ("symbol", "name")]
 
        out_hist = OUTPUT_DIR / "adj_close_monthly_1y_wide.xlsx"
        with pd.ExcelWriter(out_hist) as w:
            hist_wide[order_cols].to_excel(w, index=False, sheet_name="HistWide")
 
        print(f"Excel históricos (1 hoja): {out_hist}")
 
        # ===== Tercer Excel: Cartera (criterio: mayor mean geom. y menor vol en primeros 6 meses) =====
        prices = adj.sort_index()
 
        if prices.shape[0] < 7:
            raise ValueError("No hay suficientes meses para calcular 6 retornos (se requieren al menos 7 puntos).")
 
        idx = prices.index
 
        # Primeros 6 meses: usar 7 puntos para obtener 6 retornos
        first6_full_idx = idx[:7]       # meses 0..6
        retn_first6 = prices.loc[first6_full_idx].pct_change().iloc[1:]  # 6 retornos mensuales
 
        # Media geométrica mensual (por símbolo)
        counts = retn_first6.count()
        mean_geom_first6m = (1.0 + retn_first6).prod() ** (1.0 / counts) - 1.0
 
        # Media aritmética mensual (por referencia)
        mean_arith_first6m = retn_first6.mean()
 
        # Volatilidad mensual
        vol_first6m = retn_first6.std(ddof=0)
 
        # Puntaje ajustado por volatilidad (Sharpe-like)
        vol_adj = vol_first6m.replace(0, np.nan)
        score = mean_geom_first6m / vol_adj
 
        crit = pd.DataFrame({
            "mean_geom_first6m": mean_geom_first6m,
            "mean_arith_first6m": mean_arith_first6m,
            "vol_first6m": vol_first6m,
            "score": score
        })
 
        # Añadir nombres y ordenar por score desc (mejor arriba)
        crit = crit.join(df.set_index("symbol")[["name"]], how="left")
        crit = crit.sort_values(["score", "mean_geom_first6m"], ascending=[False, False])
 
        # Selección final: top 10 por score
        selected = crit.head(10).copy()
        selected["weight"] = 0.10  # equiponderada
 
        # Últimos 6 meses: returns de meses 6..11 (necesitamos el punto anterior para el primer retorno)
        last6_full_idx = idx[5:]  # meses 5..11 (7 puntos) → 6 retornos
        retn_last6_full = prices.loc[last6_full_idx].pct_change().iloc[1:]  # 6 retornos
        retn_last6_sel = retn_last6_full[selected.index]  # filas=meses, columnas=símbolos
 
        # Retorno mensual de la cartera (promedio simple)
        port_m = retn_last6_sel.mean(axis=1, skipna=True).to_frame("portfolio_return")
 
        # Resumen de la cartera
        port_summary = pd.DataFrame({
            "metric": ["months", "mean_monthly_return", "std_monthly_return", "cumulative_return_6m"],
            "value": [
                retn_last6_sel.shape[0],
                port_m["portfolio_return"].mean(),
                port_m["portfolio_return"].std(ddof=0),
                (1.0 + port_m["portfolio_return"]).prod() - 1.0,
            ],
        })
 
        out_port = OUTPUT_DIR / "portfolio_last6m_ranked.xlsx"
        with pd.ExcelWriter(out_port) as w:
            selected.reset_index().rename(columns={"index": "symbol"}).to_excel(w, index=False, sheet_name="Selection")
            retn_last6_sel.to_excel(w, index=True,  sheet_name="StockReturns_Last6M")
            port_m.to_excel(w,         index=True,  sheet_name="Portfolio_Last6M")
            port_summary.to_excel(w,   index=False, sheet_name="Summary")
 
        print("Criterio: media geométrica mensual alta y volatilidad baja (primeros 6 meses).")
        print(f"Acciones elegidas ({selected.shape[0]}): {', '.join(selected.index)}")
        print(f"Excel de cartera: {out_port}")
 
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
    finally:
        d.quit()
 
if __name__ == "__main__":
    main()
 