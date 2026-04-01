#!/usr/bin/env python3
"""
AGROTERM SERVER
═══════════════════════════════════════════════════════════════
Servidor local para el dashboard AgroTerm.
Expone una API REST en http://localhost:8765

Endpoints:
  GET /status     → estado de conexiones
  GET /granos     → precios granos (Matba-Rofex)
  GET /hacienda   → precios hacienda (Liniers scraping)
  GET /cosecha    → datos cosecha estáticos BCR (manual)

Fuentes:
  • Matba-Rofex vía pyRofex WebSocket → granos tiempo real
  • Mercado de Liniers scraping diario → hacienda vacuna

Instalación (una sola vez):
  pip install pyRofex requests beautifulsoup4 flask flask-cors

Credenciales Matba-Rofex:
  1. Ir a https://remarkets.primary.ventures y crear cuenta gratis
  2. Poner usuario y contraseña abajo en ROFEX_USER / ROFEX_PASS
  3. Para producción contactar mpi@primary.com.ar
═══════════════════════════════════════════════════════════════
"""

import json
import time
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from flask_cors import CORS

# pyRofex reemplazado por scraping AFA Diario
ROFEX_AVAILABLE = False
rofex_connected = False

# ══════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('AgroTerm')

# ══════════════════════════════════════════
#  ESTADO GLOBAL (thread-safe via lock)
# ══════════════════════════════════════════
lock = threading.Lock()

granos_data: Dict[str, Any] = {}
hacienda_data: Dict[str, Any] = {}
rofex_connected = False
liniers_ok = False

# Historia de precios para sparklines (últimas 7 lecturas)
price_history: Dict[str, list] = {sym: [] for sym in ROFEX_INSTRUMENTS}

# ══════════════════════════════════════════
#  MATBA-ROFEX — WEBSOCKET
# ══════════════════════════════════════════

def scrape_afa():
    """Scrapea AFA Diario para precios de granos del día."""
    global granos_data, rofex_connected
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        res = requests.get("https://www.afascl.coop/afadiario/mercados-en-linea", 
                          headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        
        now_str = datetime.now().strftime("%H:%M:%S")
        mapping = {
            'soja':    'SOJA/DISP',
            'maíz':    'MAIZ/DISP',
            'maiz':    'MAIZ/DISP',
            'trigo':   'TRIGO/DISP',
            'girasol': 'GIRAS/DISP',
            'sorgo':   'SORGO/DISP',
        }
        
        found = {}
        # Buscar cards o bloques con precio
        blocks = soup.find_all(['div', 'article', 'section'], 
                               class_=lambda c: c and any(x in str(c).lower() 
                               for x in ['mercado', 'precio', 'grano', 'card', 'cultivo']))
        
        for block in blocks:
            text = block.get_text(strip=True).lower()
            price_spans = block.find_all(['span', 'div', 'p', 'strong'], 
                                         string=lambda s: s and '$' in str(s))
            
            for crop, sym in mapping.items():
                if crop in text and sym not in found:
                    for span in price_spans:
                        raw = span.get_text(strip=True).replace('$','').replace('.','').replace(',','.').strip()
                        try:
                            val = float(raw)
                            if 50000 < val < 2000000:
                                prev = granos_data.get(sym, {}).get('last')
                                chg = round(val - prev, 0) if prev else 0
                                chgp = round(chg/prev*100, 2) if prev else 0.0
                                found[sym] = {
                                    'last': val,
                                    'open': prev or val,
                                    'high': max(val, prev or val),
                                    'low':  min(val, prev or val),
                                    'change': chg,
                                    'change_pct': chgp,
                                    'time': now_str,
                                    'history': granos_data.get(sym, {}).get('history', [])[-6:] + [val],
                                }
                                break
                        except:
                            pass
        
        if found:
            with lock:
                granos_data.update(found)
                rofex_connected = True
            log.info(f"AFA scraping OK: {list(found.keys())}")
        else:
            # Fallback: try to find any price numbers near grain names
            log.warning("AFA: no se encontraron precios con método principal, intentando fallback...")
            all_text = soup.get_text()
            import re
            for crop, sym in mapping.items():
                pattern = rf'{crop}[^\d]{{0,50}}\$\s*([\d\.]+)'
                match = re.search(pattern, all_text, re.IGNORECASE)
                if match:
                    try:
                        val = float(match.group(1).replace('.',''))
                        if 50000 < val < 2000000:
                            found[sym] = {'last': val, 'open': val, 'high': val, 
                                         'low': val, 'change': 0, 'change_pct': 0.0, 
                                         'time': now_str, 'history': [val]}
                    except:
                        pass
            if found:
                with lock:
                    granos_data.update(found)
                    rofex_connected = True
                log.info(f"AFA fallback OK: {list(found.keys())}")
            else:
                log.warning("AFA: no se pudieron scrapear precios")
                rofex_connected = False
                
    except Exception as e:
        log.error(f"AFA scraping error: {e}")
        rofex_connected = False


def afa_loop():
    """Scrapea AFA cada 15 minutos."""
    while True:
        scrape_afa()
        time.sleep(15 * 60)


def init_rofex():
    """Reemplazado por AFA scraping."""
    pass

def rofex_watchdog():
    """Reemplazado por AFA loop."""
    pass

def market_data_handler(m): pass
def error_handler(m): pass
def exception_handler(e): pass


def scrape_liniers() -> Dict[str, Any]:
    """
    Scrapea el Mercado de Liniers.
    Retorna dict con precios por categoría.
    Si falla, retorna últimos datos conocidos.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.8,en;q=0.5",
    }

    result = {}

    for url in LINIERS_URLS:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Buscar tablas o filas con precios
            # Liniers suele tener tabla con categoría y precio $/kg
            found = parse_liniers_html(soup)
            if found:
                log.info(f"Liniers scraping OK ({len(found)} categorías) de {url}")
                return found
            else:
                log.warning(f"Liniers: no se encontraron datos en {url}")

        except requests.RequestException as e:
            log.error(f"Liniers scraping error en {url}: {e}")
        except Exception as e:
            log.error(f"Liniers parse error: {e}")

    # Si falla, retornar datos de fallback con timestamp
    log.warning("Usando datos Liniers de fallback (no se pudo scrapear)")
    return get_liniers_fallback()


def parse_liniers_html(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Parsear el HTML de Liniers buscando precios por categoría.
    Retorna {} si no encuentra nada útil.
    """
    result = {}
    now_str = datetime.now().strftime("%d/%m %H:%M")

    # Estrategia 1: buscar tablas con columnas categoría/precio
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            cat_text = cells[0].get_text(strip=True).lower()
            # Buscar precio — puede estar en distintas columnas
            for i in range(1, min(len(cells), 5)):
                price_text = cells[i].get_text(strip=True)
                price = parse_price(price_text)
                if price and price > 100:
                    for key, internal in CATEGORIA_MAP.items():
                        if key in cat_text:
                            if internal not in result:
                                result[internal] = {
                                    "price":        price,
                                    "low":          price * 0.98,
                                    "high":         price * 1.02,
                                    "change_today": 0,
                                    "updated":      now_str,
                                }
                            break
                    break

    # Estrategia 2: buscar divs o spans con clases típicas de precio
    if not result:
        price_elements = soup.find_all(
            lambda tag: tag.name in ["div", "span", "td", "li"] and
            any(c in (tag.get("class") or []) for c in
                ["precio", "price", "valor", "cotizacion", "kg"])
        )
        for el in price_elements:
            text = el.get_text(strip=True)
            price = parse_price(text)
            if price and price > 100:
                # Buscar label cercano
                parent = el.find_parent()
                if parent:
                    full_text = parent.get_text(strip=True).lower()
                    for key, internal in CATEGORIA_MAP.items():
                        if key in full_text and internal not in result:
                            result[internal] = {
                                "price":        price,
                                "low":          price * 0.98,
                                "high":         price * 1.02,
                                "change_today": 0,
                                "updated":      now_str,
                            }
                            break

    return result


def parse_price(text: str) -> Optional[float]:
    """Extraer número de un string como '$2.980,50' o '2980'."""
    import re
    text = text.replace("$", "").replace(" ", "").strip()
    # Formato argentino: 2.980,50
    match = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+(?:,\d+)?)", text)
    if match:
        num_str = match.group(1).replace(".", "").replace(",", ".")
        try:
            val = float(num_str)
            # Filtrar valores absurdos
            if 100 < val < 100000:
                return val
        except ValueError:
            pass
    return None


def get_liniers_fallback() -> Dict[str, Any]:
    """Valores de referencia cuando no se puede scrapear."""
    now_str = datetime.now().strftime("%d/%m · Referencia")
    base = {
        "novillo_esp": 2980,
        "novillito":   2840,
        "vaquillona":  2680,
        "vaca":        2420,
        "ternero_inv": 3450,
        "ternera":     3280,
        "toro":        2180,
    }
    return {
        k: {
            "price":        v,
            "low":          round(v * 0.98),
            "high":         round(v * 1.02),
            "change_today": 0,
            "updated":      now_str,
        }
        for k, v in base.items()
    }


def scraping_loop():
    """Scrapea Liniers al inicio y luego cada hora."""
    global hacienda_data, liniers_ok
    while True:
        log.info("Scrapeando Liniers...")
        data = scrape_liniers()
        with lock:
            hacienda_data = data
            liniers_ok    = bool(data)
        if data:
            log.info(f"Hacienda actualizada: {list(data.keys())}")
        time.sleep(3600)  # 1 hora


# ══════════════════════════════════════════
#  FLASK API
# ══════════════════════════════════════════
app = Flask(__name__)
CORS(app)  # Permitir requests desde el HTML local (file://)

@app.route("/status")
def status():
    return jsonify({
        "version":         "1.0.0",
        "rofex_connected": rofex_connected,
        "afa_ok": rofex_connected,
        "liniers_ok":      liniers_ok,
        "granos_count":    len(granos_data),
        "hacienda_count":  len(hacienda_data),
        "timestamp":       datetime.now().isoformat(),
    })

@app.route("/granos")
def granos():
    with lock:
        data = dict(granos_data)
    # Also include instrument map so dashboard knows what to display
    data['_instruments'] = ROFEX_INSTRUMENTS
    return jsonify(data)

@app.route("/hacienda")
def hacienda():
    with lock:
        data = dict(hacienda_data)
    return jsonify(data)

@app.route("/dolar")
def dolar():
    """Proxy para dolarapi.com — evita problemas de CORS desde archivos locales."""
    try:
        res = requests.get(
            "https://dolarapi.com/v1/dolares",
            timeout=8,
            headers={"User-Agent": "AgroTerm/1.0"}
        )
        data = res.json()
        response = jsonify(data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    except Exception as e:
        log.error(f"Error fetching dolar: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/cosecha")
def cosecha():
    """Datos estáticos de cosecha — actualizar manualmente cada semana."""
    return jsonify({
        "campana": "2024/25",
        "fuente": "BCR / GEA",
        "cultivos": {
            "soja":    {"sembradas": 3200000, "cosechadas": 1600000, "proyectadas": 3140000, "rinde": 3.40, "produccion_mt": 10.68, "var": 4.2},
            "maiz":    {"sembradas": 1100000, "cosechadas":  770000, "proyectadas": 1080000, "rinde": 7.80, "produccion_mt":  8.42, "var": 8.1},
            "trigo":   {"sembradas":  820000, "cosechadas":  820000, "proyectadas":  820000, "rinde": 3.10, "produccion_mt":  2.54, "var": 2.8},
            "girasol": {"sembradas":  480000, "cosechadas":  240000, "proyectadas":  472000, "rinde": 2.20, "produccion_mt":  1.04, "var":-1.5},
            "sorgo":   {"sembradas":  260000, "cosechadas":  187000, "proyectadas":  255000, "rinde": 5.50, "produccion_mt":  1.40, "var": 6.5},
        }
    })

@app.route("/")
def index():
    return jsonify({"service": "AgroTerm API", "status": "running", "port": PORT})


# ══════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════
# ══════════════════════════════════════════
#  STARTUP — corre tanto con gunicorn como directo
#  gunicorn no ejecuta __main__, por eso los hilos
#  se inician aquí a nivel de módulo
# ══════════════════════════════════════════
log.info("AgroTerm Server v1.0 iniciando...")

# Iniciar scraping de Liniers en hilo separado
t_scraping = threading.Thread(target=scraping_loop, daemon=True)
t_scraping.start()
log.info("Hilo de scraping Liniers iniciado")

# Iniciar scraping AFA Diario (reemplaza Rofex)
log.info("Iniciando scraping AFA Diario para precios de granos...")
t_afa = threading.Thread(target=afa_loop, daemon=True)
t_afa.start()

if __name__ == "__main__":
    log.info(f"Servidor Flask iniciando en puerto {PORT}...")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
