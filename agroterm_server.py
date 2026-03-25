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

import os
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

# ── Intentar importar pyRofex (puede no estar instalado) ──
try:
    import pyRofex
    ROFEX_AVAILABLE = True
except ImportError:
    ROFEX_AVAILABLE = False
    print("⚠  pyRofex no instalado. Ejecutar: pip install pyRofex")

# ══════════════════════════════════════════
#  CONFIGURACIÓN — EDITAR AQUÍ
# ══════════════════════════════════════════

# Credenciales Matba-Rofex (reMarkets = demo gratuito)
ROFEX_USER   = os.environ.get("ROFEX_USER", "")
ROFEX_PASS   = os.environ.get("ROFEX_PASS", "")
ROFEX_ENV    = "reMarkets"       # "reMarkets" (demo) o "live"

# Puerto del servidor local
PORT = 8765

# Instrumentos a suscribir en Rofex
ROFEX_INSTRUMENTS = [
    "SOJA/MAY25",
    "MAIZ/JUL25",
    "TRIGO/DIC25",
    "GIRAS/NOV25",
    "SORGO/ABR25",
]

# Entradas de market data que queremos
def _get_entries():
    if not ROFEX_AVAILABLE:
        return []
    entries = []
    for name in ["LAST", "LA"]:
        if hasattr(pyRofex.MarketDataEntry, name):
            entries.append(getattr(pyRofex.MarketDataEntry, name)); break
    for name in ["OPEN", "OP", "OPENING_PRICE"]:
        if hasattr(pyRofex.MarketDataEntry, name):
            entries.append(getattr(pyRofex.MarketDataEntry, name)); break
    for name in ["HIGH", "HI", "SESSION_HIGH_PRICE"]:
        if hasattr(pyRofex.MarketDataEntry, name):
            entries.append(getattr(pyRofex.MarketDataEntry, name)); break
    for name in ["LOW", "LO", "SESSION_LOW_PRICE"]:
        if hasattr(pyRofex.MarketDataEntry, name):
            entries.append(getattr(pyRofex.MarketDataEntry, name)); break
    for name in ["CLOSING_PRICE", "CLOSE", "CL", "SETTLEMENT_PRICE"]:
        if hasattr(pyRofex.MarketDataEntry, name):
            entries.append(getattr(pyRofex.MarketDataEntry, name)); break
    return entries
ROFEX_ENTRIES = _get_entries()

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

def market_data_handler(message):
    """Callback cuando llega market data de Rofex."""
    global granos_data
    try:
        sym  = message.get("instrumentId", {}).get("symbol", "")
        md   = message.get("marketData", {})
        if not sym or not md:
            return

        last_list = md.get("LA", {})
        last_price = last_list.get("price") if isinstance(last_list, dict) else None

        with lock:
            prev = granos_data.get(sym, {}).get("last")
            entry = {
                "last":       last_price,
                "open":       md.get("OP", {}).get("price") if isinstance(md.get("OP"), dict) else None,
                "high":       md.get("HI", {}).get("price") if isinstance(md.get("HI"), dict) else None,
                "low":        md.get("LO", {}).get("price") if isinstance(md.get("LO"), dict) else None,
                "close":      md.get("CL", {}).get("price") if isinstance(md.get("CL"), dict) else None,
                "time":       datetime.now().strftime("%H:%M:%S"),
            }
            # Calcular variación
            ref = entry["close"] or entry["open"]
            if ref and last_price:
                entry["change"]     = round(last_price - ref, 0)
                entry["change_pct"] = round((last_price - ref) / ref * 100, 2)
            else:
                entry["change"]     = 0
                entry["change_pct"] = 0.0

            # Historia para sparkline
            if last_price and last_price != prev:
                price_history[sym].append(last_price)
                if len(price_history[sym]) > 7:
                    price_history[sym].pop(0)
            entry["history"] = price_history[sym][:]

            granos_data[sym] = entry

        log.info(f"Rofex tick: {sym} = ${last_price:,.0f}" if last_price else f"Rofex: {sym} sin precio")
    except Exception as e:
        log.error(f"market_data_handler error: {e}")


def error_handler(message):
    log.error(f"Rofex error: {message}")


def exception_handler(e):
    global rofex_connected
    log.error(f"Rofex exception: {e}")
    rofex_connected = False


def discover_instruments():
    """Descubrir instrumentos disponibles en Rofex."""
    global ROFEX_INSTRUMENTS
    try:
        all_instruments = pyRofex.get_all_instruments()
        available = [i['instrumentId']['symbol'] for i in all_instruments.get('instruments', [])]
        log.info(f"Instrumentos disponibles: {len(available)} total")

        found = []
        keywords = {
            'SOJA': ['SOJA', 'SOJ'],
            'MAIZ': ['MAIZ', 'MAI'],
            'TRIGO': ['TRIGO', 'TRI'],
            'GIRAS': ['GIRAS', 'GIR', 'GIRASOL'],
            'SORGO': ['SORGO', 'SOR'],
        }
        for crop, keys in keywords.items():
            matches = [s for s in available if any(k in s.upper() for k in keys)]
            if matches:
                # Pick the nearest expiration
                matches.sort()
                found.append(matches[0])
                log.info(f"  {crop}: usando {matches[0]} (de {len(matches)} opciones)")
            else:
                log.warning(f"  {crop}: no encontrado")

        if found:
            ROFEX_INSTRUMENTS = found
            log.info(f"Instrumentos seleccionados: {ROFEX_INSTRUMENTS}")
        else:
            # Fallback to common names
            ROFEX_INSTRUMENTS = ["SOJA/MAY25", "MAIZ/JUL25", "TRIGO/DIC25"]
            log.warning("Usando instrumentos por defecto")

    except Exception as e:
        log.error(f"Error descubriendo instrumentos: {e}")
        ROFEX_INSTRUMENTS = ["SOJA/MAY25", "MAIZ/JUL25", "TRIGO/DIC25"]


def init_rofex():
    """Conectar con Matba-Rofex WebSocket."""
    global rofex_connected
    if not ROFEX_AVAILABLE:
        log.warning("pyRofex no disponible, saltando conexión.")
        return

    if not ROFEX_USER or not ROFEX_PASS:
        log.warning("Credenciales Rofex no configuradas. Agregar ROFEX_USER y ROFEX_PASS en Render → Environment")
        return

    try:
        env = pyRofex.Environment.REMARKET if ROFEX_ENV == "reMarkets" else pyRofex.Environment.LIVE
        pyRofex.initialize(
            user=ROFEX_USER,
            password=ROFEX_PASS,
            account="",
            environment=env
        )
        log.info("Rofex inicializado ✓")

        # Discover available instruments
        discover_instruments()

        pyRofex.init_websocket_connection(
            market_data_handler=market_data_handler,
            error_handler=error_handler,
            exception_handler=exception_handler
        )
        log.info("Rofex WebSocket conectado ✓")

        if ROFEX_INSTRUMENTS:
            pyRofex.market_data_subscription(
                tickers=ROFEX_INSTRUMENTS,
                entries=ROFEX_ENTRIES
            )
            log.info(f"Suscripto a: {ROFEX_INSTRUMENTS}")
        rofex_connected = True

    except Exception as e:
        log.error(f"No se pudo conectar a Rofex: {e}")
        rofex_connected = False


def rofex_watchdog():
    """Re-intentar conexión si se cae."""
    global rofex_connected
    while True:
        time.sleep(30)
        if not rofex_connected and ROFEX_AVAILABLE and ROFEX_USER and ROFEX_PASS:
            log.warning("Rofex desconectado, reintentando...")
            init_rofex()


# ══════════════════════════════════════════
#  LINIERS — SCRAPING
# ══════════════════════════════════════════

# URLs a scrapear (en orden de prioridad)
LINIERS_URLS = [
    "https://www.mercadodeliniers.com.ar/precios-del-dia",
    "https://www.mercadodeliniers.com.ar/",
]

# Mapeo de categorías (texto en la web → clave interna)
CATEGORIA_MAP = {
    "novillo especial": "novillo_esp",
    "novillo": "novillito",
    "novillito": "novillito",
    "vaquillona": "vaquillona",
    "vaca": "vaca",
    "ternero de invernada": "ternero_inv",
    "ternero invernada": "ternero_inv",
    "ternera": "ternera",
    "toro": "toro",
    "ternero": "ternero_inv",
}

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

# Iniciar conexión Rofex
if ROFEX_AVAILABLE and ROFEX_USER and ROFEX_PASS:
    log.info(f"Iniciando conexión Rofex para usuario: {ROFEX_USER[:4]}***")
    t_rofex = threading.Thread(target=init_rofex, daemon=True)
    t_rofex.start()
    t_watchdog = threading.Thread(target=rofex_watchdog, daemon=True)
    t_watchdog.start()
else:
    if not ROFEX_USER or not ROFEX_PASS:
        log.warning("ROFEX_USER o ROFEX_PASS no configurados en variables de entorno")
    if not ROFEX_AVAILABLE:
        log.warning("pyRofex no disponible")

if __name__ == "__main__":
    log.info(f"Servidor Flask iniciando en puerto {PORT}...")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
