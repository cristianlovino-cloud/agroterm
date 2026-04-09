#!/usr/bin/env python3
"""
AGROTERM SERVER v2.0
Fuentes: AFA Diario (granos) + agrodapice/Liniers (hacienda)
Sin Rofex, sin credenciales externas.
"""

import os
import json
import time
import threading
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from flask_cors import CORS

PORT = int(os.environ.get("PORT", 8765))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('AgroTerm')

lock = threading.Lock()

# Datos de referencia cargados desde el inicio
_now = lambda: datetime.now().strftime("%d/%m · Ref")
granos_data: Dict[str, Any] = {
    'SOJA/DISP':  {'last':445000,'open':445000,'high':448000,'low':442000,'change':0,'change_pct':0.0,'time':'Ref','history':[445000]},
    'MAIZ/DISP':  {'last':247000,'open':247000,'high':249000,'low':245000,'change':0,'change_pct':0.0,'time':'Ref','history':[247000]},
    'TRIGO/DISP': {'last':258000,'open':258000,'high':260000,'low':256000,'change':0,'change_pct':0.0,'time':'Ref','history':[258000]},
    'GIRAS/DISP': {'last':536000,'open':536000,'high':540000,'low':532000,'change':0,'change_pct':0.0,'time':'Ref','history':[536000]},
    'SORGO/DISP': {'last':195000,'open':195000,'high':197000,'low':193000,'change':0,'change_pct':0.0,'time':'Ref','history':[195000]},
}
hacienda_data: Dict[str, Any] = {
    'novillo_esp': {'price':4750,'low':4608,'high':4893,'change_today':0,'updated':'Ref'},
    'novillito':   {'price':5400,'low':5238,'high':5562,'change_today':0,'updated':'Ref'},
    'vaquillona':  {'price':5000,'low':4850,'high':5150,'change_today':0,'updated':'Ref'},
    'vaca':        {'price':3800,'low':3686,'high':3914,'change_today':0,'updated':'Ref'},
    'ternero_inv': {'price':6200,'low':6014,'high':6386,'change_today':0,'updated':'Ref'},
    'ternera':     {'price':5800,'low':5626,'high':5974,'change_today':0,'updated':'Ref'},
    'toro':        {'price':3200,'low':3104,'high':3296,'change_today':0,'updated':'Ref'},
}
afa_ok = False
liniers_ok = False

price_history: Dict[str, list] = {k: [] for k in ['SOJA/DISP','MAIZ/DISP','TRIGO/DISP','GIRAS/DISP','SORGO/DISP']}

GRAIN_MAP = {'soja':'SOJA/DISP','maiz':'MAIZ/DISP','maíz':'MAIZ/DISP','trigo':'TRIGO/DISP','girasol':'GIRAS/DISP','sorgo':'SORGO/DISP'}

def parse_price(text):
    text = str(text).replace('$','').replace(' ','').strip()
    m = re.search(r'(\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d{4,})', text)
    if m:
        try:
            val = float(m.group(1).replace('.','').replace(',','.'))
            if 10000 < val < 5000000:
                return val
        except: pass
    return None

def scrape_afa():
    global granos_data, afa_ok
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    now_str = datetime.now().strftime("%H:%M:%S")
    found = {}
    for url in ["https://www.afascl.coop/afadiario/mercados-en-linea"]:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            full_text = soup.get_text(separator=' ', strip=True)
            for crop, sym in GRAIN_MAP.items():
                if sym in found: continue
                pattern = rf'{crop}\s*[^$\d]{{0,80}}\$\s*([\d\.,]+)'
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m:
                    val = parse_price(m.group(1))
                    if val:
                        prev = granos_data.get(sym, {}).get('last')
                        chg = round(val - prev, 0) if prev else 0
                        chgp = round(chg/prev*100, 2) if prev else 0.0
                        hist = price_history.get(sym, [])[-6:] + [val]
                        price_history[sym] = hist
                        found[sym] = {'last':val,'open':prev or val,'high':max(val,prev or val),'low':min(val,prev or val),'change':chg,'change_pct':chgp,'time':now_str,'history':hist}
            if found:
                log.info(f"AFA OK: {list(found.keys())}")
                break
        except Exception as e:
            log.error(f"AFA error: {e}")
    if not found:
        log.warning("AFA sin datos, usando fallback")
        now_ref = datetime.now().strftime("%d/%m · Ref")
        for sym, val in [('SOJA/DISP',445000),('MAIZ/DISP',247000),('TRIGO/DISP',258000),('GIRAS/DISP',536000),('SORGO/DISP',195000)]:
            found[sym] = {'last':val,'open':val,'high':val,'low':val,'change':0,'change_pct':0.0,'time':now_ref,'history':[val]}
    with lock:
        granos_data.update(found)
        afa_ok = any('Ref' not in v.get('time','') for v in found.values())

def afa_loop():
    while True:
        scrape_afa()
        time.sleep(15 * 60)

CATEGORIA_MAP = {"novillo especial":"novillo_esp","novillo":"novillito","novillito":"novillito","vaquillona":"vaquillona","vaca":"vaca","ternero de invernada":"ternero_inv","ternero invernada":"ternero_inv","ternero":"ternero_inv","ternera":"ternera","toro":"toro"}

def scraping_loop():
    global hacienda_data, liniers_ok
    while True:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        result = {}
        for url in ["https://www.agrodapice.com/liniers-online","https://www.agrodapice.com/canuelas-online","https://www.mercadodeliniers.com.ar/precios-del-dia"]:
            try:
                res = requests.get(url, headers=headers, timeout=15)
                soup = BeautifulSoup(res.text, "html.parser")
                now_str = datetime.now().strftime("%d/%m %H:%M")
                for table in soup.find_all("table"):
                    for row in table.find_all("tr"):
                        cells = row.find_all(["td","th"])
                        if len(cells) < 2: continue
                        cat = cells[0].get_text(strip=True).lower()
                        for i in range(1, min(len(cells),6)):
                            price = parse_price(cells[i].get_text(strip=True))
                            if price and price > 500:
                                for key, internal in CATEGORIA_MAP.items():
                                    if key in cat and internal not in result:
                                        result[internal] = {"price":price,"low":round(price*0.97),"high":round(price*1.03),"change_today":0,"updated":now_str}
                                break
                if result:
                    log.info(f"Hacienda OK ({len(result)}) de {url}")
                    if len(result) >= 5: break
            except Exception as e:
                log.error(f"Hacienda error: {e}")
        if not result:
            now_ref = datetime.now().strftime("%d/%m · Ref")
            for k,v in {"novillo_esp":4750,"novillito":5400,"vaquillona":5000,"vaca":3800,"ternero_inv":6200,"ternera":5800,"toro":3200}.items():
                result[k] = {"price":v,"low":round(v*0.97),"high":round(v*1.03),"change_today":0,"updated":now_ref}
        with lock:
            hacienda_data = result
            liniers_ok = bool(result)
        log.info(f"Hacienda actualizada: {list(result.keys())}")
        time.sleep(3600)

app = Flask(__name__)
CORS(app)

@app.route("/status")
def status():
    return jsonify({"version":"2.0.0","rofex_connected":afa_ok,"afa_ok":afa_ok,"liniers_ok":liniers_ok,"granos_count":len(granos_data),"hacienda_count":len(hacienda_data),"timestamp":datetime.now().isoformat()})

@app.route("/granos")
def granos():
    with lock: data = dict(granos_data)
    return jsonify(data)

@app.route("/hacienda")
def hacienda():
    with lock: data = dict(hacienda_data)
    return jsonify(data)

@app.route("/dolar")
def dolar():
    try:
        res = requests.get("https://dolarapi.com/v1/dolares", timeout=8, headers={"User-Agent":"AgroTerm/2.0"})
        response = jsonify(res.json())
        response.headers.add('Access-Control-Allow-Origin','*')
        return response
    except Exception as e:
        return jsonify({"error":str(e)}), 500

@app.route("/cosecha")
def cosecha():
    return jsonify({"campana":"2024/25","fuente":"BCR/GEA","cultivos":{"soja":{"sembradas":3200000,"cosechadas":1600000,"proyectadas":3140000,"rinde":3.40,"produccion_mt":10.68,"var":4.2},"maiz":{"sembradas":1100000,"cosechadas":770000,"proyectadas":1080000,"rinde":7.80,"produccion_mt":8.42,"var":8.1},"trigo":{"sembradas":820000,"cosechadas":820000,"proyectadas":820000,"rinde":3.10,"produccion_mt":2.54,"var":2.8},"girasol":{"sembradas":480000,"cosechadas":240000,"proyectadas":472000,"rinde":2.20,"produccion_mt":1.04,"var":-1.5},"sorgo":{"sembradas":260000,"cosechadas":187000,"proyectadas":255000,"rinde":5.50,"produccion_mt":1.40,"var":6.5}}})

@app.route("/")
def index():
    return jsonify({"service":"AgroTerm API v2.0","status":"running"})

# Startup — nivel módulo para que gunicorn lo ejecute
log.info("AgroTerm v2.0 iniciando...")
threading.Thread(target=afa_loop, daemon=True).start()
log.info("Hilo AFA Diario iniciado")
threading.Thread(target=scraping_loop, daemon=True).start()
log.info("Hilo hacienda iniciado")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
