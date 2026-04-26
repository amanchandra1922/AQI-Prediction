"""
IOC AQI Forecasting — FastAPI Backend
Run: uvicorn app:app --reload --port 8000
"""

import os, warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# ── Local pipeline ────────────────────────────────────────────────────────────
import sys
sys.path.insert(0, os.path.dirname(__file__))
from data_pipeline import load_pipeline, ZONE_COLORS

# ── Load data once at startup ─────────────────────────────────────────────────
print("Loading data pipeline …")
sd, ZONES_CLEAN, city_daily, zone_daily_raw = load_pipeline(verbose=True)
print("Data pipeline ready.\n")

# ── AQI category helpers ──────────────────────────────────────────────────────
AQI_BANDS = [
    (0,   50,  "#55a868", "Good"),
    (51,  100, "#a8bb5a", "Satisfactory"),
    (101, 200, "#f5c518", "Moderate"),
    (201, 300, "#f28e2b", "Poor"),
    (301, 400, "#d95f02", "Very Poor"),
    (401, 500, "#b22222", "Severe"),
]

def aqi_category(val):
    if pd.isna(val): return "Unknown"
    for lo, hi, _, label in AQI_BANDS:
        if lo <= val <= hi:
            return label
    return "Severe"

def aqi_color(val):
    if pd.isna(val): return "#888888"
    for lo, hi, color, _ in AQI_BANDS:
        if lo <= val <= hi:
            return color
    return "#b22222"

def simple_forecast(series: pd.Series, horizon: int = 30):
    """Lightweight AR(3) forecast (no statsmodels dependency for API speed)."""
    s = series.dropna().values
    if len(s) < 10:
        return [float(np.nanmean(s))] * horizon
    # Use last 90-day rolling mean + slight trend
    w = min(90, len(s))
    baseline = float(np.mean(s[-w:]))
    trend    = float(np.mean(np.diff(s[-14:]))) if len(s) >= 15 else 0.0
    trend    = np.clip(trend, -5, 5)   # cap daily drift
    preds = []
    last  = float(s[-1])
    for i in range(horizon):
        nxt = 0.7 * last + 0.3 * baseline + trend
        nxt = float(np.clip(nxt, 0, 600))
        preds.append(round(nxt, 1))
        last = nxt
    return preds

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="IOC AQI Dashboard API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

@app.get("/")
def root():
    index = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    return {"message": "IOC AQI API — visit /docs"}

# ─────────────────────────────────────────────────────────────────────────────
# /api/overview  — summary stats
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/overview")
def overview():
    total_station_rows = len(sd)
    total_zone_rows    = sum(len(v) for v in ZONES_CLEAN.values())
    zone_summaries = []
    for z, df in ZONES_CLEAN.items():
        latest_aqi = float(df["AQI"].dropna().iloc[-1]) if not df["AQI"].dropna().empty else None
        zone_summaries.append({
            "zone":        z,
            "color":       ZONE_COLORS.get(z, "#888"),
            "days":        len(df),
            "date_from":   str(df.index.min().date()),
            "date_to":     str(df.index.max().date()),
            "mean_aqi":    round(float(df["AQI"].mean()), 1),
            "max_aqi":     int(df["AQI"].max()),
            "min_aqi":     int(df["AQI"].min()),
            "latest_aqi":  round(latest_aqi, 1) if latest_aqi else None,
            "category":    aqi_category(latest_aqi),
            "cat_color":   aqi_color(latest_aqi),
        })
    return {
        "dataset": {
            "raw_station_rows": total_station_rows,
            "zone_day_rows":    total_zone_rows,
            "date_from":        str(sd["Date"].min().date()),
            "date_to":          str(sd["Date"].max().date()),
            "unique_stations":  int(sd["StationId"].nunique()),
            "zones_count":      len(ZONES_CLEAN),
        },
        "zones": zone_summaries,
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/zones  — list of zone names
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zones")
def zones():
    return {"zones": sorted(ZONES_CLEAN.keys())}

# ─────────────────────────────────────────────────────────────────────────────
# /api/zone/{zone}/timeseries?days=365
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zone/{zone}/timeseries")
def timeseries(zone: str, days: int = 365):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df = ZONES_CLEAN[zone].tail(days)
    return {
        "zone":   zone,
        "color":  ZONE_COLORS.get(zone, "#888"),
        "dates":  [str(d.date()) for d in df.index],
        "aqi":    [round(v, 1) if not pd.isna(v) else None for v in df["AQI"]],
        "pm25":   [round(v, 1) if not pd.isna(v) else None for v in df.get("PM2.5", [])],
        "pm10":   [round(v, 1) if not pd.isna(v) else None for v in df.get("PM10",  [])],
        "no2":    [round(v, 1) if not pd.isna(v) else None for v in df.get("NO2",   [])],
        "so2":    [round(v, 1) if not pd.isna(v) else None for v in df.get("SO2",   [])],
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/zone/{zone}/forecast?horizon=30
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zone/{zone}/forecast")
def forecast(zone: str, horizon: int = 30):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df     = ZONES_CLEAN[zone]
    series = df["AQI"]
    preds  = simple_forecast(series, horizon)
    last_date = df.index[-1]
    fut_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=horizon, freq="D")
    return {
        "zone":      zone,
        "color":     ZONE_COLORS.get(zone, "#888"),
        "dates":     [str(d.date()) for d in fut_dates],
        "predicted": preds,
        "horizon":   horizon,
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/zone/{zone}/monthly  — monthly averages
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zone/{zone}/monthly")
def monthly(zone: str):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df = ZONES_CLEAN[zone]
    mon = df.groupby(df.index.month)["AQI"].mean().round(1)
    return {
        "zone":   zone,
        "months": list(mon.index.astype(int)),
        "aqi":    list(mon.values),
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/zone/{zone}/yearly  — yearly averages
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zone/{zone}/yearly")
def yearly(zone: str):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df  = ZONES_CLEAN[zone]
    yoy = df.groupby(df.index.year)["AQI"].mean().round(1)
    return {
        "zone":  zone,
        "years": list(yoy.index.astype(int)),
        "aqi":   list(yoy.values),
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/zone/{zone}/category_distribution
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/zone/{zone}/category_distribution")
def cat_dist(zone: str):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df = ZONES_CLEAN[zone]
    cats = df["AQI"].apply(aqi_category).value_counts(normalize=True) * 100
    order = ["Good","Satisfactory","Moderate","Poor","Very Poor","Severe"]
    colors= ["#55a868","#a8bb5a","#f5c518","#f28e2b","#d95f02","#b22222"]
    return {
        "zone":       zone,
        "categories": order,
        "percentages":[round(float(cats.get(c, 0)), 1) for c in order],
        "colors":     colors,
    }

# ─────────────────────────────────────────────────────────────────────────────
# /api/alerts  — current alert level per zone (based on latest AQI)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/alerts")
def alerts():
    ALERT_MAP = {
        "Good":       ("🟢", "Normal Operations",    "#2ecc71"),
        "Satisfactory":("🟢","Normal Operations",    "#2ecc71"),
        "Moderate":   ("🟡", "Monitor Emissions",    "#f1c40f"),
        "Poor":       ("🟠", "Reduce Emissions",     "#e67e22"),
        "Very Poor":  ("🔴", "Activate Controls",    "#e74c3c"),
        "Severe":     ("🚨", "Emergency Protocol",   "#8e44ad"),
        "Unknown":    ("⚪", "No Data",              "#95a5a6"),
    }
    result = []
    for z, df in ZONES_CLEAN.items():
        latest = df["AQI"].dropna().iloc[-1] if not df["AQI"].dropna().empty else None
        cat    = aqi_category(latest)
        emoji, action, color = ALERT_MAP.get(cat, ("⚪","No Data","#95a5a6"))
        result.append({
            "zone":       z,
            "aqi":        round(float(latest), 1) if latest else None,
            "category":   cat,
            "emoji":      emoji,
            "action":     action,
            "color":      color,
            "date":       str(df.index[-1].date()),
        })
    result.sort(key=lambda x: x["aqi"] or 0, reverse=True)
    return {"alerts": result}

# ─────────────────────────────────────────────────────────────────────────────
# /api/allzones/timeseries  — all zones AQI for comparison chart
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/allzones/timeseries")
def all_zones_ts(days: int = 365):
    result = {}
    for z, df in ZONES_CLEAN.items():
        sub = df["AQI"].tail(days)
        result[z] = {
            "dates": [str(d.date()) for d in sub.index],
            "aqi":   [round(v, 1) if not pd.isna(v) else None for v in sub],
            "color": ZONE_COLORS.get(z, "#888"),
        }
    return result

# ─────────────────────────────────────────────────────────────────────────────
# /api/pollutants/{zone}  — pollutant breakdown
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/pollutants/{zone}")
def pollutants(zone: str, days: int = 180):
    if zone not in ZONES_CLEAN:
        raise HTTPException(404, f"Zone '{zone}' not found.")
    df = ZONES_CLEAN[zone].tail(days)
    polls = {}
    for col in ["PM2.5","PM10","NO2","SO2","CO","O3"]:
        if col in df.columns:
            polls[col] = {
                "mean":   round(float(df[col].mean()), 2),
                "max":    round(float(df[col].max()), 2),
                "values": [round(v,1) if not pd.isna(v) else None for v in df[col]],
            }
    return {"zone": zone, "dates": [str(d.date()) for d in df.index], "pollutants": polls}
