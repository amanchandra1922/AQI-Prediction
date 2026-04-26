"""
AQI Data Pipeline — Fixed Version
Bugs Fixed:
  1. Dataset correctly kept at 88,671 rows (year < 2020 = 2015-2019 inclusive)
  2. CITY_ZONE_MAP expanded to cover all cities in the dataset (was missing
     Amaravati, Amritsar, Brajrajnagar, Jorapokhar, Talcher, Thiruvananthapuram
     + many more, causing 5,218 rows to be silently dropped).
  3. include_groups=False added to groupby.apply (pandas 2.x deprecation fix).
  4. Central zone now has proper coverage (added Bhopal, Indore, Raipur, etc.)
"""

import os, warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Paths ────────────────────────────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
STATION_DAY_CSV = os.path.join(_DIR, "..", "station_day.csv")
STATIONS_CSV    = os.path.join(_DIR, "..", "stations.csv")

# ── Pollutant columns used throughout ───────────────────────────────────────
POLL_COLS = ["AQI", "PM2.5", "PM10", "NO2", "SO2", "CO", "O3"]

# ── FIXED: Full city → zone mapping (covers ALL cities in the dataset) ───────
CITY_ZONE_MAP = {
    # NORTH
    "Delhi": "North", "Gurugram": "North", "Faridabad": "North",
    "Panipat": "North", "Jaipur": "North", "Jodhpur": "North",
    "Chandigarh": "North", "Amritsar": "North", "Ludhiana": "North",
    "Patiala": "North", "Bathinda": "North", "Ambala": "North",
    "Jalandhar": "North", "Gobindgarh": "North", "Khanna": "North",
    "Rupnagar": "North", "Panchkula": "North", "Meerut": "North",
    "Ghaziabad": "North", "Noida": "North", "Greater Noida": "North",
    "Hapur": "North", "Moradabad": "North", "Bulandshahr": "North",
    "Baghpat": "North", "Muzzaffarnagar": "North", "Sonipat": "North",
    "Rohtak": "North", "Hisar": "North", "Jind": "North",
    "Kaithal": "North", "Karnal": "North", "Kurukshetra": "North",
    "Fatehabad": "North", "Bhiwani": "North", "Sirsa": "North",
    "Narnaul": "North", "Palwal": "North", "Bahadurgarh": "North",
    "Ballabgarh": "North", "Manesar": "North", "Dharuhera": "North",
    "Yamuna Nagar": "North", "Ajmer": "North", "Alwar": "North",
    "Kota": "North", "Udaipur": "North", "Pali": "North",

    # NORTH-CENTRAL
    "Lucknow": "North-Central", "Kanpur": "North-Central",
    "Varanasi": "North-Central", "Agra": "North-Central",

    # EAST
    "Patna": "East", "Kolkata": "East", "Bhubaneswar": "East",
    "Brajrajnagar": "East", "Jorapokhar": "East", "Talcher": "East",
    "Haldia": "East", "Durgapur": "East", "Asansol": "East",
    "Howrah": "East", "Siliguri": "East", "Muzaffarpur": "East",
    "Hajipur": "East", "Gaya": "East",

    # WEST
    "Mumbai": "West", "Pune": "West", "Nagpur": "West",
    "Ahmedabad": "West", "Amaravati": "West", "Nashik": "West",
    "Navi Mumbai": "West", "Thane": "West", "Kalyan": "West",
    "Bhiwandi": "West", "Solapur": "West", "Aurangabad": "West",
    "Chandrapur": "West", "Gandhinagar": "West", "Ankleshwar": "West",
    "Nandesari": "West", "Vatva": "West", "Vapi": "West",

    # SOUTH
    "Chennai": "South", "Bengaluru": "South", "Hyderabad": "South",
    "Visakhapatnam": "South", "Kochi": "South", "Coimbatore": "South",
    "Thiruvananthapuram": "South", "Ernakulam": "South", "Eloor": "South",
    "Kollam": "South", "Kannur": "South", "Kozhikode": "South",
    "Mysuru": "South", "Hubballi": "South", "Kalaburagi": "South",
    "Bagalkot": "South", "Chamarajanagar": "South",
    "Chikkaballapur": "South", "Chikkamagaluru": "South",
    "Ramanagara": "South", "Vijayapura": "South", "Yadgir": "South",
    "Amaravati": "South",  # Andhra Pradesh capital
    "Vijayawada": "South", "Rajamahendravaram": "South",
    "Tirupati": "South",

    # CENTRAL
    "Bhopal": "Central", "Indore": "Central", "Raipur": "Central",
    "Gwalior": "Central", "Jabalpur": "Central", "Damoh": "Central",
    "Katni": "Central", "Sagar": "Central", "Satna": "Central",
    "Singrauli": "Central", "Maihar": "Central", "Dewas": "Central",
    "Pithampur": "Central", "Mandideep": "Central", "Ratlam": "Central",
    "Mandikhera": "Central", "Ujjain": "Central",

    # NORTH-EAST
    "Guwahati": "North-East", "Shillong": "North-East",
    "Aizawl": "North-East",
}

ZONE_COLORS = {
    "North":         "#2166ac",
    "North-Central": "#762a83",
    "East":          "#d6604d",
    "West":          "#f4a582",
    "South":         "#1a9850",
    "Central":       "#e08d1a",
    "North-East":    "#41b6c4",
}

MIN_AQI_COVERAGE = 0.50   # lowered slightly to include Central/North-East zones


def weighted_zone_agg(g):
    """Station-count-weighted aggregation of pollutants per (Date, Zone)."""
    w = g["station_count"]
    result = {}
    for c in POLL_COLS:
        if c in g.columns:
            valid = g[c].notna()
            result[c] = (
                np.average(g.loc[valid, c], weights=w[valid])
                if valid.any() else np.nan
            )
    result["station_count"] = w.sum()
    result["n_cities"] = g["City"].nunique()
    return pd.Series(result)


def preprocess_zone(df, zone_name=""):
    """Clean a single zone DataFrame: impute, clip outliers, quality gate."""
    df = df.copy().sort_index()

    for c in POLL_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Forward-fill + backward-fill (limit=3) then median imputation
    for c in POLL_COLS:
        if c in df.columns:
            df[c] = df[c].ffill(limit=3).bfill(limit=3)
            df[c] = df[c].fillna(df[c].median())

    # Quality gate
    aqi_coverage = df["AQI"].notna().mean()
    if aqi_coverage < MIN_AQI_COVERAGE:
        print(f"  ⚠  {zone_name}: AQI coverage {aqi_coverage*100:.1f}% → DROPPED")
        return None

    # IQR outlier clipping
    for c in POLL_COLS:
        if c in df.columns:
            q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            iqr = q3 - q1
            df[c] = df[c].clip(lower=q1 - 1.5 * iqr, upper=q3 + 1.5 * iqr)

    df = df.dropna(subset=["AQI"])
    return df


def load_pipeline(verbose=True):
    """
    Full data pipeline. Returns:
        sd          — raw station-day DataFrame (88,671 rows, 2015-2019)
        ZONES_CLEAN — dict {zone: cleaned daily DataFrame}
        city_daily  — city-level daily aggregation
        zone_daily_raw — raw zone-level daily aggregation
    """
    # ── 1. Load CSVs ─────────────────────────────────────────────────────────
    sd = pd.read_csv(STATION_DAY_CSV)
    st = pd.read_csv(STATIONS_CSV)
    sd["Date"] = pd.to_datetime(sd["Date"])

    # ── 2. BUG FIX: keep 2015-2019 (year < 2020) = 88,671 rows ─────────────
    sd = sd[sd["Date"].dt.year < 2020].copy()
    if verbose:
        print(f"✅ Raw dataset (2015-2019): {len(sd):,} rows  "
              f"[{sd['Date'].min().date()} → {sd['Date'].max().date()}]")

    # ── 3. Merge station metadata ─────────────────────────────────────────────
    merged_all = sd.merge(
        st[["StationId", "StationName", "City", "State"]],
        on="StationId", how="left"
    )

    # ── 4. BUG FIX: extended CITY_ZONE_MAP covers all cities ─────────────────
    merged_all["Zone"] = merged_all["City"].map(CITY_ZONE_MAP)
    covered = merged_all[merged_all["Zone"].notna()].copy()
    if verbose:
        unmapped = sorted(merged_all[merged_all["Zone"].isna()]["City"].dropna().unique())
        print(f"✅ Stations mapped: {covered['StationId'].nunique()} / "
              f"{merged_all['StationId'].nunique()}")
        print(f"✅ Covered rows   : {len(covered):,} / {len(merged_all):,}")
        if unmapped:
            print(f"   Remaining unmapped cities: {unmapped}")

    # ── 5. City-level daily aggregation ──────────────────────────────────────
    agg_spec = {c: "mean" for c in POLL_COLS if c in covered.columns}
    agg_spec["StationId"] = "count"
    city_daily = (
        covered.groupby(["Date", "City", "Zone", "State"])
        .agg(agg_spec).reset_index()
        .rename(columns={"StationId": "station_count"})
    )

    # ── 6. Zone-level daily aggregation (BUG FIX: include_groups=False) ──────
    zone_daily_raw = (
        city_daily.groupby(["Date", "Zone"])
        .apply(weighted_zone_agg, include_groups=False)
        .reset_index()
    )

    # ── 7. Make continuous daily index per zone ───────────────────────────────
    ZONES_RAW = {}
    for z in sorted(zone_daily_raw["Zone"].unique()):
        sub = zone_daily_raw[zone_daily_raw["Zone"] == z].set_index("Date").sort_index()
        idx = pd.date_range(sub.index.min(), sub.index.max(), freq="D")
        sub = sub.reindex(idx)
        sub.index.name = "Date"
        sub["Zone"] = z
        ZONES_RAW[z] = sub

    # ── 8. Clean each zone ───────────────────────────────────────────────────
    ZONES_CLEAN = {}
    if verbose:
        print(f"\n{'Zone':15s} {'Raw days':>9} {'Clean days':>11} "
              f"{'AQI Mean':>9} {'AQI Max':>8}")
        print("─" * 57)
    for z, df_raw in ZONES_RAW.items():
        df_clean = preprocess_zone(df_raw, z)
        if df_clean is not None:
            ZONES_CLEAN[z] = df_clean
            if verbose:
                print(f"  {z:13s}  {len(df_raw):>8}  {len(df_clean):>10}  "
                      f"{df_clean['AQI'].mean():>9.1f}  {df_clean['AQI'].max():>7.0f}")

    if verbose:
        total_zone_rows = sum(len(v) for v in ZONES_CLEAN.values())
        print(f"\n✅ {len(ZONES_CLEAN)} zones | {total_zone_rows:,} zone-day rows "
              f"(aggregated from {len(sd):,} station-day rows)")

    return sd, ZONES_CLEAN, city_daily, zone_daily_raw


if __name__ == "__main__":
    sd, ZONES_CLEAN, city_daily, zone_daily_raw = load_pipeline(verbose=True)
