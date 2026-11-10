"""Build notebooks/01_source_profiling.ipynb from the cells below (run, then execute with nbconvert)."""
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[2]
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip()))  # noqa: E731
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip()))  # noqa: E731

md("""
# Workshop 1 — Source Profiling (Team 5: Extreme Weather Monitoring, Texas)

This notebook profiles the **real, unmodified samples** downloaded by
`workshop_1/scripts/fetch_samples.py` from the four project sources:

| # | Source | Role | Sample |
|---|--------|------|--------|
| H1 | NOAA Storm Events Database (yearly CSV.gz) | historical | all Texas rows of 2025 |
| H2 | GHCN-Daily via NCEI Access Data Service (JSON) | historical | 10 Texas first-order stations, 2025-01-01 → today |
| I1 | NWS API `/alerts` (GeoJSON / CAP) | incremental | Texas alerts, last ~7 days + active, two polls |
| I2 | NWS API `/stations/{id}/observations` (GeoJSON) | incremental | 10 stations, two polls |

For each source we report: volume, null percentage per field, value ranges, duplicated identifiers,
timestamp format and time zone, coordinate validity, and unit consistency. Then we list concrete
data-quality problems, cross-source overlap candidates, and the empirical update behaviour.
""")

code("""
import json, re, sys, glob
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yaml

ROOT = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
sys.path.insert(0, str(ROOT))
from src.sources.raw_readers import read_storm_events, read_ghcn_daily_json, read_nws_folder
from src.normalization import canonical as C

SAMPLES = ROOT / "workshop_1" / "samples"
FIG = ROOT / "docs" / "figures"; FIG.mkdir(parents=True, exist_ok=True)
CFG = yaml.safe_load((ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"))
STATIONS = pd.DataFrame(CFG["region"]["stations"])
BBOX = CFG["region"]["bbox"]
pd.set_option("display.max_columns", 40); pd.set_option("display.width", 200)

def latest(pattern, folder=SAMPLES):
    return sorted(p for p in Path(folder).glob(pattern) if not p.name.endswith(".headers.json"))[-1]

def profile(df, name):
    \"\"\"Null % and distinct values per field.\"\"\"
    prof = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "null_pct": (df.isna().mean() * 100).round(1),
        "n_distinct": df.astype(str).nunique(),
        "example": df.apply(lambda s: s.dropna().astype(str).iloc[0][:40] if s.notna().any() else None),
    })
    print(f"{name}: {len(df):,} records x {df.shape[1]} fields")
    return prof

def in_bbox(lat, lon, pad=0.5):
    return lat.between(BBOX["min_lat"] - pad, BBOX["max_lat"] + pad) & lon.between(BBOX["min_lon"] - pad, BBOX["max_lon"] + pad)
STATIONS
""")

# ------------------------------------------------------------------ H1
md("""
## 1. H1 — NOAA Storm Events Database (bulk CSV)

File: `StormEvents_details-ftp_v1.0_d2025_c<compilation>.csv.gz` (full file kept in `data/raw/`,
Texas rows copied byte-for-byte into `workshop_1/samples/`).
""")
code("""
se_path = latest("noaa_storm_events_TX_*.csv")
se = read_storm_events(se_path)
prof_se = profile(se, "Storm Events TX 2025")
prof_se.sort_values("null_pct", ascending=False)
""")
code("""
print("Duplicated EVENT_ID:", se.EVENT_ID.duplicated().sum())
print("Events per episode (mean):", round(se.groupby("EPISODE_ID").size().mean(), 2))
print("Time zones:", se.CZ_TIMEZONE.value_counts().to_dict())
print("CZ_TYPE (C=county, Z=forecast zone, M=marine):", se.CZ_TYPE.value_counts().to_dict())
print("BEGIN_DATE_TIME format example:", se.BEGIN_DATE_TIME.iloc[0], "(2-digit year, local time)")
lat, lon = pd.to_numeric(se.BEGIN_LAT), pd.to_numeric(se.BEGIN_LON)
print(f"Rows without coordinates: {lat.isna().mean():.1%};  outside TX bbox: {(~in_bbox(lat, lon) & lat.notna()).sum()}")
print("Coordinates missing by CZ_TYPE:", lat.isna().groupby(se.CZ_TYPE).mean().round(3).to_dict())
mag = pd.to_numeric(se.MAGNITUDE)
print("\\nMAGNITUDE by event type (units differ!):")
display(se.assign(MAG=mag).groupby("EVENT_TYPE").agg(n=("EVENT_ID", "size"), mag_non_null=("MAG", "count"),
        mag_min=("MAG", "min"), mag_max=("MAG", "max"),
        mag_types=("MAGNITUDE_TYPE", lambda s: ",".join(sorted(s.dropna().unique())))).sort_values("n", ascending=False).head(18))
""")
code("""
dmg = se.DAMAGE_PROPERTY.map(C.parse_damage)
print("DAMAGE_PROPERTY raw examples:", se.DAMAGE_PROPERTY.dropna().drop_duplicates().head(8).tolist())
print(f"Parsed USD: null {dmg.isna().mean():.1%}, zero {(dmg == 0).mean():.1%}, max {dmg.max():,.0f}")
fig, ax = plt.subplots(figsize=(9, 4))
se.EVENT_TYPE.value_counts().head(15).sort_values().plot.barh(ax=ax, color="#4472c4")
ax.set_title("Storm Events — Texas 2025, records per event type"); ax.set_xlabel("records")
plt.tight_layout(); plt.savefig(FIG / "storm_events_types.png", dpi=150); plt.show()
""")

# ------------------------------------------------------------------ H2
md("""
## 2. H2 — GHCN-Daily through the NCEI Access Data Service (REST/JSON)

One JSON object per station-day. Each element (TMAX, PRCP…) has a companion `<ELEMENT>_ATTRIBUTES`
string `"measurement_flag,quality_flag,source_flag,obs_time"`. `units=metric` returns °C, mm and m/s.
""")
code("""
gh_path = latest("ghcn_daily_2*.json")
gh_raw = read_ghcn_daily_json(gh_path)
prof_gh = profile(gh_raw, "GHCN-Daily (10 TX stations)")
prof_gh.sort_values("null_pct", ascending=False)
""")
code("""
ELEMS = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "WSF2", "WSF5"]
gh = gh_raw.copy()
for e in ELEMS: gh[e] = pd.to_numeric(gh[e], errors="coerce")
gh["DATE"] = pd.to_datetime(gh.DATE)
print("Types as received:", {k: type(v).__name__ for k, v in json.load(open(gh_path))[0].items() if k in ("TMAX", "LATITUDE")})
print("Duplicated (STATION, DATE):", gh.duplicated(["STATION", "DATE"]).sum())
print("Date range:", gh.DATE.min().date(), "->", gh.DATE.max().date())
expected = (gh.DATE.max() - gh.DATE.min()).days + 1
cov = gh.groupby("STATION").DATE.nunique().rename("days").to_frame()
cov["completeness_pct"] = (cov.days / expected * 100).round(1)
display(cov)
display(gh[ELEMS].describe().T.round(1))
for e in ["TMAX", "PRCP", "WSF5"]:
    flags = gh[f"{e}_ATTRIBUTES"].dropna().str.split(",", expand=True)
    print(f"{e}: quality flags {flags[1].replace('', '(blank)').value_counts().to_dict()}  source flags {flags[2].value_counts().to_dict()}")
print("TMIN > TMAX rows:", (gh.TMIN > gh.TMAX).sum())
print("Physically suspicious WSF5 > 60 m/s:"); display(gh.loc[gh.WSF5 > 60, ["STATION", "NAME", "DATE", "WSF5", "WSF5_ATTRIBUTES", "WSF2", "AWND"]])
""")
code("""
fig, ax = plt.subplots(figsize=(10, 4))
for st, g in gh.groupby("STATION"):
    ax.plot(g.DATE, g.TMAX, lw=0.7, label=STATIONS.set_index("ghcn_id").loc[st, "nws_id"])
ax.set_title("GHCN-Daily TMAX (°C) — 10 Texas first-order stations"); ax.legend(ncol=5, fontsize=7)
plt.tight_layout(); plt.savefig(FIG / "ghcn_tmax.png", dpi=150); plt.show()
""")

# ------------------------------------------------------------------ I1
md("""
## 3. I1 — NWS Alerts API (GeoJSON, CAP semantics)
""")
code("""
POLL1 = SAMPLES / "poll1"
al = read_nws_folder(POLL1, "nws_alerts_recent_*.geojson")
prof_al = profile(al.drop(columns=["geometry"]), "NWS alerts TX (recent, poll 1)")
prof_al.sort_values("null_pct", ascending=False).head(25)
""")
code("""
fc = json.load(open(latest("nws_alerts_recent_*.geojson", POLL1), encoding="utf-8"))
print("Pagination object present:", fc.get("pagination"))
print("Duplicated id:", al.id.duplicated().sum())
print("Timestamp examples (local offset, not UTC):", al.sent.iloc[0], "|", al.onset.iloc[0])
print("UTC offsets used in 'sent':", al.sent.str[-6:].value_counts().to_dict())
print(f"Alerts without geometry (zone-based only): {al.geometry.isna().mean():.1%}")
print("messageType:", al.messageType.value_counts().to_dict(), " status:", al.status.value_counts().to_dict())
print("severity:", al.severity.value_counts().to_dict())
print("Alerts with references to earlier messages:", (al.references.map(len) > 0).sum())
display(al.event.value_counts().to_frame("alerts"))
""")

# ------------------------------------------------------------------ I2
md("""
## 4. I2 — NWS station observations (GeoJSON, WMO unit codes)
""")
code("""
ob = read_nws_folder(POLL1, "nws_observations_*.geojson")
value_cols = [c for c in ob.columns if c.endswith(".value")]
prof_ob = profile(ob[["stationId", "timestamp", "textDescription"] + value_cols], "NWS observations (poll 1)")
prof_ob.sort_values("null_pct", ascending=False)
""")
code("""
ob["ts"] = pd.to_datetime(ob.timestamp, utc=True)
span = ob.groupby("stationId").agg(records=("ts", "size"), first=("ts", "min"), last=("ts", "max"))
span["hours_covered"] = ((span["last"] - span["first"]).dt.total_seconds() / 3600).round(1)
display(span)
print("Requested window: 7 days = 168 h. The endpoint silently caps the response at 500 features.")
print("Duplicated (station, timestamp):", ob.duplicated(["stationId", "timestamp"]).sum())
print("Unit codes:", {c: ob[c].dropna().unique().tolist() for c in ob.columns if c.endswith(".unitCode") and c.split(".")[0] in C.NWS_VARIABLES})
print("qualityControl codes (temperature):", ob["temperature.qualityControl"].value_counts().to_dict())
minutes = ob.ts.dt.minute.value_counts().head(6).to_dict(); print("Minute-of-hour distribution (routine :51/:53 vs specials):", minutes)
lon = ob.geometry.map(lambda g: g["coordinates"][0]); lat = ob.geometry.map(lambda g: g["coordinates"][1])
print("Coordinates outside TX bbox:", (~in_bbox(lat, lon)).sum(), "| coordinate decimals:", lat.astype(str).str.split(".").str[1].str.len().max())
print("Keys missing in some features:", sorted(set(ob.columns) - set(ob.dropna(axis=1, how='any').columns))[:12], "...")
""")

# ------------------------------------------------------------------ canonical
md("""
## 5. Canonical mapping applied to every sample

The draft rules (`workshop_1/mapping.md`, implemented in `src/normalization/canonical.py`) are applied
to the four samples to check that every source can fill the canonical `event` fields.
""")
code("""
canon = pd.concat([
    C.map_storm_events(se),
    C.map_ghcn_daily(gh_raw),
    C.map_nws_alerts(al),
    C.map_nws_observations(ob.drop(columns=["ts"])),
], ignore_index=True)
summary = canon.groupby("source").agg(
    events=("event_id", "size"), dup_event_id=("event_id", lambda s: s.duplicated().sum()),
    ts_min=("timestamp", "min"), ts_max=("timestamp", "max"),
    lat_null_pct=("latitude", lambda s: round(s.isna().mean() * 100, 1)),
    severity_null_pct=("severity", lambda s: round(s.isna().mean() * 100, 1)),
    units=("severity_unit", lambda s: "; ".join(sorted(s.unique()))[:90]))
summary
""")
code("""
canon.sample(8, random_state=3)
""")

# ------------------------------------------------------------------ DQ
md("""
## 6. Data-quality problems found (with example records)
""")
code("""
issues = []
ex = se[se.BEGIN_LAT.isna()].iloc[0]
issues.append(("H1 Storm Events", "No coordinates for zone/county-based events",
               f"{pd.to_numeric(se.BEGIN_LAT).isna().mean():.1%} of rows", f"EVENT_ID {ex.EVENT_ID}: {ex.EVENT_TYPE}, CZ_TYPE={ex.CZ_TYPE}, CZ_NAME={ex.CZ_NAME}, BEGIN_LAT=NaN"))
ex = se.iloc[0]
issues.append(("H1 Storm Events", "Local standard time + 2-digit year in BEGIN_DATE_TIME",
               "all rows", f"EVENT_ID {ex.EVENT_ID}: BEGIN_DATE_TIME='{ex.BEGIN_DATE_TIME}', CZ_TIMEZONE='{ex.CZ_TIMEZONE}'"))
ex = se[se.DAMAGE_PROPERTY.fillna('').str.contains('[KM]', regex=True) & (se.DAMAGE_PROPERTY != '0.00K')].iloc[0]
issues.append(("H1 Storm Events", "Damage stored as text with K/M/B suffix; blanks mean 'unknown', not 0",
               f"{se.DAMAGE_PROPERTY.isna().mean():.1%} blank", f"EVENT_ID {ex.EVENT_ID}: DAMAGE_PROPERTY='{ex.DAMAGE_PROPERTY}'"))
h = se[se.EVENT_TYPE == 'Hail'].iloc[0]; w = se[se.EVENT_TYPE == 'Thunderstorm Wind'].dropna(subset=['MAGNITUDE']).iloc[0]
issues.append(("H1 Storm Events", "MAGNITUDE has different units per event type (hail inches, wind knots; EG=estimated vs MG=measured)",
               "Hail/Wind rows", f"EVENT_ID {h.EVENT_ID} Hail MAGNITUDE={h.MAGNITUDE} (in); EVENT_ID {w.EVENT_ID} TSTM Wind MAGNITUDE={w.MAGNITUDE} {w.MAGNITUDE_TYPE} (kt)"))
z = se[(se.CZ_TYPE == 'Z')].iloc[0]
issues.append(("H1 Storm Events", "CZ_FIPS of forecast zones (Z) is a zone number, not a county FIPS; names vary ('INLAND HARRIS')",
               f"{(se.CZ_TYPE == 'Z').mean():.1%} of rows", f"EVENT_ID {z.EVENT_ID}: CZ_TYPE=Z, CZ_FIPS={z.CZ_FIPS}, CZ_NAME={z.CZ_NAME}"))
raw_gz = sorted((ROOT / "data" / "raw").glob("noaa_storm_events_*.csv.gz"))
if raw_gz:  # full US file (git-ignored); re-download with fetch_samples.py historical
    full = read_storm_events(raw_gz[-1])
    text_hit = full[(full == "TEXAS").any(axis=1)]
    wrong = text_hit[text_hit.STATE_FIPS != "48"]
    w = wrong.iloc[0]
    issues.append(("H1 Storm Events", "Filtering by the text 'TEXAS' also matches Texas County (MO, OK) and other states' fields; filter must use STATE_FIPS=48",
                   f"{len(wrong)} wrong rows ({', '.join(f'{k}: {v}' for k, v in wrong.STATE.value_counts().items())})",
                   f"EVENT_ID {w.EVENT_ID}: STATE={w.STATE}, STATE_FIPS={w.STATE_FIPS}, CZ_NAME={w.CZ_NAME} (Texas County)"))
o = gh.loc[gh.WSF5.idxmax()]
issues.append(("H2 GHCN-Daily", "Implausible wind gust value not flagged by QC",
               "1+ rows", f"{o.STATION} {o.DATE.date()}: WSF5={o.WSF5} m/s (~{o.WSF5/C.KNOT_TO_MS:.0f} kt), attributes='{o.WSF5_ATTRIBUTES}'"))
miss = gh_raw[gh_raw.WSF5.isna()].iloc[0]
issues.append(("H2 GHCN-Daily", "Missing elements are absent keys (not null); numbers arrive as strings",
               f"WSF5 absent in {gh_raw.WSF5.isna().mean():.1%}", f"{miss.STATION} {miss.DATE}: keys without WSF5; TMAX='{miss.TMAX}' (str)"))
issues.append(("H2 GHCN-Daily", "Latency: most recent days not yet published",
               f"last DATE {gh.DATE.max().date()}", f"query endDate = download date; last available = {gh.DATE.max().date()}"))
a = al[al.geometry.isna()].iloc[0]
issues.append(("I1 NWS alerts", "Alerts without polygon geometry (only UGC zone codes)",
               f"{al.geometry.isna().mean():.1%}", f"{a.id[-20:]}: event={a.event}, geometry=None, UGC={a.geocode['UGC'][:3]}"))
issues.append(("I1 NWS alerts", "Timestamps carry local offsets (-05:00/-06:00/-07:00), not UTC",
               "all rows", f"sent='{al.sent.iloc[0]}'"))
c = span.hours_covered.min()
issues.append(("I2 NWS observations", "Silent 500-feature cap truncates the requested 7-day window",
               f"every station (min {c} h of 168 h)", f"KDFW: {span.loc['KDFW','records']} records from {span.loc['KDFW','first']} to {span.loc['KDFW','last']}"))
g0 = ob[ob['windGust.value'].isna()].iloc[0]
issues.append(("I2 NWS observations", "Nulls encoded as value=None with qualityControl='Z'; some keys absent",
               f"windGust null {ob['windGust.value'].isna().mean():.0%}", f"{g0.stationId} {g0.timestamp}: windGust={{'value': None, 'qualityControl': '{g0['windGust.qualityControl']}'}}"))
dq = pd.DataFrame(issues, columns=["source", "problem", "extent", "example record"])
dq.to_csv(ROOT / "workshop_1" / "data_quality_issues.csv", index=False)
dq
""")

# ------------------------------------------------------------------ overlap
md("""
## 7. Cross-source overlap — candidate pairs describing the same real-world event

Rule used for the candidates (to be refined in Workshop 3): Storm Events record in the **county
of a station** (exact `CZ_FIPS` for county rows, word-boundary name match for zone rows) and on
the **same local calendar day** as the GHCN-Daily record, with a compatible variable
(hail/wind ↔ WSF5, flood/rain ↔ PRCP, heat ↔ TMAX, cold ↔ TMIN, winter ↔ SNOW).
""")
code("""
STATION_COUNTY = {"USW00003927": ("TARRANT", "439"), "USW00012960": ("HARRIS", "201"), "USW00013904": ("TRAVIS", "453"),
    "USW00012921": ("BEXAR", "029"), "USW00023044": ("EL PASO", "141"), "USW00023042": ("LUBBOCK", "303"),
    "USW00023047": ("POTTER", "375"), "USW00012924": ("NUECES", "355"), "USW00023023": ("MIDLAND", "329"),
    "USW00012919": ("CAMERON", "061")}
VAR = {"Thunderstorm Wind": "WSF5", "High Wind": "WSF5", "Hail": "WSF5", "Flash Flood": "PRCP", "Heavy Rain": "PRCP",
       "Heat": "TMAX", "Excessive Heat": "TMAX", "Extreme Cold/Wind Chill": "TMIN", "Cold/Wind Chill": "TMIN",
       "Winter Storm": "SNOW", "Frost/Freeze": "TMIN"}
se2 = se.copy(); se2["local_date"] = pd.to_datetime(se2.BEGIN_YEARMONTH + se2.BEGIN_DAY.str.zfill(2))
pairs = []
for st, (name, fips) in STATION_COUNTY.items():
    m_c = (se2.CZ_TYPE == "C") & (se2.CZ_FIPS.str.zfill(3) == fips)
    m_z = (se2.CZ_TYPE == "Z") & se2.CZ_NAME.str.contains(rf"\\b{name}\\b", regex=True)
    cand = se2[(m_c | m_z) & se2.EVENT_TYPE.isin(VAR)].merge(gh[gh.STATION == st], left_on="local_date", right_on="DATE")
    pairs.append(cand)
pairs = pd.concat(pairs)
pairs["ghcn_var"] = pairs.EVENT_TYPE.map(VAR)
pairs["ghcn_value"] = [r[v] for r, v in zip(pairs.to_dict("records"), pairs.ghcn_var)]
pairs["se_value_SI"] = [C.storm_event_severity(r)[0] if r["EVENT_TYPE"] in ("Hail", "Thunderstorm Wind", "High Wind") else np.nan
                        for _, r in pairs.iterrows()]
cols = ["EVENT_ID", "EVENT_TYPE", "CZ_TYPE", "CZ_NAME", "BEGIN_DATE_TIME", "CZ_TIMEZONE", "MAGNITUDE", "MAGNITUDE_TYPE", "se_value_SI",
        "STATION", "DATE", "ghcn_var", "ghcn_value"]
print("Candidate pairs found:", len(pairs))
LOW = {"Extreme Cold/Wind Chill", "Cold/Wind Chill", "Frost/Freeze"}  # rank cold events by the lowest TMIN
pairs["rank_val"] = [-v if et in LOW else v for v, et in zip(pairs.ghcn_value, pairs.EVENT_TYPE)]
best = (pairs.sort_values("rank_val", ascending=False).groupby("EVENT_TYPE").head(1))
best[cols].reset_index(drop=True)
""")
md("""
**Second overlap: NWS observations ↔ GHCN-Daily.** First-order stations are published by both
services (same ASOS sensor). The daily maximum of the NWS hourly temperatures should match GHCN
`TMAX` for the overlapping days, but only after converting UTC timestamps to the station's
local standard day (UTC-6 / UTC-7), which is the GHCN-Daily day definition at these stations.
""")
code("""
o2 = ob.merge(STATIONS, left_on="stationId", right_on="nws_id")
o2["lst_offset"] = o2.ghcn_id.map(C.STATION_LST).fillna(-6)
o2["local_day"] = (o2.ts + pd.to_timedelta(o2.lst_offset, unit="h")).dt.tz_localize(None).dt.normalize()
o2["utc_day"] = o2.ts.dt.tz_localize(None).dt.normalize()
o2["local_hour"] = (o2.ts + pd.to_timedelta(o2.lst_offset, unit="h")).dt.hour
agg_lst = o2.groupby(["ghcn_id", "nws_id", "local_day"]).agg(
    max=("temperature.value", "max"), size=("ts", "size"), first_hour=("local_hour", "min"), last_hour=("local_hour", "max")).reset_index()
cmp = agg_lst.merge(gh[["STATION", "DATE", "TMAX"]], left_on=["ghcn_id", "local_day"], right_on=["STATION", "DATE"])
partial = cmp[(cmp.first_hour > 0) | (cmp.last_hour < 23)]
print(f"Partial days excluded (500-record cap starts mid-day): {len(partial)}")
cmp = cmp[(cmp.first_hour == 0) & (cmp.last_hour == 23)]  # complete local days only
agg_utc = o2.groupby(["ghcn_id", "utc_day"])["temperature.value"].max().rename("max_utc_day").reset_index()
cmp = cmp.merge(agg_utc, left_on=["ghcn_id", "local_day"], right_on=["ghcn_id", "utc_day"], how="left")
cmp["diff_lst"] = (cmp["max"] - cmp.TMAX).round(1); cmp["diff_utc_day"] = (cmp.max_utc_day - cmp.TMAX).round(1)
print(f"Station-days compared: {len(cmp)};  mean |diff| local-standard-day = {cmp.diff_lst.abs().mean():.2f} °C,  using UTC day = {cmp.diff_utc_day.abs().mean():.2f} °C")
cmp[["nws_id", "local_day", "size", "max", "TMAX", "diff_lst", "max_utc_day", "diff_utc_day"]].head(12)
""")
md("""
**Third overlap: NWS alerts ↔ NWS alerts (same event, several messages).** An `Update` or `Cancel`
message references the original alert id(s) in `properties.references`; these chains are the
event-identification problem inside the alert source itself.
""")
code("""
ids = set(al.id)
chains = al[al.references.map(len) > 0].copy()
chains["refs_in_sample"] = chains.references.map(lambda refs: sum(r["identifier"] in ids for r in refs))
chains[["id", "event", "messageType", "sent", "refs_in_sample"]].assign(id=lambda d: d.id.str[-26:]).head(8)
""")

# ------------------------------------------------------------------ polls
md("""
## 8. Incremental behaviour — two polls at least one hour apart

`fetch_samples.py incremental pollN` was executed twice (poll 1 and poll 2). We compare identifiers,
content hashes and HTTP caching headers to learn how new and changed records can be detected.
""")
code("""
POLL2 = SAMPLES / "poll2"
if not POLL2.exists():
    print("poll2 not available yet")
else:
    def headers(folder, pattern):
        return json.load(open(sorted(Path(folder).glob(pattern + ".headers.json"))[-1]))
    for pat in ["nws_alerts_active_*", "nws_observations_KDFW_*"]:
        h1, h2 = headers(POLL1, pat), headers(POLL2, pat)
        print(pat, "\\n  poll1:", h1["retrieved_utc"], h1["headers"], "\\n  poll2:", h2["retrieved_utc"], h2["headers"])

    act1 = read_nws_folder(POLL1, "nws_alerts_active_*.geojson"); act2 = read_nws_folder(POLL2, "nws_alerts_active_*.geojson")
    rec1 = read_nws_folder(POLL1, "nws_alerts_recent_*.geojson"); rec2 = read_nws_folder(POLL2, "nws_alerts_recent_*.geojson")
    s1, s2 = set(act1.get("id", [])), set(act2.get("id", []))
    print(f"\\nACTIVE alerts: poll1={len(s1)} poll2={len(s2)} new={len(s2 - s1)} no-longer-active={len(s1 - s2)} kept={len(s1 & s2)}")
    r1, r2 = set(rec1.id), set(rec2.id)
    print(f"RECENT alerts: poll1={len(r1)} poll2={len(r2)} new={len(r2 - r1)} dropped-from-window={len(r1 - r2)}")
    common = r1 & r2
    h = lambda df: dict(zip(df.id, [C.content_hash(r) for r in df.drop(columns=['raw_file']).to_dict('records')]))
    hh1, hh2 = h(rec1), h(rec2)
    print("Recent alerts whose content changed between polls (same id):", sum(hh1[i] != hh2[i] for i in common))
    new_al = rec2[rec2.id.isin(r2 - r1)]
    print("New alerts by messageType:", new_al.messageType.value_counts().to_dict(), " max sent poll1:", rec1.sent.max(), " poll2:", rec2.sent.max())

    ob2 = read_nws_folder(POLL2, "nws_observations_*.geojson")
    k1 = ob.set_index(["stationId", "timestamp"]); k2 = ob2.set_index(["stationId", "timestamp"])
    new_keys = k2.index.difference(k1.index); same = k2.index.intersection(k1.index)
    vc = [c for c in value_cols if c in k2.columns]
    changed = (k1.loc[same, vc].fillna(-9999).ne(k2.loc[same, vc].fillna(-9999))).any(axis=1)
    print(f"\\nOBSERVATIONS: poll1={len(k1)} poll2={len(k2)} new (station,timestamp)={len(new_keys)} revised existing={changed.sum()}")
    print("Max timestamp per station poll1 vs poll2:")
    display(pd.DataFrame({"poll1": ob.groupby("stationId").timestamp.max(), "poll2": ob2.groupby("stationId").timestamp.max()}))
    if changed.any():
        i = changed[changed].index[0]
        diff = pd.DataFrame({"poll1": k1.loc[i, vc], "poll2": k2.loc[i, vc]}); display(diff[diff.poll1.fillna(-9999) != diff.poll2.fillna(-9999)])
""")

# ------------------------------------------------------------------ volume
md("""
## 9. Historical volume estimate and partitioning
""")
code("""
listing = (sorted(SAMPLES.glob("noaa_storm_events_listing_*.html"))[-1]).read_text(encoding="utf-8")
rows = re.findall(r'>(StormEvents_details-ftp_v1\\.0_d(\\d{4})_c(\\d{8})\\.csv\\.gz)</a></td>\\s*<td[^>]*>([^<]+)</td>\\s*<td[^>]*>(\\d+)</td>', listing)
sizes = pd.DataFrame(rows, columns=["file", "year", "compiled", "modified", "bytes"]).astype({"year": int, "bytes": int})
sizes = sizes[sizes.year >= 2010]
sizes["MB_gz"] = (sizes.bytes / 2**20).round(1)
display(sizes[["year", "compiled", "modified", "MB_gz"]].set_index("year").T)
tx_per_year = len(se)
gh_rows_year = len(gh) / ((gh.DATE.max() - gh.DATE.min()).days / 365.25)
obs_per_day = (span.records / (span.hours_covered / 24)).mean()
vol = pd.DataFrame([
    ["H1 Storm Events (TX, 2010-2026)", f"{len(sizes)} yearly files, {sizes.MB_gz.sum():.0f} MB gz (US-wide)", f"~{tx_per_year * len(sizes):,.0f} TX rows", "1 file per year"],
    ["H2 GHCN-Daily (10 stations, 2010-2026)", f"~{gh_path.stat().st_size / 2**20 / ((gh.DATE.max()-gh.DATE.min()).days/365.25) * 17:.0f} MB JSON", f"~{gh_rows_year * 17:,.0f} station-days (x8 elements)", "1 request per station-year (170 requests)"],
    ["I1 NWS alerts (TX)", "~1.4 MB per week", f"~{len(al) * 52:,} alerts/year", "cursor pagination, poll every 15 min"],
    ["I2 NWS observations (10 stations)", "~2 MB per station per 500 obs", f"~{obs_per_day * 10 * 365:,.0f} obs/year", "per station, start=watermark-overlap"],
], columns=["source", "size", "rows", "partitioning"])
vol
""")

nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3"}})
out = ROOT / "notebooks" / "01_source_profiling.ipynb"
nbf.write(nb, out)
print("wrote", out)
