"""Build docs/report/Workshop1_Report.html from the real samples, then print it to PDF with Chrome.

    python docs/report/build_report.py            # html + pdf
"""
from __future__ import annotations

import base64
import html
import json
import shutil
import subprocess
import sys
from pathlib import Path
from string import Template

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src.normalization import canonical as C  # noqa: E402
from src.sources.raw_readers import read_ghcn_daily_json, read_nws_folder, read_storm_events  # noqa: E402

S = ROOT / "workshop_1" / "samples"
P1, P2 = S / "poll1", S / "poll2"
HERE = Path(__file__).parent


def latest(folder: Path, pattern: str) -> Path:
    files = [p for p in folder.glob(pattern) if not p.name.endswith(".headers.json")]
    return sorted(files)[-1]


def hdr(path: Path) -> dict:
    return json.loads(Path(str(path).removesuffix(".gz") + ".headers.json").read_text())


def esc(s: str) -> str:
    return html.escape(s)


def img(name: str) -> str:
    data = base64.b64encode((ROOT / "docs" / "figures" / name).read_bytes()).decode()
    return f"data:image/png;base64,{data}"


def table(df: pd.DataFrame) -> str:
    return df.to_html(index=False, border=0, classes="t", escape=True)


# ------------------------------------------------------------------ evidence snippets
se_file = latest(S, "noaa_storm_events_TX_*.csv")
se_lines = se_file.read_text(encoding="utf-8").splitlines()
se_raw = sorted((ROOT / "data" / "raw").glob("noaa_storm_events_*.csv.gz"))[-1]
se_h = json.loads(Path(str(se_raw) + ".headers.json").read_text())
se_snip = se_lines[0][:400] + " …\n" + next(l for l in se_lines if "Thunderstorm Wind" in l and '"MG"' in l)[:400] + " …"

gh_file = latest(S, "ghcn_daily_2*.json")
gh_json = json.loads(gh_file.read_text())
gh_h = hdr(gh_file)
gh_snip = json.dumps(gh_json[0], indent=1)

al_file = latest(P1, "nws_alerts_recent_*.geojson")
al_json = json.loads(al_file.read_text(encoding="utf-8"))
al_h = hdr(al_file)
f0 = next(f for f in al_json["features"] if f["properties"]["references"])
keep = ["id", "sent", "effective", "onset", "expires", "status", "messageType", "severity", "certainty",
        "urgency", "event", "areaDesc", "geocode", "references"]
al_snip = json.dumps({"geometry": "(Polygon, %d vertices)" % len(f0["geometry"]["coordinates"][0]) if f0["geometry"] else None,
                      "properties": {k: f0["properties"][k] for k in keep},
                      "pagination": al_json.get("pagination")}, indent=1)

ob_file = latest(P1, "nws_observations_KDFW_*.geojson*")
import gzip  # noqa: E402
ob_json = json.loads((gzip.open(ob_file, "rt", encoding="utf-8") if ob_file.suffix == ".gz" else open(ob_file, encoding="utf-8")).read())
ob_h = hdr(ob_file)
o0 = ob_json["features"][0]
keep = ["stationId", "timestamp", "textDescription", "temperature", "windSpeed", "windGust", "precipitationLastHour", "heatIndex"]
ob_snip = json.dumps({"geometry": o0["geometry"], "properties": {k: o0["properties"].get(k, "(key absent)") for k in keep}}, indent=1)

# ------------------------------------------------------------------ profiling numbers
se = read_storm_events(se_file)
gh = read_ghcn_daily_json(gh_file)
al = read_nws_folder(P1, "nws_alerts_recent_*.geojson")
ob = read_nws_folder(P1, "nws_observations_*.geojson")
canon = pd.concat([C.map_storm_events(se), C.map_ghcn_daily(gh), C.map_nws_alerts(al), C.map_nws_observations(ob)])
prof = canon.groupby("source").agg(records=("event_id", "size"), duplicate_ids=("event_id", lambda s: s.duplicated().sum()),
                                   first_utc=("timestamp", "min"), last_utc=("timestamp", "max"),
                                   no_coordinates_pct=("latitude", lambda s: round(s.isna().mean() * 100, 1))).reset_index()
prof["first_utc"] = prof.first_utc.dt.strftime("%Y-%m-%d %H:%M")
prof["last_utc"] = prof.last_utc.dt.strftime("%Y-%m-%d %H:%M")
raw_counts = {"noaa_storm_events": f"{len(se):,} rows", "ghcn_daily": f"{len(gh):,} station-days",
              "nws_alerts": f"{len(al):,} alerts", "nws_observations": f"{len(ob):,} observations"}
prof.insert(1, "raw_records", prof.source.map(raw_counts))
prof = prof.rename(columns={"records": "canonical_events"})

dq = pd.read_csv(ROOT / "workshop_1" / "data_quality_issues.csv")

# ------------------------------------------------------------------ polls
poll_rows, poll_notes = [], []
if P2.exists():
    a1, a2 = read_nws_folder(P1, "nws_alerts_active_*.geojson"), read_nws_folder(P2, "nws_alerts_active_*.geojson")
    r1, r2 = read_nws_folder(P1, "nws_alerts_recent_*.geojson"), read_nws_folder(P2, "nws_alerts_recent_*.geojson")
    o1, o2 = ob, read_nws_folder(P2, "nws_observations_*.geojson")
    t1 = hdr(latest(P1, "nws_alerts_active_*.geojson"))["retrieved_utc"]
    t2 = hdr(latest(P2, "nws_alerts_active_*.geojson"))["retrieved_utc"]
    s1, s2 = set(a1.get("id", [])), set(a2.get("id", []))
    q1, q2 = set(r1.id), set(r2.id)
    h = lambda df: dict(zip(df.id, [C.content_hash(r) for r in df.drop(columns=["raw_file"]).to_dict("records")]))  # noqa: E731
    hh1, hh2 = h(r1), h(r2)
    changed_al = sum(hh1[i] != hh2[i] for i in q1 & q2)
    new_types = r2[r2.id.isin(q2 - q1)].messageType.value_counts().to_dict()
    k1, k2 = o1.set_index(["stationId", "timestamp"]), o2.set_index(["stationId", "timestamp"])
    vc = [c for c in k1.columns if c.endswith(".value") and c in k2.columns]
    same = k1.index.intersection(k2.index)
    changed_ob = int(k1.loc[same, vc].fillna(-9999).ne(k2.loc[same, vc].fillna(-9999)).any(axis=1).sum())
    ha1, ha2 = hdr(latest(P1, "nws_alerts_active_*.geojson"))["headers"], hdr(latest(P2, "nws_alerts_active_*.geojson"))["headers"]
    hb1, hb2 = hdr(latest(P1, "nws_observations_KDFW_*.geojson*"))["headers"], hdr(latest(P2, "nws_observations_KDFW_*.geojson*"))["headers"]
    poll_rows = [
        ["Poll time (UTC)", t1, t2, "—"],
        ["Active alerts TX", len(s1), len(s2), f"{len(s2 - s1)} new, {len(s1 - s2)} no longer active, {len(s1 & s2)} unchanged ids"],
        ["Recent alerts TX (/alerts)", len(q1), len(q2), f"{len(q2 - q1)} new ids {new_types}; {len(q1 - q2)} left the window; {changed_al} same id with changed content"],
        ["Max alert 'sent'", r1.sent.max(), r2.sent.max(), "watermark candidate moves forward"],
        ["Observations (10 stations)", len(o1), len(o2), f"{len(k2.index.difference(k1.index))} new (station, timestamp); {changed_ob} existing keys with revised values"],
        ["Max obs timestamp KDFW", o1[o1.stationId == 'KDFW'].timestamp.max(), o2[o2.stationId == 'KDFW'].timestamp.max(), "per-station watermark"],
        ["Alerts ETag / Last-Modified", f"{ha1.get('ETag', '—')} / {ha1.get('Last-Modified', '—')}", f"{ha2.get('ETag', '—')} / {ha2.get('Last-Modified', '—')}", ""],
        ["Obs KDFW Expires / Last-Modified", f"{hb1.get('Expires', '—')} / {hb1.get('Last-Modified', '—')}", f"{hb2.get('Expires', '—')} / {hb2.get('Last-Modified', '—')}", ""],
    ]
    poll_tbl = table(pd.DataFrame(poll_rows, columns=["Measure", "Poll 1", "Poll 2", "What changed"]))
else:
    poll_tbl = "<p><b>Poll 2 not yet available.</b></p>"

# ------------------------------------------------------------------ overlap pairs (same rule as notebook §7)
STATION_COUNTY = {"USW00003927": ("TARRANT", "439"), "USW00012960": ("HARRIS", "201"), "USW00013904": ("TRAVIS", "453"),
                  "USW00012921": ("BEXAR", "029"), "USW00023044": ("EL PASO", "141"), "USW00023042": ("LUBBOCK", "303"),
                  "USW00023047": ("POTTER", "375"), "USW00012924": ("NUECES", "355"), "USW00023023": ("MIDLAND", "329"),
                  "USW00012919": ("CAMERON", "061")}
VAR = {"Thunderstorm Wind": "WSF5", "High Wind": "WSF5", "Hail": "WSF5", "Flash Flood": "PRCP", "Heavy Rain": "PRCP",
       "Heat": "TMAX", "Excessive Heat": "TMAX", "Extreme Cold/Wind Chill": "TMIN", "Cold/Wind Chill": "TMIN",
       "Winter Storm": "SNOW", "Frost/Freeze": "TMIN"}  # same rule as notebook section 7
LOW_TAIL = {"Extreme Cold/Wind Chill", "Cold/Wind Chill", "Frost/Freeze"}
g2 = gh.copy()
for e in ["TMAX", "TMIN", "PRCP", "SNOW", "WSF5"]:  # noqa
    g2[e] = pd.to_numeric(g2[e], errors="coerce")
g2["DATE"] = pd.to_datetime(g2.DATE)
se2 = se.copy()
se2["local_date"] = pd.to_datetime(se2.BEGIN_YEARMONTH + se2.BEGIN_DAY.str.zfill(2))
cands = []
for st, (name, fips) in STATION_COUNTY.items():
    m = ((se2.CZ_TYPE == "C") & (se2.CZ_FIPS.str.zfill(3) == fips)) | \
        ((se2.CZ_TYPE == "Z") & se2.CZ_NAME.str.contains(rf"\b{name}\b", regex=True))
    cands.append(se2[m & se2.EVENT_TYPE.isin(VAR)].merge(g2[g2.STATION == st], left_on="local_date", right_on="DATE"))
cands = pd.concat(cands)
n_pairs = len(cands)
cands["var"] = cands.EVENT_TYPE.map(VAR)
cands["ghcn"] = [r[v] for r, v in zip(cands.to_dict("records"), cands["var"])]
cands["rank_val"] = [-v if et in LOW_TAIL else v for v, et in zip(cands.ghcn, cands.EVENT_TYPE)]
pick = cands.sort_values("rank_val", ascending=False).groupby("EVENT_TYPE").head(1)
pick = pick[pick.EVENT_TYPE != "Hail"]  # hail day at Lubbock duplicates the Thunderstorm Wind pair
pairs = []
for _, r in pick.iterrows():
    se_val = ""
    if r.EVENT_TYPE in ("Thunderstorm Wind", "High Wind", "Strong Wind"):
        se_val = f"{r.MAGNITUDE} kt {r.MAGNITUDE_TYPE} = {float(r.MAGNITUDE) * C.KNOT_TO_MS:.1f} m/s"
    unit = {"WSF5": "m/s", "PRCP": "mm", "TMAX": "°C", "TMIN": "°C", "SNOW": "mm"}[r["var"]]
    pairs.append([f"SE {r.EVENT_ID} — {r.EVENT_TYPE}", f"{r.CZ_TYPE}:{r.CZ_NAME}", f"{r.BEGIN_DATE_TIME} {r.CZ_TIMEZONE}", se_val or "no magnitude",
                  f"{r.STATION} {r.DATE.date()}", f"{r['var']} = {r.ghcn} {unit}"])
pairs_tbl = table(pd.DataFrame(pairs, columns=["Storm Events record", "Zone/county", "Begin (local)", "SE magnitude", "GHCN record", "GHCN value"]))

# ------------------------------------------------------------------ render
tpl = Template((HERE / "report_template.html").read_text(encoding="utf-8"))
out_html = HERE / "Workshop1_Report.html"
out_html.write_text(tpl.substitute(
    se_snip=esc(se_snip), se_url=esc(se_h["url"]), se_hdr=esc(json.dumps(se_h["headers"])),
    gh_snip=esc(gh_snip), gh_url=esc(gh_h["url"][:260] + "…"), gh_hdr=esc(json.dumps(gh_h["headers"])),
    al_snip=esc(al_snip), al_url=esc(al_h["url"]), al_hdr=esc(json.dumps(al_h["headers"])),
    ob_snip=esc(ob_snip), ob_url=esc(ob_h["url"]), ob_hdr=esc(json.dumps(ob_h["headers"])),
    prof_tbl=table(prof), dq_tbl=table(dq), poll_tbl=poll_tbl, pairs_tbl=pairs_tbl, n_pairs=f"{n_pairs:,}",
    fig_types=img("storm_events_types.png"), fig_tmax=img("ghcn_tmax.png"),
), encoding="utf-8")
print("wrote", out_html)

chrome = next((p for p in [shutil.which("chrome"), r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                           r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"] if p and Path(p).exists()), None)
if chrome:
    pdf = ROOT / "docs" / "Workshop1_Report.pdf"
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={pdf}", out_html.as_uri()], check=True, capture_output=True, timeout=120)
    print("wrote", pdf)
