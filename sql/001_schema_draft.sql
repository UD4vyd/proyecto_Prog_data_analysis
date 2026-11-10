-- Team 5 - Extreme Weather Monitoring (Texas)
-- DRAFT canonical schema from Workshop 1 (PostgreSQL 16 + PostGIS). Finalised in Workshop 2.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------- common contract
CREATE TABLE source (
    source_id   TEXT PRIMARY KEY,                 -- noaa_storm_events, ghcn_daily, nws_alerts, nws_observations
    name        TEXT NOT NULL,
    url         TEXT NOT NULL,
    format      TEXT NOT NULL,
    license     TEXT NOT NULL,
    role        TEXT NOT NULL CHECK (role IN ('historical', 'incremental'))
);

CREATE TABLE ingestion_run (
    run_id        BIGSERIAL PRIMARY KEY,
    source_id     TEXT REFERENCES source(source_id),
    mode          TEXT NOT NULL CHECK (mode IN ('historical', 'incremental')),
    started       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished      TIMESTAMPTZ,
    status        TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_in       INTEGER DEFAULT 0,
    rows_new      INTEGER DEFAULT 0,
    rows_updated  INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    error         TEXT
);

CREATE TABLE ingestion_state (
    source_id     TEXT PRIMARY KEY REFERENCES source(source_id),
    watermark     TEXT NOT NULL,                  -- ISO timestamp, or file compilation tag for Storm Events
    last_success  TIMESTAMPTZ,
    last_run_id   BIGINT REFERENCES ingestion_run(run_id)
);

CREATE TABLE event (
    event_id       TEXT PRIMARY KEY,              -- '<source>:<native key>' (deterministic)
    source         TEXT NOT NULL REFERENCES source(source_id),
    event_type     TEXT NOT NULL,
    "timestamp"    TIMESTAMPTZ NOT NULL,          -- UTC
    latitude       DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude      DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    location_method TEXT DEFAULT 'point',         -- point | polygon_centroid | zone_centroid
    severity       DOUBLE PRECISION,
    severity_unit  TEXT,
    raw_reference  TEXT NOT NULL,
    content_hash   CHAR(64) NOT NULL,
    first_seen_run BIGINT REFERENCES ingestion_run(run_id),
    last_seen_run  BIGINT REFERENCES ingestion_run(run_id),
    geom           GEOMETRY(Point, 4326)
);
CREATE INDEX event_type_ts_idx ON event (event_type, "timestamp");
CREATE INDEX event_geom_idx    ON event USING GIST (geom);

CREATE TABLE event_group (
    group_id    BIGINT NOT NULL,
    event_id    TEXT NOT NULL REFERENCES event(event_id),
    match_rule  TEXT,
    match_score DOUBLE PRECISION,
    PRIMARY KEY (group_id, event_id)
);

CREATE TABLE rejected_record (
    id        BIGSERIAL PRIMARY KEY,
    run_id    BIGINT REFERENCES ingestion_run(run_id),
    source    TEXT NOT NULL,
    reason    TEXT NOT NULL,
    payload   JSONB NOT NULL,
    rejected_at TIMESTAMPTZ DEFAULT now()
);

-- ---------------------------------------------------------------- static reference
CREATE TABLE zone (
    ugc_code     TEXT PRIMARY KEY,                -- TXC201 (county) / TXZ213 (forecast zone)
    zone_type    TEXT NOT NULL,
    name         TEXT NOT NULL,
    county_fips  TEXT,
    geom         GEOMETRY(MultiPolygon, 4326)
);

CREATE TABLE weather_station (
    station_id     TEXT PRIMARY KEY,              -- = nws_id (KDFW)
    ghcn_id        TEXT UNIQUE NOT NULL,          -- USW00003927
    nws_id         TEXT UNIQUE NOT NULL,
    name           TEXT NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    elevation_m    DOUBLE PRECISION,
    county_fips    TEXT,
    utc_offset_lst SMALLINT NOT NULL              -- -6 or -7
);

-- ---------------------------------------------------------------- domain tables
CREATE TABLE weather_measurement (
    event_id         TEXT PRIMARY KEY REFERENCES event(event_id),
    station_id       TEXT NOT NULL REFERENCES weather_station(station_id),
    element          TEXT NOT NULL,               -- TMAX, PRCP, temperature, windGust...
    period           TEXT NOT NULL,               -- P1D (GHCN) | PT1H / instant (NWS)
    obs_date         DATE,                        -- local standard day for P1D
    value            DOUBLE PRECISION NOT NULL,
    unit             TEXT NOT NULL,
    raw_value        TEXT,
    raw_unit         TEXT,
    measurement_flag TEXT,
    quality_flag     TEXT,
    source_flag      TEXT,
    qc_code          TEXT                         -- NWS qualityControl (V, C, Z...)
);

CREATE TABLE storm_report (
    event_id            TEXT PRIMARY KEY REFERENCES event(event_id),
    episode_id          BIGINT,
    cz_type             CHAR(1) NOT NULL,         -- C county | Z zone | M marine
    cz_fips             TEXT NOT NULL,
    cz_name             TEXT NOT NULL,
    ugc_code            TEXT REFERENCES zone(ugc_code),
    wfo                 TEXT,
    end_timestamp       TIMESTAMPTZ,
    magnitude           DOUBLE PRECISION,
    magnitude_type      TEXT,
    deaths_direct       INTEGER, deaths_indirect   INTEGER,
    injuries_direct     INTEGER, injuries_indirect INTEGER,
    damage_property_usd DOUBLE PRECISION,
    damage_crops_usd    DOUBLE PRECISION,
    tor_f_scale         TEXT,
    tor_length_mi       DOUBLE PRECISION,
    tor_width_yd        DOUBLE PRECISION,
    begin_location      TEXT,
    narrative           TEXT,
    file_compilation    CHAR(8)                   -- _cYYYYMMDD of the yearly file
);

CREATE TABLE weather_alert (
    event_id      TEXT PRIMARY KEY REFERENCES event(event_id),
    cap_id        TEXT UNIQUE NOT NULL,
    message_type  TEXT NOT NULL,                  -- Alert | Update | Cancel
    status        TEXT NOT NULL,
    severity      TEXT, certainty TEXT, urgency TEXT,
    sent          TIMESTAMPTZ NOT NULL,
    onset         TIMESTAMPTZ,
    expires       TIMESTAMPTZ,
    ends          TIMESTAMPTZ,
    vtec          TEXT,
    headline      TEXT,
    sender_name   TEXT,
    geom          GEOMETRY(Polygon, 4326)
);

CREATE TABLE alert_zone (
    event_id  TEXT REFERENCES weather_alert(event_id),
    ugc_code  TEXT,
    PRIMARY KEY (event_id, ugc_code)
);

CREATE TABLE alert_reference (
    event_id           TEXT REFERENCES weather_alert(event_id),
    referenced_cap_id  TEXT,
    referenced_sent    TIMESTAMPTZ,
    PRIMARY KEY (event_id, referenced_cap_id)
);
