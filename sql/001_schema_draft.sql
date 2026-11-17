-- Draft from Workshop 1, implemented in Workshop 2.
-- PostgreSQL. Event fields and domain tables follow workshop_1/mapping.md.

-- Ingestion control ---------------------------------------------------------

CREATE TABLE source (
    source_id   TEXT PRIMARY KEY,               -- F1, F2, F3, F4
    name        TEXT NOT NULL,
    owner       TEXT NOT NULL,
    url         TEXT NOT NULL,
    license     TEXT NOT NULL
);

CREATE TABLE ingestion_run (
    run_id         BIGSERIAL PRIMARY KEY,
    source_id      TEXT NOT NULL REFERENCES source (source_id),
    mode           TEXT NOT NULL CHECK (mode IN ('historical', 'incremental')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    raw_file       TEXT,
    rows_loaded    INTEGER,
    rows_rejected  INTEGER
);

-- One watermark per source and partition (station for F2/F4, year for F1, '' for F3).
-- It only moves after a successful load (docs/ingestion_contract.md).
CREATE TABLE ingestion_state (
    source_id      TEXT NOT NULL REFERENCES source (source_id),
    partition_key  TEXT NOT NULL DEFAULT '',
    watermark      TIMESTAMPTZ NOT NULL,
    last_run_id    BIGINT REFERENCES ingestion_run (run_id),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, partition_key)
);

-- Quarantine: rows that failed validation, kept with the reason.
CREATE TABLE rejected_record (
    rejected_id    BIGSERIAL PRIMARY KEY,
    run_id         BIGINT NOT NULL REFERENCES ingestion_run (run_id),
    source_id      TEXT NOT NULL REFERENCES source (source_id),
    raw_reference  TEXT NOT NULL,
    reason         TEXT NOT NULL,
    payload        JSONB,
    rejected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Canonical event -----------------------------------------------------------

CREATE TABLE event (
    event_id       TEXT PRIMARY KEY,            -- source label + source key, e.g. F1:1230373
    source         TEXT NOT NULL REFERENCES source (source_id),
    event_type     TEXT NOT NULL,
    timestamp_utc  TIMESTAMPTZ NOT NULL,
    latitude       DOUBLE PRECISION,            -- NULL when the source only gives a zone
    longitude      DOUBLE PRECISION,
    severity       TEXT,                        -- number for F1, F2, F4; CAP category for F3
    severity_unit  TEXT,
    raw_reference  TEXT NOT NULL
);

-- Events that describe the same real-world event (F1-F2 candidate pairs, F3 message chains).
CREATE TABLE event_group (
    event_group_id  BIGINT NOT NULL,
    event_id        TEXT NOT NULL REFERENCES event (event_id),
    rule            TEXT NOT NULL,
    PRIMARY KEY (event_group_id, event_id)
);

-- Domain tables -------------------------------------------------------------

CREATE TABLE station (
    ghcn_id        TEXT PRIMARY KEY,
    wban           TEXT NOT NULL,
    icao           TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    forecast_zone  TEXT NOT NULL,
    county_zone    TEXT NOT NULL,
    timezone       TEXT NOT NULL
);

-- F2
CREATE TABLE daily_observation (
    ghcn_id    TEXT NOT NULL REFERENCES station (ghcn_id),
    date       DATE NOT NULL,                   -- local station day
    element    TEXT NOT NULL,                   -- TMAX, TMIN, PRCP
    value      DOUBLE PRECISION,
    unit       TEXT NOT NULL,
    m_flag     TEXT,
    q_flag     TEXT,
    s_flag     TEXT,
    is_trace   BOOLEAN NOT NULL DEFAULT false,
    raw_file   TEXT NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ghcn_id, date, element)
);

-- F4
CREATE TABLE observation (
    icao           TEXT NOT NULL REFERENCES station (icao),
    timestamp_utc  TIMESTAMPTZ NOT NULL,
    temperature_c  DOUBLE PRECISION,
    dewpoint_c     DOUBLE PRECISION,
    wind_speed_ms  DOUBLE PRECISION,
    wind_gust_ms   DOUBLE PRECISION,
    pressure_hpa   DOUBLE PRECISION,
    precip_1h_mm   DOUBLE PRECISION,
    qc_codes       JSONB,                       -- qualityControl code per variable
    raw_file       TEXT NOT NULL,
    PRIMARY KEY (icao, timestamp_utc)
);

-- F1
CREATE TABLE storm_event (
    event_id             BIGINT PRIMARY KEY,
    episode_id           BIGINT,
    event_type           TEXT NOT NULL,
    cz_type              TEXT NOT NULL,         -- C county, Z forecast zone
    cz_fips              INTEGER NOT NULL,
    cz_name              TEXT,
    begin_local          TEXT,                  -- BEGIN_DATE_TIME as received (local standard time)
    cz_timezone          TEXT NOT NULL,         -- CST-6, MST-7
    begin_utc            TIMESTAMPTZ NOT NULL,
    end_utc              TIMESTAMPTZ,
    lat                  DOUBLE PRECISION,
    lon                  DOUBLE PRECISION,
    magnitude            DOUBLE PRECISION,
    magnitude_type       TEXT,
    damage_property_usd  NUMERIC,               -- NULL means unknown, not zero
    source_file          TEXT NOT NULL
);

-- F3
CREATE TABLE alert (
    alert_id      TEXT PRIMARY KEY,
    message_type  TEXT NOT NULL,                -- Alert, Update, Cancel
    event         TEXT NOT NULL,
    severity      TEXT,
    certainty     TEXT,
    urgency       TEXT,
    sent_utc      TIMESTAMPTZ NOT NULL,
    onset_utc     TIMESTAMPTZ,
    expires_utc   TIMESTAMPTZ,
    area_desc     TEXT,
    raw_file      TEXT NOT NULL
);

CREATE TABLE alert_zone (
    alert_id  TEXT NOT NULL REFERENCES alert (alert_id),
    ugc       TEXT NOT NULL,
    PRIMARY KEY (alert_id, ugc)
);

-- referenced_id has no foreign key: the earlier message can be older than the 7-day window.
CREATE TABLE alert_reference (
    alert_id       TEXT NOT NULL REFERENCES alert (alert_id),
    referenced_id  TEXT NOT NULL,
    PRIMARY KEY (alert_id, referenced_id)
);
