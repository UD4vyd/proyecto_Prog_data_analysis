# src

Python ingestion package (sources, normalization, storage), built in Workshops 2 and 3.
Workshop 1 only keeps the empty package layout; the evidence and profiling are in `notebooks/` and `workshop_1/`.

Workshop 2 adds the code that the Airflow DAGs call. The DAG files stay thin; the logic lives here so it can be
tested without Airflow and reused later by `ingest.py`.

- `config.py`: paths from environment variables (container) with repository defaults (laptop), stations, Texas box.
- `sources/`: `nws.py` (F3, F4), `ghcn.py` (F2), `storm_events.py` (F1), `http.py` (timeout and retry).
- `storage/bronze.py`: Bronze file names, raw saving with `.headers.json`, watermarks, ready markers.
- `storage/silver.py`: Silver Parquet files, rejected rows, manifest of processed Bronze files.
- `normalization/silver.py`: the preprocessing rules as pure functions, plus the pyarrow schemas.
- `pipelines/`: `bronze_jobs.py` and `silver_jobs.py`, one function per DAG task.
