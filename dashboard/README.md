# dashboard

Two independent Plotly Dash applications (Workshop 4). Both read **only** the newest Gold Parquet file of
their kind in `datalake_gold/` with `pd.read_parquet()`; neither touches Silver, Bronze or an API.

| App | File | Port | Gold file | Audience |
|---|---|---|---|---|
| Governance | `governance_app.py` | 8050 | `governance_*.parquet` | data engineers, course reviewers |
| Storytelling | `storytelling_app.py` | 8051 | `storytelling_*.parquet` | emergency-management analyst (Workshop 1) |

```
dashboard/
  governance_app.py        layout, figures and callbacks of the governance dashboard
  storytelling_app.py      layout, figures and callbacks of the storytelling dashboard
  common/data.py           finds and reads the newest Gold file (cached by file name and modification time)
  common/governance_prep.py   pandas preparation of the governance charts and the KPI table
  common/storytelling_prep.py pandas preparation of the storytelling charts and the narrative text
  common/palette.py        colour palettes (checked for colour-vision deficiency)
  common/theme.py          Plotly template, titles and the source note of every chart
  common/components.py     header, KPI cards, chart cards (Dash Bootstrap Components)
  assets/style.css         shared CSS, served by Dash automatically
  Dockerfile, requirements.txt   image used by the two services of docker-compose.yml
```

KPI thresholds are in `config/governance_thresholds.yaml`.

Run locally (from the repository root):

```bash
poetry install --with dashboard
poetry run python dashboard/governance_app.py      # http://localhost:8050
poetry run python dashboard/storytelling_app.py    # http://localhost:8051
```

Or with Docker: `docker compose up -d dashboard-governance dashboard-storytelling`.

Both apps reload the newest Gold file every 5 minutes and with the **Refresh data** button, so a new run of
`gold_processing` appears without a restart. Environment variables: `GOLD_DIR` (default `datalake_gold/`),
`DASH_HOST` (default `127.0.0.1`; `0.0.0.0` in Docker), `GOVERNANCE_PORT`, `STORYTELLING_PORT`, `DASH_DEBUG=1`.
