# Dashboard (Plotly Dash) — placeholder

Implemented in Workshop 4. It will run at `http://localhost:8050` and read **only** from the database
or its analytical views. Planned panels (from the user stories in the Workshop 1 report):

1. Current situation: map of stations coloured NORMAL / UNUSUAL / EXTREME + active NWS alerts.
2. What is new since my last visit: alerts and observations ingested in the last runs.
3. Historical vs current: today's value against the station-season percentile band.
4. Storm Events history for the selected county / zone.
5. Data health: last run, watermark age per source, rejected records.
