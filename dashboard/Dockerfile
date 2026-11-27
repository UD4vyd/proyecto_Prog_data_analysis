# Image of the two Plotly Dash dashboards (Workshop 4). Small on purpose: Python, Dash, Plotly, pandas and
# pyarrow. No Spark and no Airflow, because the dashboards only read the Gold Parquet files.
FROM python:3.12-slim

WORKDIR /app
COPY dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The code, config/ and datalake_gold/ are mounted by docker-compose.yml, so a new Gold file (and a code
# change) is visible without rebuilding the image.
ENV PYTHONPATH=/app \
    GOLD_DIR=/app/datalake_gold \
    DASH_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1
