# Airflow image with the Python libraries used by the DAGs.
FROM apache/airflow:3.4.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
