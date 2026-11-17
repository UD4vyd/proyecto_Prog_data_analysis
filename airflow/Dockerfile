# Official Airflow image plus only the packages it does not ship (see airflow/requirements.txt).
# The install uses the Airflow constraints file of the same Airflow and Python version, so pandas,
# pyarrow and requests stay at the versions Airflow was tested with. Poetry (pyproject.toml) stays the
# source of truth for local work; this file is only for the containers.
FROM apache/airflow:3.4.0

COPY requirements.txt /tmp/requirements.txt
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
