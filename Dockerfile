# Airflow image with the libraries of the project.
# The list of libraries is read from pyproject.toml, which is managed with Poetry, so there is one place
# to declare a dependency. Only the direct dependencies are installed here and pip resolves the rest
# inside the Airflow environment: the full Poetry lock cannot be installed in this image, because Airflow
# pins some of the same packages to other versions (for example python-dateutil below 3).
FROM apache/airflow:3.4.0
COPY pyproject.toml /tmp/pyproject.toml
RUN python -c "import subprocess, sys, tomllib; deps = tomllib.load(open('/tmp/pyproject.toml', 'rb'))['project']['dependencies']; subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--no-cache-dir', *deps])"
