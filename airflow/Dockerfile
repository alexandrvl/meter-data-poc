FROM apache/airflow:2.10.4

USER root
RUN apt-get update && \
    apt-get install -y git python3-venv libpq-dev build-essential && \
    apt-get clean

RUN mkdir -p /opt/dbt/meterdata && chown -R airflow:root /opt/dbt
COPY dbt/meterdata /opt/dbt/meterdata
RUN chown -R airflow:root /opt/dbt/meterdata && \
    chmod -R 775 /opt/dbt/meterdata

USER airflow
COPY airflow/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

