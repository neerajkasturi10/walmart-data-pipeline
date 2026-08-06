FROM apache/airflow:3.3.0

USER root

RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
