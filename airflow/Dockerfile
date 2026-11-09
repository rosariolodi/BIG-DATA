FROM apache/airflow:3.3.2-python3.12

USER root
# Spark necesita Java
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home \
 && test -x /opt/java-home/bin/java \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java-home

USER airflow
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
