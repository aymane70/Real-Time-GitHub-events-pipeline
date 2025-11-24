FROM apache/airflow:2.8.1

USER root

# Install Java (required for PySpark) and procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python \
    requests \
    psycopg2-binary \
    python-dotenv \
    cryptography