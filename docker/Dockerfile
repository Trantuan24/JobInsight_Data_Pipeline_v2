# JobInsight Data Pipeline Dockerfile
FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy application code
COPY dags /opt/airflow/dags
COPY src /opt/airflow/src
COPY sql /opt/airflow/sql

# Set working directory
WORKDIR /opt/airflow
