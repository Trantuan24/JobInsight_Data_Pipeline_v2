# ============================================
# JobInsight Data Pipeline - Dockerfile
# Optimized for Airflow with Playwright support
# ============================================

FROM apache/airflow:2.8.1-python3.10

USER root

# ============================================
# SYSTEM DEPENDENCIES
# ============================================

# Install system packages for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER airflow

# ============================================
# PYTHON DEPENDENCIES
# ============================================

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ============================================
# PLAYWRIGHT SETUP
# ============================================

RUN playwright install chromium

# ============================================
# ENVIRONMENT VARIABLES
# ============================================

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}" \
    AIRFLOW_HOME="/opt/airflow" \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
