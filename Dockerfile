FROM --platform=linux/amd64 apache/airflow:2.10.0

# OS-level build libraries required by pymssql (FreeTDS)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        freetds-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python runtime dependencies (pandas, pymssql, openpyxl, playwright)
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# System libraries required by headless Chromium for Playwright
USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright Chromium browser binary for the airflow runtime user
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium
