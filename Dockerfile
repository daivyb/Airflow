FROM --platform=linux/amd64 apache/airflow:2.10.0

# ---------- Paquetes del SO necesarios para pymssql ----------
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        freetds-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------- Dependencias Python (incluye Playwright) ----------
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# ---------- Dependencias de SO para Playwright ----------
USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------- Descargar el navegador Chromium ----------
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium
