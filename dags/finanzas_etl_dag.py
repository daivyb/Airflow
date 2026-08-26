"""
finanzas_etl_dag.py
===================
DAG principal del pipeline ETL Financiero de Profonanpe.

Tareas:
  1. scrape_vtrack  — DockerOperator (Playwright) que descarga el reporte
     de entidades desde vtrack.cl y lo guarda en /data/reporte_completo.xlsx.
  2. transformar_finanzas — PythonOperator que ejecuta el ETL completo:
     extrae de SQL Server, transforma (modelo estrella) y carga al DW.

Dependencia: scrape_vtrack >> transformar_finanzas
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.etl_finanzas import ejecutar_transformacion

# =========================================================
# Configuración del DAG
# =========================================================

default_args = {
    "owner": "profonanpe",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Ruta al directorio data/ en el host (para el mapeo de volumen
# del DockerOperator). Se resuelve relativo al directorio del DAG.
HOST_DATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)

HOST_SCRIPTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "scripts")
)


with DAG(
    dag_id="finanzas_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL Financiero Profonanpe — Scraping vtrack + Transformación + Carga DW",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["profonanpe", "finanzas", "etl"],
) as dag:

    # ---------------------------------------------------------
    # Tarea 1: Scraping con Playwright (BashOperator)
    # ---------------------------------------------------------
    scrape_vtrack = BashOperator(
        task_id="scrape_vtrack",
        bash_command='python /opt/airflow/dags/scripts/scrape_vtrack.py',
        env={
            "VTRACK_URL": "{{ var.value.get('vtrack_url', 'https://www.vtrack.cl') }}",
            "VTRACK_USER": "{{ var.value.get('vtrack_user', '') }}",
            "VTRACK_PASS": "{{ var.value.get('vtrack_pass', '') }}",
            "OUTPUT_PATH": "/opt/airflow/data/reporte_completo.xlsx",
        },
    )

    # ---------------------------------------------------------
    # Tarea 2: Transformación ETL (PythonOperator)
    # ---------------------------------------------------------
    transformar_finanzas = PythonOperator(
        task_id="transformar_finanzas",
        python_callable=ejecutar_transformacion,
    )

    # ---------------------------------------------------------
    # Dependencias
    # ---------------------------------------------------------
    scrape_vtrack >> transformar_finanzas
