"""Apache Airflow DAG for the Multi-Year Financial Data Warehouse ETL Pipeline.

Orchestrates the productionized version of the Kimball Star Schema and automated
eight-table QA reconciliation engine (aligned with Module 02 of daivyb/BI-Reporting).

Task Graph:
    extract_portal_catalog >> transform_star_schema >> validate_data_quality >> load_data_warehouse
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts.dw_loader import run_load_task, run_transform_task, run_validate_task

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

RUN_STAGING_DIR = "/opt/airflow/data/staging/{{ ts_nodash }}"
PORTAL_CATALOG_PATH = f"{RUN_STAGING_DIR}/portal_entities_catalog.xlsx"

with DAG(
    dag_id="financial_dw_etl_pipeline",
    default_args=default_args,
    description="Automated multi-year financial Kimball Star Schema ETL with Playwright extraction, DQ gate, and transactional SQL Server DW load.",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "etl", "kimball", "sqlserver", "finance", "bi"],
) as dag:

    extract_portal_catalog = BashOperator(
        task_id="extract_portal_catalog",
        bash_command="python /opt/airflow/dags/scripts/extract_portal_catalog.py",
        env={
            "PORTAL_URL": "{{ var.value.get('PORTAL_URL', '') }}",
            "PORTAL_USER": "{{ var.value.get('PORTAL_USER', '') }}",
            "PORTAL_PASS": "{{ var.value.get('PORTAL_PASS', '') }}",
            "PORTAL_LOGIN_PATH": "{{ var.value.get('PORTAL_LOGIN_PATH', '/users/sign_in') }}",
            "PORTAL_REPORT_PATH": "{{ var.value.get('PORTAL_REPORT_PATH', '/entities/catalog') }}",
            "OUTPUT_PATH": PORTAL_CATALOG_PATH,
            "ERROR_ARTIFACT_DIR": "/opt/airflow/data/errors",
        },
    )

    transform_star_schema = PythonOperator(
        task_id="transform_star_schema",
        python_callable=run_transform_task,
        op_kwargs={
            "portal_catalog_path": PORTAL_CATALOG_PATH,
            "staging_dir": RUN_STAGING_DIR,
            "source_conn_id": "sqlserver_source",
            "source_table": "{{ var.value.get('SOURCE_TABLE', 'dbo.FinanzasProyectos') }}",
            "split_codes_raw": "{{ var.value.get('SPLIT_PROJECT_CODES', '900') }}",
        },
    )

    validate_data_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=run_validate_task,
        op_kwargs={
            "staging_dir": RUN_STAGING_DIR,
            "strict_reconciliation": True,
        },
    )

    load_data_warehouse = PythonOperator(
        task_id="load_data_warehouse",
        python_callable=run_load_task,
        op_kwargs={
            "staging_dir": RUN_STAGING_DIR,
            "dw_conn_id": "sqlserver_dw",
            "dw_schema": "dbo",
        },
    )

    (
        extract_portal_catalog
        >> transform_star_schema
        >> validate_data_quality
        >> load_data_warehouse
    )
