"""Static integrity checks for the Airflow DAG, Playwright extractor, Docker Compose, and seeder."""

from __future__ import annotations

import ast
from pathlib import Path

from seed_dev_database import load_ledger_files

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_dag_file_ast_and_configuration():
    dag_path = REPO_ROOT / "dags" / "financial_dw_etl_dag.py"
    source = dag_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(dag_path))

    # Ensure DAG syntax parses cleanly and defines all four expected task IDs
    assert tree is not None
    for expected_token in (
        'dag_id="financial_dw_etl_pipeline"',
        'task_id="extract_portal_catalog"',
        'task_id="transform_star_schema"',
        'task_id="validate_data_quality"',
        'task_id="load_data_warehouse"',
        'schedule="0 4 * * *"',
        "catchup=False",
        "max_active_runs=1",
    ):
        assert expected_token in source

    # Airflow Variables injected via AIRFLOW_VAR_* must be queried in uppercase Jinja keys
    for var_key in ("PORTAL_URL", "PORTAL_USER", "PORTAL_PASS", "SOURCE_TABLE", "SPLIT_PROJECT_CODES"):
        assert f"var.value.get('{var_key}'" in source


def test_playwright_extractor_syntax_and_dom_resilience():
    scraper_path = REPO_ROOT / "dags" / "scripts" / "extract_portal_catalog.py"
    source = scraper_path.read_text(encoding="utf-8")
    ast.parse(source, filename=str(scraper_path))

    assert 'state="attached"' in source
    assert "node => node.click()" in source
    assert "PORTAL_URL" in source


def test_docker_compose_hardening():
    compose_text = (REPO_ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    assert "/var/run/docker.sock" not in compose_text
    assert "AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'false'" in compose_text
    assert "airflow-scheduler:" in compose_text
    assert "airflow-webserver:" in compose_text

    reqs_text = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "apache-airflow-providers-docker" not in reqs_text
    assert "docker==" not in reqs_text


def test_seed_dev_database_reads_synthetic_sample():
    df_all = load_ledger_files(REPO_ROOT / "data" / "sample")
    assert len(df_all) == 40
    assert len(df_all.columns) == 21
