# Production Deployment, Sizing & Operations Runbook

Practical deployment blueprint covering operational impact, infrastructure sizing, security hardening, routine administration, and incident recovery for running `financial_dw_etl_pipeline` in production.

---

## Operational Impact Summary

| Dimension | Legacy Manual Process | Automated Airflow Pipeline |
|---|---|---|
| **Human Intervention** | Manual portal downloads, local Excel merges, and ad-hoc script runs | Zero manual touch; unattended daily execution at 04:00 |
| **Data Freshness** | On demand, dependent on analyst availability | Deterministic daily refresh ready before 08:00 executive reporting |
| **Schema Governance** | Risk of inconsistent manual edits and unverified rollups | Automated 8-table QA audit + circuit breaker blocking un-reconciled DW loads |
| **Error Visibility** | Silent failures discovered only when stakeholders report dashboard discrepancies | Task retries, Playwright PNG/HTML failure artifacts, and atomic SQL rollback |
| **Software Licensing Cost** | Proprietary orchestration fees avoided | `$0 USD` (Apache Airflow, Docker, PostgreSQL, Python, Playwright) |

---

## Infrastructure Sizing & Hosting Options

The entire stack (PostgreSQL 15 metadata store, Airflow Scheduler, Airflow Webserver, and periodic headless Chromium execution) runs comfortably on a single Linux virtual machine.

| Option | Recommended Specification | Estimated Monthly Infra Cost | Key Advantages |
|---|---|---|---|
| **Option A: Cloud VM (AWS EC2 / Azure VM / GCP Compute)** | 4 vCPU, 8 GB RAM, 50 GB SSD, Ubuntu LTS + Docker Engine | `~$50 to $80 USD` | Managed uptime SLA, automated disk snapshots, isolated VPC security groups, minimal hardware maintenance. |
| **Option B: On-Premises Virtual Machine** | 4 vCPU, 8 GB RAM, 50 GB SSD on internal hypervisor with outbound HTTPS access | `$0 USD` incremental hardware cost | Keeps ERP SQL Server and DW traffic strictly inside the corporate LAN under internal IT governance. |

### Network & Firewall Prerequisites
* **Outbound HTTPS (`443`):** Required from the Airflow host to the Grant Management Portal domain for Playwright extraction.
* **Internal SQL Server (`1433`):** Required from the Airflow host to the source ERP SQL Server and target Data Warehouse SQL Server instances.
* **Internal Web UI (`8080`):** Restrict inbound access to the Airflow Webserver port behind corporate VPN or a reverse proxy with TLS (`443`).

---

## Production Security Hardening Checklist

Before promoting the stack from local development to production, complete the following controls:

1. **Generate a Dedicated Fernet Key:**
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   Set `AIRFLOW_FERNET_KEY` in `.env` (or inject via your enterprise secrets manager) so Airflow encrypts connection credentials at rest in PostgreSQL.
2. **Provision Dedicated Least-Privilege Accounts:**
   * **Source ERP SQL login:** Grant `db_datareader` only on `dbo.FinanzasProyectos` (never `db_owner` or write access).
   * **Target DW SQL login:** Scope permissions exclusively to the target Data Warehouse database (`FinancialDW`).
   * **Grant Portal account:** Create a dedicated non-personal service account with read/export permissions only.
3. **Replace Default Passwords & Vault Secrets:**
   * Change `POSTGRES_PASSWORD` and `AIRFLOW_ADMIN_PASSWORD` in `.env`.
   * For enterprise deployments, configure an Airflow Secrets Backend (AWS Secrets Manager, Azure Key Vault, or HashiCorp Vault) instead of static `.env` files.
4. **Verify Hardened Compose Settings:**
   * Confirm `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` remains `'false'`.
   * Confirm `/var/run/docker.sock` is never mounted into Airflow containers.

---

## Daily Operations & Maintenance

### Starting and Stopping Services
* **Initial build or after `Dockerfile` / `requirements.txt` updates:**
  ```bash
  docker compose up -d --build
  ```
* **Pause without losing metadata or PostgreSQL state:**
  ```bash
  docker compose stop
  ```
* **Resume paused services:**
  ```bash
  docker compose start
  ```
* **Inspect live scheduler or webserver logs:**
  ```bash
  docker compose logs -f airflow-scheduler
  docker compose logs -f airflow-webserver
  ```

### Staging Directory Housekeeping
Each pipeline execution writes an isolated staging bundle under `data/staging/<ts_nodash>/`. Retain the most recent 14 to 30 days for audit replay and schedule a periodic host cron job to prune older run directories:

```bash
find ./data/staging -mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +
```

---

## Incident Triage & Recovery Playbook

### Case 1: `extract_portal_catalog` Fails
1. Open the failed task log in the Airflow UI to inspect whether authentication, navigation, or export generation timed out.
2. Inspect the automatically captured PNG screenshot and DOM HTML file inside `data/errors/` (`login_failure_*.png` or `extract_failure_*.png`).
3. Common root causes: rotated portal password (update `AIRFLOW_VAR_PORTAL_PASS`), changed portal CSS selector (override via environment variables without editing Python code), or transient portal slowdown (increase `EXPORT_TIMEOUT_MS`).
4. Once resolved, clear the failed task instance in the Airflow UI to resume downstream tasks cleanly.

### Case 2: `validate_data_quality` Fails (Circuit Breaker Triggered)
1. Confirm the Data Warehouse was **not modified** (Power BI remains served by the previous valid run).
2. Read the `DataQualityError` message in the Airflow task log.
3. If `QA_Cuadre` reports `REVISAR` rows, inspect `data/staging/<ts_nodash>/QA_Cuadre.csv` to identify which project and fiscal year has a Level 1 versus Level 5 ERP ledger mismatch exceeding `0.01`.
4. Coordinate with Finance / ERP stewards to fix the unbalanced ledger row, then rerun the DAG.

### Case 3: `load_data_warehouse` Fails Mid-Load
1. Verify in the task log that `Rolling back entire transaction` completed cleanly (guaranteeing zero partial table state in SQL Server DW).
2. Check SQL Server connectivity, disk space, or lock contention from an overlapping Power BI gateway refresh.
3. Clear and retry only the `load_data_warehouse` task; it re-reads the already validated staging bundle under `data/staging/<ts_nodash>/` without re-scraping the portal.
