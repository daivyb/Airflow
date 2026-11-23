# Architecture Decisions & Production Troubleshooting Notes

This document records the key architectural decisions behind the Apache Airflow orchestration stack and summarizes six real-world production issues encountered and resolved during containerization, headless browser automation, and SQL Server integration.

---

## Architectural Decision Records (ADRs)

### ADR 01: Native Playwright Installation Over `DockerOperator`
* **Context:** Early iterations executed browser automation inside a sibling container via Airflow's `DockerOperator`. That required mounting `/var/run/docker.sock` and sharing bind volumes across nested containers.
* **Decision:** Install FreeTDS, Playwright, and Chromium natively inside the custom Airflow `Dockerfile` and invoke the scraper with a lightweight `BashOperator`.
* **Consequence:** Eliminates Docker-in-Docker bind-mount path errors on Windows/WSL2, removes the security risk of exposing `/var/run/docker.sock` inside the Airflow runtime, and reduces task startup latency.

### ADR 02: Decoupled Four-Task DAG with Typed Run Staging
* **Context:** Combining extraction, transformation, validation, and Data Warehouse replacement into one Python callable made retries expensive and risked partial writes whenever upstream data drifted.
* **Decision:** Split execution into four idempotent tasks (`extract_portal_catalog >> transform_star_schema >> validate_data_quality >> load_data_warehouse`) exchanging artifacts through a per-run typed staging bundle (`CSV` + `manifest.json`) keyed by `{{ ts_nodash }}`.
* **Consequence:** Preserves exact string codes with leading zeros (such as Peruvian Ubigeo `"04"` and project codes `"101"`, `"900010"`) and datetime types without adding heavy C++ binary dependencies, while allowing `validate_data_quality` to block invalid loads cleanly.

### ADR 03: Single-Transaction Data Warehouse Load with Empty QA Table Persistence
* **Context:** Dropping and recreating tables one by one with intermediate commits exposes Power BI refreshes to inconsistent snapshots if a later table fails. Omitting empty QA tables also breaks downstream monitoring views.
* **Decision:** Execute all 18 `DROP TABLE`, `CREATE TABLE`, and batched `INSERT INTO` operations inside a single SQL Server transaction (`conn.autocommit(False)`), committing only after every table succeeds and rolling back completely on any exception. Empty QA audit tables are still created with their exact schema.
* **Consequence:** Guarantees atomic DW refreshes and deterministic SQL schemas for Power BI and BI stewards.

### ADR 04: Separate Scheduler and Webserver Containers
* **Context:** Running `airflow scheduler & airflow webserver` inside one container masks scheduler crashes because the container remains alive as long as the foreground webserver stays up.
* **Decision:** Run `airflow-scheduler` and `airflow-webserver` as independent Docker Compose services, each with its own restart policy and healthcheck, and disable UI configuration exposure (`AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'false'`).
* **Consequence:** Container orchestration accurately detects and restarts failed scheduler or webserver processes independently.

---

## Production Troubleshooting Lessons Learned

### 1. `DockerOperator` Bind Mount Failure on Windows / WSL2
* **Symptom:** Launching a child container through `DockerOperator` attempted to mount `/opt/airflow/data` to the worker container. Docker Desktop on Windows translated the Linux path literally into `C:\opt\airflow\...`, throwing missing-path mount errors.
* **Root Cause:** Nested bind mounts executed through the host Docker daemon resolve relative to the host filesystem rather than the Airflow container filesystem.
* **Resolution:** Replaced `DockerOperator` with native Playwright + Chromium layers in the Airflow `Dockerfile`, executing the extractor directly via `BashOperator` and removing `docker` and `apache-airflow-providers-docker` dependencies.

### 2. Asynchronous Dropdown DOM Race Conditions in Headless Chromium
* **Symptom:** After clicking the portal export menu trigger, Playwright either failed silently on raw JS clicks (receiving `null` before DOM injection completed) or raised visibility timeout errors under `force=True` because CSS dropdown animations had not finished rendering.
* **Root Cause:** The server-rendered portal injects the export anchor element asynchronously into the DOM after the menu toggle event fires.
* **Resolution:** Adopted a two-step deterministic interaction in `dags/scripts/extract_portal_catalog.py`: wait explicitly for DOM attachment (`state="attached"`), then trigger the element click directly via JavaScript evaluation:
  ```python
  export_locator.wait_for(state="attached", timeout=15000)
  export_locator.evaluate("node => node.click()")
  ```

### 3. `pymssql` Bulk Insert Error: `Invalid column name 'nan'`
* **Symptom:** Bulk inserts into SQL Server failed whenever numeric or datetime columns contained missing values (`NaN` or `NaT`). `pymssql` serialized floating-point `nan` as an unquoted token, causing SQL Server to raise `Invalid column name 'nan'`.
* **Root Cause:** Standard `df.where(pd.notna(df), None)` on float or datetime Series silently coerces Python `None` back into `NaN`/`NaT` inside pandas blocks.
* **Resolution:** Implemented `sanitize_dataframe_for_pymssql()` in `dags/scripts/dw_loader.py` to extract columns through `object` dtype into native Python lists, explicitly mapping every `pd.isna(v)` value to Python `None` and converting `pd.Timestamp` instances via `.to_pydatetime()`.

### 4. SQL Server Error `10061` ("Connection Refused") from Docker Containers
* **Symptom:** Connecting from the Linux Airflow container to a local Windows SQL Server Express instance via `host.docker.internal:1433` raised `Adaptive Server is unavailable or does not exist (10061)`.
* **Root Cause:** SQL Server Express disables TCP/IP listeners by default and uses dynamic ports instead of static port `1433`.
* **Resolution:** Opened **SQL Server Configuration Manager**, enabled **TCP/IP** under SQL Server Network Configuration, configured IPAll TCP Port to `1433`, ensured the Windows Firewall allowed inbound port `1433` from the Docker bridge subnet, and restarted the SQL Server service.

### 5. SQL Server Authentication Failures from Linux Containers
* **Symptom:** `pymssql` connections failed authentication when targeting SQL Server instances configured exclusively for Windows Integrated Authentication (NTLM/Kerberos).
* **Root Cause:** Containerized Linux workloads cannot natively negotiate host Windows domain tokens without complex Kerberos keytab provisioning.
* **Resolution:** Enabled **Mixed Mode Authentication** (SQL Server and Windows Authentication mode) on the SQL Server instance and provisioned a dedicated least-privilege SQL login for the Airflow ETL service (`read-only` on the source ERP database, `db_ddladmin` + `db_datawriter` on the target DW).

### 6. Airflow Variable Case Sensitivity in Jinja Templates and Docker Compose `$` Escaping
* **Symptom:** Environment variables injected via `AIRFLOW_VAR_PORTAL_USER` resolved as empty strings when queried in lowercase (`{{ var.value.get('portal_user') }}`), and passwords containing `$` characters were truncated by Docker Compose variable interpolation.
* **Root Cause:** Airflow registers `AIRFLOW_VAR_*` keys in uppercase inside its metadata store, and Docker Compose treats single `$` tokens inside `.env` files as variable references.
* **Resolution:** Standardized all Airflow Jinja variable references to strict uppercase keys (enforced automatically by `tests/test_dag_integrity.py`) and documented escaping literal `$` characters as `$$` inside `.env`.
