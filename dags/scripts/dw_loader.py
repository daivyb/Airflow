"""SQL Server I/O, atomic transactional Data Warehouse loader, and Airflow task callables.

Isolates pymssql database connectivity and Airflow runtime hooks from the pure
Python transformation and data quality modules so all SQL generation, NaN
sanitization, and rollback behavior can be verified in automated tests without
requiring a live SQL Server instance.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.data_quality import validate_star_schema
from scripts.financial_transformer import (
    REQUIRED_FINANCIAL_COLS,
    parse_split_codes,
    read_staging_bundle,
    transform_financial_portfolio,
    write_staging_bundle,
)

logger = logging.getLogger(__name__)

_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def validate_sql_identifier(identifier: str) -> str:
    """Validates a single SQL Server identifier against a strict allowlist."""
    if not _SQL_IDENT_RE.fullmatch(identifier):
        raise ValueError(f"Invalid SQL identifier: '{identifier}'")
    return identifier


def validate_qualified_table(table_name: str) -> str:
    """Validates a schema-qualified SQL Server table name (e.g. dbo.FinanzasProyectos)."""
    if not _QUALIFIED_TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid qualified SQL table name: '{table_name}'")
    return table_name


def sanitize_dataframe_for_pymssql(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    """Converts pandas NaN/NaT/NA values safely into Python None for pymssql inserts.

    Extracts columns via object dtype into pure Python lists so pandas cannot
    re-coerce None back into float NaN or datetime NaT, preventing pymssql
    failures such as 'Invalid column name nan'.
    """
    if df.empty:
        return []

    sanitized_columns: list[list[Any]] = []
    for col in df.columns:
        raw_values = df[col].astype(object).tolist()
        clean_values = [
            None
            if pd.isna(v)
            else (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v)
            for v in raw_values
        ]
        sanitized_columns.append(clean_values)

    return list(zip(*sanitized_columns))


def generate_create_table_sql(df: pd.DataFrame, table_name: str, schema: str = "dbo") -> str:
    """Generates a SQL Server CREATE TABLE statement inferred from DataFrame dtypes."""
    safe_schema = validate_sql_identifier(schema)
    safe_table = validate_sql_identifier(table_name)

    col_defs: list[str] = []
    for col_name, dtype in df.dtypes.items():
        safe_col = validate_sql_identifier(str(col_name))
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "DATETIME"
        else:
            sql_type = "NVARCHAR(500)"
        col_defs.append(f"[{safe_col}] {sql_type} NULL")

    return f"CREATE TABLE [{safe_schema}].[{safe_table}] ({', '.join(col_defs)})"


def load_tables_transactionally(
    conn: Any,
    tables: dict[str, pd.DataFrame],
    schema: str = "dbo",
    batch_size: int = 500,
) -> dict[str, int]:
    """Replaces all Data Warehouse tables inside a single atomic database transaction.

    All DROP, CREATE, and batched INSERT statements execute within one transaction
    boundary. If any table raises an error, the entire transaction rolls back so
    downstream Power BI models never observe a partially refreshed warehouse.
    """
    safe_schema = validate_sql_identifier(schema)
    if hasattr(conn, "autocommit"):
        conn.autocommit(False)

    cursor = conn.cursor()
    loaded_counts: dict[str, int] = {}

    try:
        for table_name, df in tables.items():
            safe_table = validate_sql_identifier(table_name)
            qualified = f"[{safe_schema}].[{safe_table}]"
            logger.info("Staging DW table %s (%d rows)...", qualified, len(df))

            cursor.execute(
                f"IF OBJECT_ID('{safe_schema}.{safe_table}', 'U') IS NOT NULL DROP TABLE {qualified}"
            )
            cursor.execute(generate_create_table_sql(df, safe_table, schema=safe_schema))

            rows = sanitize_dataframe_for_pymssql(df)
            if rows:
                safe_cols = [validate_sql_identifier(str(c)) for c in df.columns]
                col_list = ", ".join(f"[{c}]" for c in safe_cols)
                placeholders = ", ".join(["%s"] * len(safe_cols))
                insert_sql = f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"

                for start in range(0, len(rows), batch_size):
                    cursor.executemany(insert_sql, rows[start : start + batch_size])

            loaded_counts[table_name] = len(rows)

        conn.commit()
        logger.info("Committed %d tables atomically to SQL Server DW.", len(loaded_counts))
        return loaded_counts

    except Exception:
        logger.exception("Error during DW load. Rolling back entire transaction.")
        conn.rollback()
        raise
    finally:
        cursor.close()


def _get_pymssql_conn(conn_id: str) -> Any:
    """Builds a pymssql connection from an Airflow Connection ID."""
    import pymssql
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    return pymssql.connect(
        server=conn.host,
        port=int(conn.port or 1433),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
    )


def _extract_source_financials(conn_id: str, source_table: str) -> pd.DataFrame:
    """Extracts the multi-year ERP financial ledger table from SQL Server."""
    safe_table = validate_qualified_table(source_table)
    select_cols = ", ".join(f"[{c}]" for c in REQUIRED_FINANCIAL_COLS)
    query = f"SELECT {select_cols} FROM {safe_table}"

    logger.info("Extracting source financial records from %s (%s)...", safe_table, conn_id)
    conn = _get_pymssql_conn(conn_id)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    df["ArchivoOrigen"] = safe_table
    logger.info("Extracted %d financial rows from SQL Server.", len(df))
    return df


def _read_portal_catalog(portal_catalog_path: str) -> pd.DataFrame:
    """Reads the portal entities Excel file downloaded by the Playwright task."""
    catalog_path = Path(portal_catalog_path)
    if not catalog_path.exists():
        raise FileNotFoundError(f"Portal entities catalog not found at '{catalog_path}'.")
    logger.info("Reading portal entities catalog from %s...", catalog_path)
    df = pd.read_excel(catalog_path)
    df.columns = df.columns.astype(str).str.strip()
    logger.info("Loaded %d portal entity rows.", len(df))
    return df


# =============================================================================
# Airflow PythonOperator Task Callables
# =============================================================================


def run_transform_task(
    portal_catalog_path: str,
    staging_dir: str,
    source_conn_id: str = "sqlserver_source",
    source_table: str = "dbo.FinanzasProyectos",
    split_codes_raw: str = "900",
    **context: Any,
) -> dict[str, Any]:
    """Airflow Task 2: Extract ERP SQL + Portal Excel, transform v7 Star Schema, stage artifacts."""
    df_fin = _extract_source_financials(conn_id=source_conn_id, source_table=source_table)
    df_entities = _read_portal_catalog(portal_catalog_path)
    split_codes = parse_split_codes(split_codes_raw)

    result = transform_financial_portfolio(
        df_fin_raw=df_fin,
        df_entities_raw=df_entities,
        split_codes=split_codes,
    )
    manifest_path = write_staging_bundle(result, staging_dir=staging_dir)
    logger.info(
        "Staged %d star tables and %d QA tables to %s",
        len(result.star_tables),
        len(result.qa_tables),
        manifest_path,
    )
    return {
        "staging_dir": str(staging_dir),
        "manifest_path": str(manifest_path),
        "warnings_count": len(result.warnings),
    }


def run_validate_task(
    staging_dir: str,
    strict_reconciliation: bool = True,
    **context: Any,
) -> dict[str, Any]:
    """Airflow Task 3: Enforce Data Quality Gate circuit breaker before loading DW."""
    result = read_staging_bundle(staging_dir)
    summary = validate_star_schema(
        star_tables=result.star_tables,
        qa_tables=result.qa_tables,
        strict_reconciliation=strict_reconciliation,
    )
    logger.info(
        "Data Quality Gate PASSED: %d project-years reconciled (max variance %.4f).",
        summary["reconciled_project_years"],
        summary["max_reconciliation_variance"],
    )
    return summary


def run_load_task(
    staging_dir: str,
    dw_conn_id: str = "sqlserver_dw",
    dw_schema: str = "dbo",
    **context: Any,
) -> dict[str, int]:
    """Airflow Task 4: Load validated 10 Star Schema + 8 QA Audit tables transactionally into DW."""
    result = read_staging_bundle(staging_dir)
    conn = _get_pymssql_conn(dw_conn_id)
    try:
        loaded = load_tables_transactionally(
            conn=conn,
            tables=result.all_tables(),
            schema=dw_schema,
        )
    finally:
        conn.close()
    return loaded
