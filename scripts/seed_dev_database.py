"""Local SQL Server Development Database Initializer and Synthetic Seeder.

Bootstraps the source ERP database and target Data Warehouse database in a local
SQL Server instance and loads synthetic multi-year financial execution ledgers
from data/sample/ using the complete 21-column hierarchical schema consumed by
the Airflow pipeline.

All connection parameters and credentials are supplied via CLI flags or
environment variables so no sensitive values are hardcoded.
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "dags"))

from scripts.dw_loader import sanitize_dataframe_for_pymssql, validate_sql_identifier
from scripts.financial_transformer import REQUIRED_FINANCIAL_COLS


def _connect(server: str, port: int, database: str, user: str | None, password: str | None):
    import pymssql

    kwargs: dict[str, object] = {
        "server": server,
        "port": port,
        "database": database,
    }
    if user and password:
        kwargs["user"] = user
        kwargs["password"] = password
    return pymssql.connect(**kwargs)


def _exec_sql(conn, sql: str, autocommit: bool = True) -> None:
    conn.autocommit(autocommit)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        if not autocommit:
            conn.commit()
    finally:
        cursor.close()


def create_databases(server: str, port: int, user: str | None, password: str | None, source_db: str, dw_db: str) -> None:
    safe_source = validate_sql_identifier(source_db)
    safe_dw = validate_sql_identifier(dw_db)

    conn = _connect(server, port, "master", user, password)
    try:
        for db_name in (safe_source, safe_dw):
            print(f"[1/4] Ensuring database '{db_name}' exists...")
            _exec_sql(
                conn,
                f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}') "
                f"CREATE DATABASE [{db_name}]",
                autocommit=True,
            )
    finally:
        conn.close()


def create_source_table(
    server: str,
    port: int,
    user: str | None,
    password: str | None,
    source_db: str,
    table_name: str,
) -> None:
    safe_table = validate_sql_identifier(table_name)
    conn = _connect(server, port, source_db, user, password)
    try:
        print(f"[2/4] Creating source table dbo.{safe_table} (21-column hierarchical schema)...")
        _exec_sql(
            conn,
            f"IF OBJECT_ID('dbo.{safe_table}', 'U') IS NOT NULL DROP TABLE dbo.[{safe_table}]",
            autocommit=False,
        )
        month_ddl = ",\n            ".join(f"FN_IMP_EJEC_ME{m:02d} FLOAT" for m in range(1, 13))
        create_ddl = f"""
        CREATE TABLE dbo.[{safe_table}] (
            FI_NUM_ANNO      INT,
            FS_COD_PART      NVARCHAR(50),
            FS_COD_PART_CORT NVARCHAR(50),
            FS_DES_PART      NVARCHAR(500),
            FN_IMP_PRAN      FLOAT,
            {month_ddl},
            FN_IMP_TOTA_EJEC FLOAT,
            FN_IMP_COAN      FLOAT,
            FN_IMP_TOTA      FLOAT,
            FN_IMP_SALD      FLOAT
        )
        """
        _exec_sql(conn, create_ddl, autocommit=False)
    finally:
        conn.close()


def load_ledger_files(input_dir: Path) -> pd.DataFrame:
    files = sorted(
        f
        for ext in ("xlsx", "xlsb")
        for f in glob.glob(str(input_dir / f"*.{ext}"))
        if "~$" not in os.path.basename(f)
        and not any(k in os.path.basename(f).lower() for k in ("entidades", "entities"))
    )
    if not files:
        raise FileNotFoundError(f"No financial ledger files (.xlsx or .xlsb) found in '{input_dir}'.")

    frames: list[pd.DataFrame] = []
    dtypes = {"FS_COD_PART": str, "FS_COD_PART_CORT": str}
    for path in files:
        print(f"      Reading {os.path.basename(path)}...")
        if path.lower().endswith(".xlsb"):
            df = pd.read_excel(path, engine="pyxlsb", dtype=dtypes)
        else:
            df = pd.read_excel(path, dtype=dtypes)
        df.columns = df.columns.astype(str).str.strip()
        missing = [c for c in REQUIRED_FINANCIAL_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"File '{os.path.basename(path)}' is missing required columns: {missing}")
        frames.append(df[REQUIRED_FINANCIAL_COLS])

    return pd.concat(frames, ignore_index=True)


def seed_source_table(
    server: str,
    port: int,
    user: str | None,
    password: str | None,
    source_db: str,
    table_name: str,
    input_dir: Path,
    batch_size: int = 500,
) -> int:
    safe_table = validate_sql_identifier(table_name)
    print(f"[3/4] Loading synthetic ledger files from '{input_dir}'...")
    df_all = load_ledger_files(input_dir)
    rows = sanitize_dataframe_for_pymssql(df_all)

    conn = _connect(server, port, source_db, user, password)
    conn.autocommit(False)
    cursor = conn.cursor()
    try:
        col_list = ", ".join(f"[{c}]" for c in REQUIRED_FINANCIAL_COLS)
        placeholders = ", ".join(["%s"] * len(REQUIRED_FINANCIAL_COLS))
        insert_sql = f"INSERT INTO dbo.[{safe_table}] ({col_list}) VALUES ({placeholders})"

        for idx in range(0, len(rows), batch_size):
            cursor.executemany(insert_sql, rows[idx : idx + batch_size])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    print(f"[4/4] Seeded {len(rows)} rows into {source_db}.dbo.{safe_table}.")
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize local SQL Server databases and seed synthetic financial ledgers."
    )
    parser.add_argument("--server", default=os.getenv("SQL_SERVER", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SQL_PORT", "1433")))
    parser.add_argument("--user", default=os.getenv("SQL_ADMIN_USER"))
    parser.add_argument("--password", default=os.getenv("SQL_ADMIN_PASSWORD"))
    parser.add_argument("--source-db", default=os.getenv("SOURCE_DB", "FinancialSourceDB"))
    parser.add_argument("--dw-db", default=os.getenv("DW_DB", "FinancialDW"))
    parser.add_argument("--source-table", default=os.getenv("SOURCE_TABLE_NAME", "FinanzasProyectos"))
    parser.add_argument(
        "--input-dir",
        default=str(REPO_ROOT / "data" / "sample"),
        help="Directory containing synthetic or development .xlsx/.xlsb ledger workbooks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    create_databases(args.server, args.port, args.user, args.password, args.source_db, args.dw_db)
    create_source_table(args.server, args.port, args.user, args.password, args.source_db, args.source_table)
    seed_source_table(
        args.server,
        args.port,
        args.user,
        args.password,
        args.source_db,
        args.source_table,
        Path(args.input_dir),
    )
    print("Development SQL Server bootstrap completed successfully.")


if __name__ == "__main__":
    main()
