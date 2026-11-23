"""Tests for SQL Server DW DDL generation, NaN sanitization, and atomic rollback."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import pytest

from scripts.dw_loader import (
    generate_create_table_sql,
    load_tables_transactionally,
    sanitize_dataframe_for_pymssql,
    validate_sql_identifier,
)


class _FakeCursor:
    def __init__(self, fail_on_table: str | None = None):
        self.executed: list[str] = []
        self.inserted_batches: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.fail_on_table = fail_on_table
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if self.fail_on_table and f"[{self.fail_on_table}]" in sql and sql.startswith("CREATE TABLE"):
            raise RuntimeError(f"Simulated SQL error on {self.fail_on_table}")

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        self.inserted_batches.append((sql, rows))

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, fail_on_table: str | None = None):
        self.autocommit_calls: list[bool] = []
        self.committed = 0
        self.rolled_back = 0
        self.cursor_obj = _FakeCursor(fail_on_table=fail_on_table)

    def autocommit(self, flag: bool) -> None:
        self.autocommit_calls.append(flag)

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1


def test_sanitize_dataframe_converts_nan_and_nat_to_python_none():
    df = pd.DataFrame(
        {
            "IdProyecto": [1, 2],
            "POG": [1000.0, np.nan],
            "Fecha": [pd.Timestamp("2025-01-01"), pd.NaT],
            "Label": ["ALPHA", None],
        }
    )

    rows = sanitize_dataframe_for_pymssql(df)
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][1] == 1000.0
    assert isinstance(rows[0][2], datetime)
    assert rows[1][1] is None
    assert rows[1][2] is None
    assert rows[1][3] is None


def test_generate_create_table_sql_and_identifier_guard():
    df = pd.DataFrame(
        {
            "IdFecha": pd.Series([1], dtype="int64"),
            "Fecha": pd.to_datetime(["2025-01-01"]),
            "POA": pd.Series([150.5], dtype="float64"),
            "Codigo": pd.Series(["101"], dtype="object"),
        }
    )
    ddl = generate_create_table_sql(df, "FactSample", schema="dbo")
    assert ddl == (
        "CREATE TABLE [dbo].[FactSample] "
        "([IdFecha] INT NULL, [Fecha] DATETIME NULL, [POA] FLOAT NULL, [Codigo] NVARCHAR(500) NULL)"
    )

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        validate_sql_identifier("FactFinanciero; DROP TABLE Users")


def test_transactional_load_commits_once_and_creates_empty_qa_tables():
    conn = _FakeConnection()
    tables = {
        "DimFecha": pd.DataFrame({"IdFecha": [1, 2], "Anio": [2024, 2025]}),
        "QA_Duplicados": pd.DataFrame({"Anio": pd.Series(dtype="int64"), "Codigo": pd.Series(dtype="object")}),
    }

    counts = load_tables_transactionally(conn, tables, schema="dbo", batch_size=1)
    assert counts == {"DimFecha": 2, "QA_Duplicados": 0}
    assert conn.autocommit_calls == [False]
    assert conn.committed == 1
    assert conn.rolled_back == 0
    assert conn.cursor_obj.closed is True
    # Empty QA table is still dropped and recreated so downstream SQL/BI queries stay valid
    assert any("CREATE TABLE [dbo].[QA_Duplicados]" in stmt for stmt in conn.cursor_obj.executed)
    # Batch size 1 splits 2 rows into 2 executemany calls
    assert len(conn.cursor_obj.inserted_batches) == 2


def test_transactional_load_rolls_back_completely_on_mid_batch_error():
    conn = _FakeConnection(fail_on_table="FactFinanciero")
    tables = {
        "DimFecha": pd.DataFrame({"IdFecha": [1], "Anio": [2025]}),
        "FactFinanciero": pd.DataFrame({"IdFecha": [1], "POA": [500.0]}),
    }

    with pytest.raises(RuntimeError, match="Simulated SQL error"):
        load_tables_transactionally(conn, tables, schema="dbo")

    assert conn.committed == 0
    assert conn.rolled_back == 1
    assert conn.cursor_obj.closed is True
