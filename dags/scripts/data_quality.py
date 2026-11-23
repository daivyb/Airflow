"""Automated Data Quality Gate (circuit breaker) for the Financial DW pipeline.

Validates staged Kimball Star Schema and QA audit artifacts before any write
operation touches the production SQL Server Data Warehouse. Raises
DataQualityError on hard rule violations so Airflow halts the DAG run and
preserves the last known good state consumed by Power BI.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from scripts.financial_transformer import (
    QA_AUDIT_TABLES,
    RECONCILIATION_TOLERANCE,
    STAR_SCHEMA_TABLES,
)


class DataQualityError(RuntimeError):
    """Raised when staged dimensional outputs violate mandatory DW integrity checks."""


DIMENSION_PRIMARY_KEYS: dict[str, str] = {
    "DimProyecto": "IdProyecto",
    "DimDonante": "IdDonante",
    "DimEjecutor": "IdEjecutor",
    "DimUnidadEjecutora": "IdUnidadEjecutora",
    "DimComponente": "IdComponente",
    "DimFecha": "IdFecha",
    "DimUbicacion": "IdUbicacion",
}

CORE_NON_EMPTY_TABLES: tuple[str, ...] = (
    "FactFinanciero",
    "FactPOG",
    "DimProyecto",
    "DimDonante",
    "DimEjecutor",
    "DimUnidadEjecutora",
    "DimComponente",
    "DimFecha",
)

FOREIGN_KEY_RULES: tuple[tuple[str, str, str, str], ...] = (
    ("FactFinanciero", "IdFecha", "DimFecha", "IdFecha"),
    ("FactFinanciero", "IdProyecto", "DimProyecto", "IdProyecto"),
    ("FactFinanciero", "IdDonante", "DimDonante", "IdDonante"),
    ("FactFinanciero", "IdEjecutor", "DimEjecutor", "IdEjecutor"),
    ("FactFinanciero", "IdUnidadEjecutora", "DimUnidadEjecutora", "IdUnidadEjecutora"),
    ("FactFinanciero", "IdComponente", "DimComponente", "IdComponente"),
    ("FactPOG", "IdFecha", "DimFecha", "IdFecha"),
    ("FactPOG", "IdProyecto", "DimProyecto", "IdProyecto"),
    ("BridgeProyectoUbicacion", "IdProyecto", "DimProyecto", "IdProyecto"),
    ("BridgeProyectoUbicacion", "IdUbicacion", "DimUbicacion", "IdUbicacion"),
)


def validate_star_schema(
    star_tables: dict[str, pd.DataFrame],
    qa_tables: dict[str, pd.DataFrame],
    strict_reconciliation: bool = True,
) -> dict[str, Any]:
    """Runs schema, PK uniqueness, FK referential, and accounting reconciliation gates."""
    errors: list[str] = []

    # 1. Verify all expected star schema and QA audit tables exist
    missing_star = [t for t in STAR_SCHEMA_TABLES if t not in star_tables]
    missing_qa = [t for t in QA_AUDIT_TABLES if t not in qa_tables]
    if missing_star:
        errors.append(f"Missing required star schema tables: {missing_star}")
    if missing_qa:
        errors.append(f"Missing required QA audit tables: {missing_qa}")
    if errors:
        raise DataQualityError("; ".join(errors))

    # 2. Verify core tables are non-empty
    for table_name in CORE_NON_EMPTY_TABLES:
        if star_tables[table_name].empty:
            errors.append(f"Core table '{table_name}' is empty.")

    # 3. Verify primary key non-nullability and uniqueness across all dimensions
    for dim_name, pk_col in DIMENSION_PRIMARY_KEYS.items():
        dim_df = star_tables[dim_name]
        if pk_col not in dim_df.columns:
            errors.append(f"Dimension '{dim_name}' is missing primary key column '{pk_col}'.")
            continue
        if dim_df[pk_col].isna().any():
            errors.append(f"Dimension '{dim_name}' contains null primary keys in '{pk_col}'.")
        if dim_df[pk_col].duplicated().any():
            errors.append(f"Dimension '{dim_name}' contains duplicate primary keys in '{pk_col}'.")

    dim_proj = star_tables["DimProyecto"]
    if not dim_proj.empty and dim_proj["CodigoProyecto"].duplicated().any():
        errors.append("Dimension 'DimProyecto' contains duplicate CodigoProyecto business keys.")

    # 4. Verify 100% foreign key referential integrity
    for child_table, fk_col, parent_table, pk_col in FOREIGN_KEY_RULES:
        child_df = star_tables[child_table]
        parent_df = star_tables[parent_table]
        if child_df.empty:
            continue
        if child_df[fk_col].isna().any():
            errors.append(
                f"Table '{child_table}' has {int(child_df[fk_col].isna().sum())} null foreign keys in '{fk_col}'."
            )
            continue
        valid_keys = set(parent_df[pk_col].dropna())
        orphan_mask = ~child_df[fk_col].isin(valid_keys)
        if orphan_mask.any():
            errors.append(
                f"Table '{child_table}.{fk_col}' has {int(orphan_mask.sum())} orphan keys missing from '{parent_table}.{pk_col}'."
            )

    # 5. Enforce Level 1 vs Level 5 accounting reconciliation gate (QA_Cuadre)
    qa_cuadre = qa_tables["QA_Cuadre"]
    failed_reconciliations = 0
    max_abs_diff = 0.0
    if qa_cuadre.empty:
        errors.append("QA_Cuadre reconciliation table is empty.")
    else:
        failed_reconciliations = int((qa_cuadre["Resultado"] == "REVISAR").sum())
        diff_cols = [c for c in qa_cuadre.columns if c.startswith("Dif_")]
        if diff_cols:
            max_abs_diff = float(qa_cuadre[diff_cols].abs().max().max())
        if strict_reconciliation and failed_reconciliations > 0:
            errors.append(
                f"Accounting reconciliation failed for {failed_reconciliations} project-year rows "
                f"(max abs variance={max_abs_diff:.2f}, tolerance={RECONCILIATION_TOLERANCE:.2f})."
            )

    if errors:
        raise DataQualityError(" | ".join(errors))

    return {
        "status": "PASSED",
        "row_counts": {name: int(len(df)) for name, df in star_tables.items()},
        "qa_counts": {name: int(len(df)) for name, df in qa_tables.items()},
        "reconciled_project_years": int(len(qa_cuadre)),
        "failed_reconciliations": failed_reconciliations,
        "max_reconciliation_variance": round(max_abs_diff, 4),
    }
