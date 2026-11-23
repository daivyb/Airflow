"""Tests for the Data Quality Gate circuit breaker."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_quality import DataQualityError, validate_star_schema
from scripts.financial_transformer import transform_financial_portfolio

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def _build_valid_result():
    dtypes = {"FS_COD_PART": str, "FS_COD_PART_CORT": str}
    f24 = pd.read_excel(SAMPLE_DIR / "financial_execution_2024.xlsx", dtype=dtypes)
    f25 = pd.read_excel(SAMPLE_DIR / "financial_execution_2025.xlsx", dtype=dtypes)
    df_fin = pd.concat([f24, f25], ignore_index=True)
    df_ent = pd.read_excel(SAMPLE_DIR / "entities_master_sample.xlsx")
    return transform_financial_portfolio(df_fin, df_ent, split_codes=("900",))


def test_data_quality_gate_passes_on_reconciled_sample():
    result = _build_valid_result()
    summary = validate_star_schema(result.star_tables, result.qa_tables)

    assert summary["status"] == "PASSED"
    assert summary["reconciled_project_years"] == 6
    assert summary["failed_reconciliations"] == 0
    assert summary["max_reconciliation_variance"] == 0.0
    assert summary["row_counts"]["FactFinanciero"] == 10


def test_data_quality_gate_blocks_reconciliation_drift():
    result = _build_valid_result()
    result.qa_tables["QA_Cuadre"].loc[0, "Dif_Ejec"] = 250.50
    result.qa_tables["QA_Cuadre"].loc[0, "Resultado"] = "REVISAR"

    with pytest.raises(DataQualityError, match="Accounting reconciliation failed"):
        validate_star_schema(result.star_tables, result.qa_tables, strict_reconciliation=True)


def test_data_quality_gate_blocks_duplicate_primary_keys():
    result = _build_valid_result()
    dim_proj = result.star_tables["DimProyecto"]
    result.star_tables["DimProyecto"] = pd.concat([dim_proj, dim_proj.iloc[[0]]], ignore_index=True)

    with pytest.raises(DataQualityError, match="duplicate primary keys"):
        validate_star_schema(result.star_tables, result.qa_tables)


def test_data_quality_gate_blocks_orphan_foreign_keys():
    result = _build_valid_result()
    result.star_tables["FactFinanciero"].loc[0, "IdProyecto"] = 99999

    with pytest.raises(DataQualityError, match="orphan keys"):
        validate_star_schema(result.star_tables, result.qa_tables)


def test_data_quality_gate_blocks_empty_fact_table():
    result = _build_valid_result()
    result.star_tables["FactFinanciero"] = result.star_tables["FactFinanciero"].iloc[0:0]

    with pytest.raises(DataQualityError, match="Core table 'FactFinanciero' is empty"):
        validate_star_schema(result.star_tables, result.qa_tables)
