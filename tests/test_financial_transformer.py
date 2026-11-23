"""Unit and integration tests for the v7 Kimball Star Schema and QA engine."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.financial_transformer import (
    QA_AUDIT_TABLES,
    STAR_SCHEMA_TABLES,
    convert_to_usd,
    normalize_account_code,
    parse_entity_codes,
    read_staging_bundle,
    strip_accents,
    transform_financial_portfolio,
    write_staging_bundle,
)

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def _load_sample_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    dtypes = {"FS_COD_PART": str, "FS_COD_PART_CORT": str}
    f24 = pd.read_excel(SAMPLE_DIR / "financial_execution_2024.xlsx", dtype=dtypes)
    f24["ArchivoOrigen"] = "financial_execution_2024.xlsx"
    f25 = pd.read_excel(SAMPLE_DIR / "financial_execution_2025.xlsx", dtype=dtypes)
    f25["ArchivoOrigen"] = "financial_execution_2025.xlsx"
    df_fin = pd.concat([f24, f25], ignore_index=True)
    df_ent = pd.read_excel(SAMPLE_DIR / "entities_master_sample.xlsx")
    return df_fin, df_ent


def test_helper_normalizers_and_fx():
    assert strip_accents("GESTIÓN AMBIENTAL Y NIÑO") == "GESTION AMBIENTAL Y NIÑO"
    assert normalize_account_code("42.0") == "042"
    assert normalize_account_code("10101") == "010101"
    assert parse_entity_codes("101, 202 y 900010") == ["101", "202", "900010"]
    assert parse_entity_codes("900-1") == ["900"]
    assert convert_to_usd(1000, "EUR") == 1100.0
    assert convert_to_usd(1000, "CAD") == 720.0
    assert convert_to_usd(340, "PEN") == 100.0
    assert convert_to_usd(500, "USD") == 500.0


def test_full_star_schema_and_qa_reconciliation():
    df_fin, df_ent = _load_sample_inputs()
    result = transform_financial_portfolio(df_fin, df_ent, split_codes=("900",))

    assert tuple(result.star_tables.keys()) == STAR_SCHEMA_TABLES
    assert tuple(result.qa_tables.keys()) == QA_AUDIT_TABLES
    assert result.warnings == []

    # Umbrella code 900 is promoted into donor-level standalone projects 900010 and 900020
    dim_proj = result.star_tables["DimProyecto"]
    assert list(dim_proj["CodigoProyecto"]) == ["101", "202", "900010", "900020"]

    # FactFinanciero has 5 leaf components x 2 fiscal years = 10 rows and 12 monthly columns
    fact_fin = result.star_tables["FactFinanciero"]
    assert len(fact_fin) == 10
    for month in range(1, 13):
        assert f"Ejec_M{month:02d}" in fact_fin.columns

    # QA_Cuadre reconciles split 900010 + 900020 back to 900 with zero variance across all 6 project-years
    qa_cuadre = result.qa_tables["QA_Cuadre"]
    assert len(qa_cuadre) == 6
    assert (qa_cuadre["Resultado"] == "OK").all()
    assert qa_cuadre[["Dif_POA", "Dif_Ejec", "Dif_Comprom", "Dif_Saldo"]].abs().max().max() == 0.0

    # Accent canonicalization unifies GESTION AMBIENTAL (2024) into GESTIÓN AMBIENTAL (2025)
    dim_comp = result.star_tables["DimComponente"]
    comp_101 = dim_comp.loc[dim_comp["CodigoComponenteCompleto"] == "101010001EU0101", "Componente"].iloc[0]
    assert comp_101 == "COMPONENTE 1: GESTIÓN AMBIENTAL"
    assert len(result.qa_tables["QA_NombresLimpieza"]) >= 1

    # Dual FactPOG engine: fixed USD agreement vs cumulative historical execution
    fact_pog = result.star_tables["FactPOG"].merge(
        dim_proj[["IdProyecto", "CodigoProyecto"]], on="IdProyecto"
    ).merge(result.star_tables["DimFecha"][["IdFecha", "Anio"]], on="IdFecha")

    pog_lookup = {
        (row.CodigoProyecto, int(row.Anio)): float(row.POG)
        for row in fact_pog.itertuples(index=False)
    }
    assert pog_lookup[("101", 2024)] == pytest.approx(1200000.0)
    assert pog_lookup[("202", 2024)] == pytest.approx(935000.0)  # 850,000 EUR * 1.10
    assert pog_lookup[("900010", 2024)] == pytest.approx(96000.0)
    assert pog_lookup[("900010", 2025)] == pytest.approx(206400.0)  # 96,000 + 110,400 cumsum
    assert pog_lookup[("900020", 2024)] == pytest.approx(200000.0)  # 680,000 PEN / 3.40

    # Geospatial Ubigeo bridge preserves leading zeros
    dim_ubi = result.star_tables["DimUbicacion"]
    assert len(dim_ubi) == 7
    assert "04" in set(dim_ubi["Ubigeo"])
    assert "08" in set(dim_ubi["Ubigeo"])
    assert len(result.star_tables["BridgeProyectoUbicacion"]) == 7
    assert result.qa_tables["QA_ProyectosSinUbicacion"].empty
    assert result.qa_tables["QA_DepartamentosSinUbigeo"].empty


def test_inactivity_alert_flags_active_zero_execution_project():
    df_fin, df_ent = _load_sample_inputs()
    # Zero out 2025 execution for project 202 while keeping its POA status active
    mask_202_2025 = (df_fin["FI_NUM_ANNO"].astype(int) == 2025) & (
        df_fin["FS_COD_PART"].astype(str).str.startswith("202")
    )
    df_fin.loc[mask_202_2025, "FN_IMP_TOTA_EJEC"] = 0.0

    result = transform_financial_portfolio(df_fin, df_ent, split_codes=("900",))
    dim_proj = result.star_tables["DimProyecto"].set_index("CodigoProyecto")
    assert dim_proj.loc["202", "AlertaSinMovimiento"] == "Sí"
    assert dim_proj.loc["101", "AlertaSinMovimiento"] == "No"


def test_staging_bundle_roundtrip_preserves_types_and_leading_zeros(tmp_path: Path):
    df_fin, df_ent = _load_sample_inputs()
    original = transform_financial_portfolio(df_fin, df_ent, split_codes=("900",))

    staging_dir = tmp_path / "run_20260101"
    manifest_path = write_staging_bundle(original, staging_dir)
    assert manifest_path.exists()

    reloaded = read_staging_bundle(staging_dir)
    assert set(reloaded.star_tables.keys()) == set(original.star_tables.keys())
    assert set(reloaded.qa_tables.keys()) == set(original.qa_tables.keys())

    # Leading zeros in Ubigeo and CodigoProyecto must survive CSV staging intact
    assert list(reloaded.star_tables["DimUbicacion"]["Ubigeo"]) == list(
        original.star_tables["DimUbicacion"]["Ubigeo"]
    )
    assert list(reloaded.star_tables["DimProyecto"]["CodigoProyecto"]) == list(
        original.star_tables["DimProyecto"]["CodigoProyecto"]
    )
    assert pd.api.types.is_datetime64_any_dtype(reloaded.star_tables["DimFecha"]["Fecha"])
    assert reloaded.qa_tables["QA_CodigosRaros"].empty
    assert list(reloaded.qa_tables["QA_CodigosRaros"].columns) == list(
        original.qa_tables["QA_CodigosRaros"].columns
    )
