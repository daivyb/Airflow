"""Multi-Year Financial Portfolio Dimensional Transformer and QA Engine.

Pure Python transformation layer synchronized with Module 02 of daivyb/BI-Reporting
(02-financial-portfolio-bi). Decoupled from Airflow and SQL Server I/O so that all
Kimball Star Schema builders, linguistic normalization rules, dual POG calculations,
and eight QA reconciliation tables can be unit tested deterministically.

Outputs produced per run:
  Star Schema (10 tables):
    FactFinanciero, FactPOG, DimProyecto, DimDonante, DimEjecutor,
    DimUnidadEjecutora, DimComponente, DimFecha, DimUbicacion,
    BridgeProyectoUbicacion
  QA Audit Suite (8 tables):
    QA_Cuadre, QA_NombresLimpieza, QA_RenombresEntreAnios,
    QA_SegmentosSinFila, QA_CodigosRaros, QA_Duplicados,
    QA_ProyectosSinUbicacion, QA_DepartamentosSinUbigeo
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

LEVEL_BY_LENGTH: dict[int, int] = {3: 1, 6: 2, 9: 3, 13: 4, 15: 5}
MONTH_COLS: list[str] = [f"FN_IMP_EJEC_ME{m:02d}" for m in range(1, 13)]
NUMERIC_COLS: list[str] = (
    ["FN_IMP_PRAN"]
    + MONTH_COLS
    + ["FN_IMP_TOTA_EJEC", "FN_IMP_COAN", "FN_IMP_TOTA", "FN_IMP_SALD"]
)
REQUIRED_FINANCIAL_COLS: list[str] = (
    ["FI_NUM_ANNO", "FS_COD_PART", "FS_COD_PART_CORT", "FS_DES_PART"] + NUMERIC_COLS
)
RECONCILIATION_TOLERANCE: float = 0.01

STAR_SCHEMA_TABLES: tuple[str, ...] = (
    "FactFinanciero",
    "FactPOG",
    "DimProyecto",
    "DimDonante",
    "DimEjecutor",
    "DimUnidadEjecutora",
    "DimComponente",
    "DimFecha",
    "DimUbicacion",
    "BridgeProyectoUbicacion",
)

QA_AUDIT_TABLES: tuple[str, ...] = (
    "QA_Cuadre",
    "QA_NombresLimpieza",
    "QA_RenombresEntreAnios",
    "QA_SegmentosSinFila",
    "QA_CodigosRaros",
    "QA_Duplicados",
    "QA_ProyectosSinUbicacion",
    "QA_DepartamentosSinUbigeo",
)

PERU_DEPARTMENT_UBIGEO: dict[str, str] = {
    "AMAZONAS": "01",
    "ANCASH": "02",
    "APURIMAC": "03",
    "AREQUIPA": "04",
    "AYACUCHO": "05",
    "CAJAMARCA": "06",
    "CALLAO": "07",
    "CUSCO": "08",
    "HUANCAVELICA": "09",
    "HUANUCO": "10",
    "ICA": "11",
    "JUNIN": "12",
    "LA LIBERTAD": "13",
    "LAMBAYEQUE": "14",
    "LIMA": "15",
    "LORETO": "16",
    "MADRE DE DIOS": "17",
    "MOQUEGUA": "18",
    "PASCO": "19",
    "PIURA": "20",
    "PUNO": "21",
    "SAN MARTIN": "22",
    "TACNA": "23",
    "TUMBES": "24",
    "UCAYALI": "25",
}


@dataclass
class TransformationResult:
    """Container for all star schema tables, QA audit tables, and soft warnings."""

    star_tables: dict[str, pd.DataFrame]
    qa_tables: dict[str, pd.DataFrame]
    warnings: list[str] = field(default_factory=list)

    def all_tables(self) -> dict[str, pd.DataFrame]:
        combined: dict[str, pd.DataFrame] = {}
        combined.update(self.star_tables)
        combined.update(self.qa_tables)
        return combined


def strip_accents(text: object) -> object:
    """Removes diacritics while preserving Spanish letter N-tilde."""
    if pd.isna(text):
        return text
    t = str(text).replace("Ñ", "\u0001").replace("ñ", "\u0002")
    t = "".join(
        c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn"
    )
    return t.replace("\u0001", "Ñ").replace("\u0002", "ñ")


def count_accents(text: str) -> int:
    return sum(a != b for a, b in zip(text, str(strip_accents(text))))


def clean_label(text: object) -> str:
    """Normalizes whitespace, stray edge quotes, punctuation, and casing."""
    if pd.isna(text):
        return ""
    t = unicodedata.normalize("NFC", str(text)).replace("\xa0", " ")
    t = t.strip().strip("'\"`´").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+([:;,.)])", r"\1", t)
    t = re.sub(r"\(\s+", "(", t)
    return t.upper()


def build_accent_canonical_map(cleaned_series: pd.Series) -> dict[str, str]:
    """Selects the properly accented canonical form when labels differ only by diacritics."""
    freq = cleaned_series.value_counts()
    mapping: dict[str, str] = {}
    unique_vals = pd.Series(freq.index)
    for _, group in unique_vals.groupby(unique_vals.map(strip_accents)):
        best = max(group, key=lambda v: (count_accents(v), freq[v]))
        for variant in group:
            mapping[variant] = best
    return mapping


def normalize_entity_code(code: object) -> str:
    """Zero-pads entity codes to 3 digits (standard) or 6 digits (split donor projects)."""
    if pd.isna(code):
        return ""
    s = str(code).strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    s = s.split("-")[0].strip()
    if s.isdigit():
        if len(s) <= 3:
            return s.zfill(3)
        if len(s) <= 6:
            return s.zfill(6)
    return s


def parse_entity_codes(code: object) -> list[str]:
    """Parses single, hyphenated, or multi-code entity fields into normalized project codes."""
    if pd.isna(code):
        return []
    raw = str(code).strip()
    if not raw:
        return []
    if "," in raw or re.search(r"\s+y\s+", raw, flags=re.IGNORECASE):
        tokens = re.split(r",|\s+[yY]\s+", raw)
        normalized = [normalize_entity_code(tok) for tok in tokens]
        return [tok for tok in normalized if tok]
    single = normalize_entity_code(raw)
    return [single] if single else []


def normalize_account_code(val: object) -> str:
    """Restores leading zeros stripped by exports on hierarchical budget codes."""
    if pd.isna(val):
        return ""
    s = str(val).strip().upper()
    if "-" in s:
        part0 = s.split("-")[0].strip()
        if part0.isdigit():
            s = part0
    s = re.sub(r"\s+", "", s)
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    if s.isdigit():
        if len(s) <= 3:
            s = s.zfill(3)
        elif len(s) == 5:
            s = s.zfill(6)
        elif len(s) == 8:
            s = s.zfill(9)
    return s


def convert_to_usd(amount: object, currency: object) -> float:
    """Converts donor funding amounts into USD using standardized FX conversion factors."""
    if pd.isna(amount) or amount == "":
        return 0.0
    try:
        val = float(amount)
    except (TypeError, ValueError):
        return 0.0

    curr = str(currency).upper()
    if "EUR" in curr or "EURO" in curr:
        return round(val * 1.10, 2)
    if "CAD" in curr or "CANADI" in curr:
        return round(val * 0.72, 2)
    if "PEN" in curr or "SOL" in curr:
        return round(val / 3.40, 2)
    return round(val * 1.00, 2)


def find_column(pattern: str, columns: Iterable[str]) -> str | None:
    for col in columns:
        if re.search(pattern, str(col), re.IGNORECASE):
            return str(col)
    return None


def latest_name_by_code(df: pd.DataFrame, code_col: str = "Cod") -> pd.DataFrame:
    """Selects one unique record per code using the most recent fiscal year."""
    return (
        df.sort_values("Anio", ascending=False)
        .drop_duplicates(code_col)
        .sort_values(code_col)
        .reset_index(drop=True)
    )


def parse_split_codes(raw_value: str | Iterable[str] | None) -> tuple[str, ...]:
    """Parses comma-separated or iterable 3-digit split project codes."""
    if raw_value is None:
        return ()
    if isinstance(raw_value, str):
        tokens = [tok.strip() for tok in raw_value.split(",")]
    else:
        tokens = [str(tok).strip() for tok in raw_value]
    return tuple(tok.zfill(3) for tok in tokens if tok)


def _prepare_entities(
    df_entities_raw: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes portal entity metadata and geographic attributes."""
    if df_entities_raw is None or df_entities_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    ent_raw = df_entities_raw.copy()
    ent_raw.columns = ent_raw.columns.astype(str).str.strip()

    c_cod = find_column(r"^c[oó]digo|^code", ent_raw.columns) or str(ent_raw.columns[0])
    c_interv = find_column(r"tipo.*intervenci[oó]n|intervention.*type", ent_raw.columns)
    c_area = find_column(r"[aá]rea.*tem[aá]tica|thematic.*area", ent_raw.columns)
    c_rol = find_column(r"rol.*(instituci|organizaci|entidad)|organization.*role", ent_raw.columns)
    c_seg = find_column(r"seguimiento|tracking", ent_raw.columns)
    c_trg = find_column(r"tipo.*recurso.*global|global.*resource", ent_raw.columns)
    c_mon = find_column(r"moneda|currency", ent_raw.columns)
    c_monto = find_column(r"monto.*donante|donor.*amount", ent_raw.columns)
    c_poa = find_column(r"estado.*plan.*operativo|operating.*plan.*status", ent_raw.columns)
    c_depto = find_column(r"departamento|department", ent_raw.columns)

    ent = pd.DataFrame()
    ent["CodigoProyecto"] = ent_raw[c_cod].apply(parse_entity_codes)
    ent["TipoIntervencion"] = (
        ent_raw[c_interv].fillna("Sin Clasificar").astype(str).str.strip()
        if c_interv
        else "Sin Clasificar"
    )
    ent["AreaTematica"] = (
        ent_raw[c_area].fillna("Sin Clasificar").astype(str).str.strip()
        if c_area
        else "Sin Clasificar"
    )
    ent["RolInstitucional"] = (
        ent_raw[c_rol].fillna("Sin Clasificar").astype(str).str.strip()
        if c_rol
        else "Sin Clasificar"
    )
    ent["Seguimiento"] = (
        ent_raw[c_seg].fillna("Sin Clasificar").astype(str).str.strip()
        if c_seg
        else "Sin Clasificar"
    )
    ent["TipoRecursoGlobal"] = (
        ent_raw[c_trg].fillna("Sin Clasificar").astype(str).str.strip()
        if c_trg
        else "Sin Clasificar"
    )
    ent["Moneda"] = (
        ent_raw[c_mon].fillna("No Registrada").astype(str).str.strip()
        if c_mon
        else "No Registrada"
    )
    ent["MontoDonanteOriginal"] = (
        pd.to_numeric(ent_raw[c_monto], errors="coerce").fillna(0.0)
        if c_monto
        else 0.0
    )
    ent["MontoDonanteUSD"] = [
        convert_to_usd(m, curr)
        for m, curr in zip(ent["MontoDonanteOriginal"], ent["Moneda"])
    ]
    ent["EstadoPlanOperativoAnual"] = (
        ent_raw[c_poa].fillna("No Registrado").astype(str).str.strip()
        if c_poa
        else "No Registrado"
    )
    if c_depto:
        ent["Departamentos"] = ent_raw[c_depto].astype(str)

    ent = ent.explode("CodigoProyecto")
    ent["CodigoProyecto"] = ent["CodigoProyecto"].fillna("").astype(str).str.strip()
    ent = ent[ent["CodigoProyecto"].ne("")].copy()

    master_cols = [
        "CodigoProyecto",
        "TipoIntervencion",
        "AreaTematica",
        "RolInstitucional",
        "Seguimiento",
        "TipoRecursoGlobal",
        "Moneda",
        "MontoDonanteOriginal",
        "MontoDonanteUSD",
        "EstadoPlanOperativoAnual",
    ]
    df_master = ent[master_cols].drop_duplicates(subset=["CodigoProyecto"]).reset_index(drop=True)

    df_location = (
        ent[["CodigoProyecto", "Departamentos"]].reset_index(drop=True)
        if "Departamentos" in ent.columns
        else pd.DataFrame()
    )
    return df_master, df_location


def transform_financial_portfolio(
    df_fin_raw: pd.DataFrame,
    df_entities_raw: pd.DataFrame | None = None,
    split_codes: Iterable[str] | str = ("900",),
) -> TransformationResult:
    """Transforms ERP financial ledgers and portal catalog into Kimball Star Schema + QA tables."""
    warnings_log: list[str] = []

    def warn(msg: str) -> None:
        warnings_log.append(msg)

    split_prefixes = parse_split_codes(split_codes)

    def is_split_code(series: pd.Series) -> pd.Series:
        if not split_prefixes:
            return pd.Series(False, index=series.index)
        return series.str.startswith(split_prefixes)

    df_fin = df_fin_raw.copy()
    df_fin.columns = df_fin.columns.astype(str).str.strip()

    missing = [col for col in REQUIRED_FINANCIAL_COLS if col not in df_fin.columns]
    if missing:
        raise ValueError(f"Financial dataset is missing required columns: {missing}")

    if "ArchivoOrigen" not in df_fin.columns:
        df_fin["ArchivoOrigen"] = "sqlserver_source"

    df_entities_master, df_ent_location = _prepare_entities(df_entities_raw)
    if df_entities_master.empty:
        warn("Entity catalog was empty or unavailable; default project attributes applied.")

    # Step 1: Clean fiscal year, account code hierarchy, and numeric columns
    df_fin["Anio"] = pd.to_numeric(df_fin["FI_NUM_ANNO"], errors="coerce")
    df_fin = df_fin[df_fin["Anio"].notna()].copy()
    df_fin["Anio"] = df_fin["Anio"].astype(int)

    df_fin["Cod"] = df_fin["FS_COD_PART"].apply(normalize_account_code)
    df_fin["Longitud"] = df_fin["Cod"].str.len()
    df_fin["Nivel"] = df_fin["Longitud"].map(LEVEL_BY_LENGTH)

    for col in NUMERIC_COLS:
        df_fin[col] = pd.to_numeric(df_fin[col], errors="coerce").fillna(0.0)

    qa_codigos_raros = df_fin[df_fin["Nivel"].isna()][
        ["ArchivoOrigen", "Anio", "FS_COD_PART", "FS_DES_PART"]
    ].reset_index(drop=True)
    if len(qa_codigos_raros):
        warn(f"{len(qa_codigos_raros)} rows with unexpected code length excluded (QA_CodigosRaros).")

    df_fin = df_fin[df_fin["Nivel"].notna()].copy()
    df_fin["Nivel"] = df_fin["Nivel"].astype(int)

    if df_fin.empty:
        raise ValueError("No valid hierarchical rows remain after code length filtering.")

    dup = df_fin[df_fin.duplicated(["Anio", "Cod"], keep=False)]
    qa_duplicados = (
        dup[["ArchivoOrigen", "Anio", "Cod", "FS_DES_PART", "FN_IMP_TOTA_EJEC"]]
        .sort_values(["Anio", "Cod"])
        .reset_index(drop=True)
    )
    if len(qa_duplicados):
        warn(f"{len(qa_duplicados)} duplicate code rows detected within the same year (QA_Duplicados).")

    df_fin["Nombre_Original"] = df_fin["FS_DES_PART"].astype(str).str.strip()
    df_fin["_base"] = df_fin["Nombre_Original"].apply(clean_label)
    accent_map = build_accent_canonical_map(df_fin["_base"])
    df_fin["Nombre"] = df_fin["_base"].map(accent_map)

    years = sorted(int(y) for y in df_fin["Anio"].unique())

    # Step 2: Build Kimball dimensions
    n1 = df_fin[df_fin["Nivel"] == 1].copy()
    n2 = df_fin[df_fin["Nivel"] == 2].copy()
    n5 = df_fin[df_fin["Nivel"] == 5].copy()

    n1_regular = n1[~n1["Cod"].isin(split_prefixes)].copy()
    project_frames: list[pd.DataFrame] = []

    if len(n1_regular):
        p_regular = latest_name_by_code(n1_regular)[["Cod", "Nombre", "Nombre_Original"]].rename(
            columns={"Cod": "CodigoProyecto", "Nombre": "Proyecto", "Nombre_Original": "Proyecto_Original"}
        )
        project_frames.append(p_regular)

    split_in_n2 = n2[is_split_code(n2["Cod"])].copy()
    if len(split_in_n2):
        p_split = latest_name_by_code(split_in_n2)[["Cod", "Nombre", "Nombre_Original"]].rename(
            columns={"Cod": "CodigoProyecto", "Nombre": "Proyecto", "Nombre_Original": "Proyecto_Original"}
        )
        project_frames.append(p_split)
    else:
        split_in_n5 = n5[is_split_code(n5["Cod"])].copy()
        if len(split_in_n5):
            split_in_n5["Cod6"] = split_in_n5["Cod"].str[:6]
            p_split = latest_name_by_code(split_in_n5, "Cod6")[
                ["Cod6", "Nombre", "Nombre_Original"]
            ].rename(
                columns={"Cod6": "CodigoProyecto", "Nombre": "Proyecto", "Nombre_Original": "Proyecto_Original"}
            )
            project_frames.append(p_split)

    if project_frames:
        dim_proyecto = (
            pd.concat(project_frames, ignore_index=True)
            .sort_values("CodigoProyecto")
            .reset_index(drop=True)
        )
    else:
        dim_proyecto = pd.DataFrame(columns=["CodigoProyecto", "Proyecto", "Proyecto_Original"])

    default_attrs: dict[str, object] = {
        "TipoIntervencion": "Sin Clasificar",
        "AreaTematica": "Sin Clasificar",
        "RolInstitucional": "Sin Clasificar",
        "Seguimiento": "Sin Clasificar",
        "TipoRecursoGlobal": "Sin Clasificar",
        "Moneda": "No Registrada",
        "MontoDonanteOriginal": 0.0,
        "MontoDonanteUSD": 0.0,
        "EstadoPlanOperativoAnual": "No Registrado",
    }

    if not df_entities_master.empty:
        dim_proyecto = dim_proyecto.merge(df_entities_master, on="CodigoProyecto", how="left")
        for col, default_val in default_attrs.items():
            dim_proyecto[col] = dim_proyecto[col].fillna(default_val)
    else:
        for col, default_val in default_attrs.items():
            dim_proyecto[col] = default_val

    latest_year = max(years)
    reg_active = n1[
        (n1["Anio"] == latest_year)
        & (~n1["Cod"].isin(split_prefixes))
        & (n1["FN_IMP_TOTA_EJEC"].abs() > 0)
    ]["Cod"]
    split_active = n5[
        (n5["Anio"] == latest_year)
        & is_split_code(n5["Cod"])
        & (n5["FN_IMP_TOTA_EJEC"].abs() > 0)
    ]["Cod"].str[:6]
    active_projects = set(reg_active).union(set(split_active))

    dim_proyecto["AlertaSinMovimiento"] = np.where(
        (
            dim_proyecto["EstadoPlanOperativoAnual"].str.contains(
                r"^S[ií]|con ejecuci[oó]n", case=False, na=False
            )
        )
        & (~dim_proyecto["CodigoProyecto"].isin(active_projects)),
        "Sí",
        "No",
    )
    dim_proyecto.insert(0, "IdProyecto", range(1, len(dim_proyecto) + 1))

    dim_proyecto = dim_proyecto[
        [
            "IdProyecto",
            "CodigoProyecto",
            "Proyecto",
            "Proyecto_Original",
            "EstadoPlanOperativoAnual",
            "AlertaSinMovimiento",
            "TipoIntervencion",
            "AreaTematica",
            "RolInstitucional",
            "Seguimiento",
            "TipoRecursoGlobal",
            "Moneda",
            "MontoDonanteOriginal",
            "MontoDonanteUSD",
        ]
    ]

    def build_segment_dim(
        level: int,
        start: int,
        end: int,
        col_code: str,
        col_name: str,
        col_id: str,
    ) -> pd.DataFrame:
        subset = df_fin[df_fin["Nivel"] == level].copy()
        subset["Seg"] = subset["Cod"].str[start:end]
        uniq = latest_name_by_code(subset, "Seg")[["Seg", "Nombre", "Nombre_Original"]]
        uniq.columns = [col_code, col_name, col_name + "_Original"]
        uniq.insert(0, col_id, range(1, len(uniq) + 1))
        return uniq

    dim_donante = build_segment_dim(2, 3, 6, "CodigoDonante", "Donante", "IdDonante")
    dim_ejecutor = build_segment_dim(3, 6, 9, "CodigoEjecutor", "Ejecutor", "IdEjecutor")
    dim_unidad = build_segment_dim(4, 9, 13, "CodigoUnidadEjecutora", "UnidadEjecutora", "IdUnidadEjecutora")

    dim_comp_src = latest_name_by_code(n5)
    dim_comp = pd.DataFrame(
        {
            "CodigoComponenteCompleto": dim_comp_src["Cod"],
            "CodigoComponente": dim_comp_src["Cod"].str[13:15],
            "Componente": dim_comp_src["Nombre"],
            "Componente_Original": dim_comp_src["Nombre_Original"],
        }
    )
    dim_comp.insert(0, "IdComponente", range(1, len(dim_comp) + 1))

    dim_fecha = pd.DataFrame({"Anio": years})
    dim_fecha["Fecha"] = pd.to_datetime(dict(year=dim_fecha["Anio"], month=1, day=1))
    dim_fecha.insert(0, "IdFecha", range(1, len(dim_fecha) + 1))
    dim_fecha = dim_fecha[["IdFecha", "Fecha", "Anio"]]

    # Step 3: Build FactFinanciero and FactPOG
    fact = n5.copy()
    fact["CodigoProyecto"] = np.where(is_split_code(fact["Cod"]), fact["Cod"].str[:6], fact["Cod"].str[:3])
    fact["CodigoDonante"] = fact["Cod"].str[3:6]
    fact["CodigoEjecutor"] = fact["Cod"].str[6:9]
    fact["CodigoUnidadEjecutora"] = fact["Cod"].str[9:13]

    fact["IdProyecto"] = fact["CodigoProyecto"].map(dict(zip(dim_proyecto.CodigoProyecto, dim_proyecto.IdProyecto)))
    fact["IdDonante"] = fact["CodigoDonante"].map(dict(zip(dim_donante.CodigoDonante, dim_donante.IdDonante)))
    fact["IdEjecutor"] = fact["CodigoEjecutor"].map(dict(zip(dim_ejecutor.CodigoEjecutor, dim_ejecutor.IdEjecutor)))
    fact["IdUnidadEjecutora"] = fact["CodigoUnidadEjecutora"].map(
        dict(zip(dim_unidad.CodigoUnidadEjecutora, dim_unidad.IdUnidadEjecutora))
    )
    fact["IdComponente"] = fact["Cod"].map(dict(zip(dim_comp.CodigoComponenteCompleto, dim_comp.IdComponente)))
    fact["IdFecha"] = fact["Anio"].map(dict(zip(dim_fecha.Anio, dim_fecha.IdFecha)))

    fact_renames = {
        "FN_IMP_PRAN": "POA",
        "FN_IMP_TOTA_EJEC": "EjecutadoAnual",
        "FN_IMP_COAN": "Comprometido",
        "FN_IMP_TOTA": "Total",
        "FN_IMP_SALD": "Saldo",
    }
    fact_renames.update({f"FN_IMP_EJEC_ME{m:02d}": f"Ejec_M{m:02d}" for m in range(1, 13)})

    fact_finanzas = (
        fact.sort_values(["Anio", "IdComponente"])
        .rename(columns=fact_renames)[
            ["IdFecha", "IdProyecto", "IdDonante", "IdEjecutor", "IdUnidadEjecutora", "IdComponente", "POA"]
            + [f"Ejec_M{m:02d}" for m in range(1, 13)]
            + ["EjecutadoAnual", "Comprometido", "Total", "Saldo"]
        ]
        .reset_index(drop=True)
    )

    pog = fact.groupby(["CodigoProyecto", "Anio"], as_index=False)["FN_IMP_TOTA_EJEC"].sum()
    pog = pog.sort_values(["CodigoProyecto", "Anio"]).reset_index(drop=True)
    pog = pog.merge(
        dim_proyecto[["CodigoProyecto", "IdProyecto", "TipoRecursoGlobal", "MontoDonanteUSD"]],
        on="CodigoProyecto",
        how="left",
    )
    pog["POG"] = np.nan
    mask_agreement = pog["TipoRecursoGlobal"].astype(str).str.contains("Convenio|Agreement", case=False, na=False)
    pog.loc[mask_agreement, "POG"] = pog.loc[mask_agreement, "MontoDonanteUSD"]
    mask_cumulative = pog["TipoRecursoGlobal"].astype(str).str.contains("Acumulad|Cumulative", case=False, na=False)
    pog.loc[mask_cumulative, "POG"] = pog.loc[mask_cumulative].groupby("CodigoProyecto")["FN_IMP_TOTA_EJEC"].cumsum()
    pog["POG"] = pog["POG"].fillna(0.0)
    pog["IdFecha"] = pog["Anio"].map(dict(zip(dim_fecha.Anio, dim_fecha.IdFecha)))
    fact_pog = (
        pog[["IdFecha", "IdProyecto", "TipoRecursoGlobal", "POG"]]
        .sort_values(["IdFecha", "IdProyecto"])
        .reset_index(drop=True)
    )

    # Step 4: Build DimUbicacion and BridgeProyectoUbicacion
    dim_ubicacion = pd.DataFrame(columns=["IdUbicacion", "Departamento", "Ubigeo"])
    bridge_proyecto_ubicacion = pd.DataFrame(columns=["IdProyecto", "IdUbicacion"])
    qa_sin_ubicacion = dim_proyecto[["IdProyecto", "CodigoProyecto", "Proyecto"]].copy()
    qa_depto_sin_ubigeo = pd.DataFrame(columns=["Departamento"])

    if not df_ent_location.empty:
        df_ub = df_ent_location.copy()
        df_ub["Departamento"] = df_ub["Departamentos"].astype(str).str.upper().str.split(";")
        df_ub = df_ub.explode("Departamento")
        df_ub["Departamento"] = df_ub["Departamento"].str.strip().apply(strip_accents)
        df_ub = df_ub[
            df_ub["Departamento"].notna()
            & df_ub["Departamento"].ne("")
            & df_ub["Departamento"].ne("NAN")
        ]

        dim_ubicacion = df_ub[["Departamento"]].drop_duplicates().reset_index(drop=True)
        dim_ubicacion["Ubigeo"] = dim_ubicacion["Departamento"].map(PERU_DEPARTMENT_UBIGEO)
        qa_depto_sin_ubigeo = (
            dim_ubicacion[dim_ubicacion["Ubigeo"].isna()][["Departamento"]].reset_index(drop=True)
        )
        dim_ubicacion = (
            dim_ubicacion.dropna(subset=["Ubigeo"]).sort_values("Ubigeo").reset_index(drop=True)
        )
        dim_ubicacion.insert(0, "IdUbicacion", range(1, len(dim_ubicacion) + 1))

        br = df_ub[["CodigoProyecto", "Departamento"]].merge(
            dim_proyecto[["CodigoProyecto", "IdProyecto"]], on="CodigoProyecto", how="inner"
        )
        br["Ubigeo"] = br["Departamento"].map(PERU_DEPARTMENT_UBIGEO)
        br = br.merge(dim_ubicacion[["Ubigeo", "IdUbicacion"]], on="Ubigeo", how="inner")
        bridge_proyecto_ubicacion = (
            br[["IdProyecto", "IdUbicacion"]]
            .drop_duplicates()
            .sort_values(["IdProyecto", "IdUbicacion"])
            .reset_index(drop=True)
        )
        located_ids = set(bridge_proyecto_ubicacion["IdProyecto"])
        qa_sin_ubicacion = dim_proyecto[~dim_proyecto["IdProyecto"].isin(located_ids)][
            ["IdProyecto", "CodigoProyecto", "Proyecto"]
        ].reset_index(drop=True)
        if len(qa_depto_sin_ubigeo):
            warn(f"{len(qa_depto_sin_ubigeo)} unmapped department names (QA_DepartamentosSinUbigeo).")

    # Step 5: Build eight-table automated QA reconciliation suite
    metrics = [
        ("POA", "FN_IMP_PRAN"),
        ("Ejec", "FN_IMP_TOTA_EJEC"),
        ("Comprom", "FN_IMP_COAN"),
        ("Saldo", "FN_IMP_SALD"),
    ]
    a_n1 = n1.groupby(["Anio", "Cod"], as_index=False)[[m[1] for m in metrics]].sum()
    b_fact = fact.copy()
    b_fact["CodComparacion"] = b_fact["Cod"].str[:3]
    b_fact = b_fact.groupby(["Anio", "CodComparacion"], as_index=False)[[m[1] for m in metrics]].sum()
    b_fact = b_fact.rename(columns={"CodComparacion": "Cod"})

    reconcile = a_n1.merge(b_fact, on=["Anio", "Cod"], how="outer", suffixes=("_N1", "_Fact")).fillna(0.0)
    n1_names = latest_name_by_code(n1)[["Cod", "Nombre"]].rename(columns={"Nombre": "Proyecto"})
    reconcile = reconcile.merge(n1_names, on="Cod", how="left")
    qa_cuadre = reconcile[["Anio", "Cod", "Proyecto"]].rename(columns={"Cod": "CodigoProyecto"}).copy()

    diff_cols: list[str] = []
    for label, raw_col in metrics:
        qa_cuadre[f"{label}_Nivel1"] = reconcile[raw_col + "_N1"].astype(float).values
        qa_cuadre[f"{label}_Fact"] = reconcile[raw_col + "_Fact"].astype(float).values
        qa_cuadre[f"Dif_{label}"] = (
            (reconcile[raw_col + "_Fact"] - reconcile[raw_col + "_N1"]).round(2).astype(float).values
        )
        diff_cols.append(f"Dif_{label}")

    qa_cuadre["Resultado"] = np.where(
        qa_cuadre[diff_cols].abs().max(axis=1) > RECONCILIATION_TOLERANCE, "REVISAR", "OK"
    )
    qa_cuadre = qa_cuadre.sort_values(["Anio", "CodigoProyecto"]).reset_index(drop=True)

    failed_checks = int((qa_cuadre["Resultado"] == "REVISAR").sum())
    if failed_checks:
        warn(f"{failed_checks} project-year combinations failed Level 1 vs Component reconciliation.")

    qa_nombres_limpieza = (
        df_fin[df_fin["Nombre_Original"] != df_fin["Nombre"]][["Nivel", "Cod", "Nombre_Original", "Nombre"]]
        .drop_duplicates("Nombre_Original")
        .rename(columns={"Cod": "Codigo", "Nombre": "Nombre_Limpio"})
        .sort_values(["Nivel", "Codigo"])
        .reset_index(drop=True)
    )

    renames_grp = df_fin.groupby(["Nivel", "Cod"])["Nombre"].agg(lambda v: sorted(set(v)))
    renames_grp = renames_grp[renames_grp.map(len) > 1].reset_index()
    renames_grp["Nombres"] = renames_grp["Nombre"].map(lambda v: " | ".join(v))
    qa_renombres = (
        renames_grp[["Nivel", "Cod", "Nombres"]]
        .rename(columns={"Cod": "Codigo"})
        .reset_index(drop=True)
    )

    comp_codes = dim_comp["CodigoComponenteCompleto"]
    qa_segments: list[dict[str, str]] = []
    for col, dim_df, level_name, start, end in [
        ("CodigoProyecto", dim_proyecto, "Proyecto", 0, 3),
        ("CodigoDonante", dim_donante, "Donante", 3, 6),
        ("CodigoEjecutor", dim_ejecutor, "Ejecutor", 6, 9),
        ("CodigoUnidadEjecutora", dim_unidad, "UnidadEjecutora", 9, 13),
    ]:
        if level_name == "Proyecto":
            expected = set(np.where(is_split_code(comp_codes), comp_codes.str[:6], comp_codes.str[:3]))
        else:
            expected = set(comp_codes.str[start:end])
        missing_seg = sorted(expected - set(dim_df[col]))
        for code in missing_seg:
            qa_segments.append({"Nivel": level_name, "Codigo": code})
    qa_segmentos = pd.DataFrame(qa_segments, columns=["Nivel", "Codigo"])

    star_tables = {
        "FactFinanciero": fact_finanzas,
        "FactPOG": fact_pog,
        "DimProyecto": dim_proyecto,
        "DimDonante": dim_donante,
        "DimEjecutor": dim_ejecutor,
        "DimUnidadEjecutora": dim_unidad,
        "DimComponente": dim_comp,
        "DimFecha": dim_fecha,
        "DimUbicacion": dim_ubicacion,
        "BridgeProyectoUbicacion": bridge_proyecto_ubicacion,
    }
    qa_tables = {
        "QA_Cuadre": qa_cuadre,
        "QA_NombresLimpieza": qa_nombres_limpieza,
        "QA_RenombresEntreAnios": qa_renombres,
        "QA_SegmentosSinFila": qa_segmentos,
        "QA_CodigosRaros": qa_codigos_raros,
        "QA_Duplicados": qa_duplicados,
        "QA_ProyectosSinUbicacion": qa_sin_ubicacion,
        "QA_DepartamentosSinUbigeo": qa_depto_sin_ubigeo,
    }

    return TransformationResult(star_tables=star_tables, qa_tables=qa_tables, warnings=warnings_log)


def _classify_dtype(dtype: object) -> str:
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    return "str"


def write_staging_bundle(result: TransformationResult, staging_dir: str | Path) -> Path:
    """Writes staged CSV files plus a typed manifest.json for lossless inter-task handoff."""
    base = Path(staging_dir)
    base.mkdir(parents=True, exist_ok=True)

    schemas: dict[str, dict[str, str]] = {}
    for table_name, df in result.all_tables().items():
        schemas[table_name] = {col: _classify_dtype(dtype) for col, dtype in df.dtypes.items()}
        df.to_csv(base / f"{table_name}.csv", index=False, encoding="utf-8")

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "star_tables": list(result.star_tables.keys()),
        "qa_tables": list(result.qa_tables.keys()),
        "warnings": list(result.warnings),
        "schemas": schemas,
    }
    manifest_path = base / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def _read_typed_csv(csv_path: Path, col_schema: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8")
    if df.empty:
        empty_cols: dict[str, pd.Series] = {}
        for col, kind in col_schema.items():
            if kind == "datetime":
                empty_cols[col] = pd.Series(dtype="datetime64[ns]")
            elif kind == "int":
                empty_cols[col] = pd.Series(dtype="int64")
            elif kind == "float":
                empty_cols[col] = pd.Series(dtype="float64")
            else:
                empty_cols[col] = pd.Series(dtype="object")
        return pd.DataFrame(empty_cols)

    for col, kind in col_schema.items():
        if col not in df.columns:
            continue
        if kind == "datetime":
            df[col] = pd.to_datetime(df[col].replace("", pd.NA), errors="coerce")
        elif kind == "int":
            numeric = pd.to_numeric(df[col].replace("", pd.NA), errors="coerce")
            df[col] = numeric.astype("int64") if numeric.notna().all() else numeric
        elif kind == "float":
            df[col] = pd.to_numeric(df[col].replace("", pd.NA), errors="coerce").astype("float64")
        else:
            df[col] = df[col].astype(object)
    return df


def read_staging_bundle(staging_dir: str | Path) -> TransformationResult:
    """Reloads a typed staging bundle with zero loss of leading zeros or datetime dtypes."""
    base = Path(staging_dir)
    manifest_path = base / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Staging manifest not found at '{manifest_path}'.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schemas: dict[str, dict[str, str]] = manifest["schemas"]

    star_tables = {
        name: _read_typed_csv(base / f"{name}.csv", schemas[name])
        for name in manifest["star_tables"]
    }
    qa_tables = {
        name: _read_typed_csv(base / f"{name}.csv", schemas[name])
        for name in manifest["qa_tables"]
    }
    return TransformationResult(
        star_tables=star_tables,
        qa_tables=qa_tables,
        warnings=list(manifest.get("warnings", [])),
    )
