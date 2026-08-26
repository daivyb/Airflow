"""
etl_finanzas.py
===============
Refactorización del script original v5_ET.py para ejecutarse como
PythonOperator dentro de Apache Airflow.

Cambios principales respecto al original:
  - Lectura de finanzas: SQL Server vía pymssql (en lugar de .xlsb)
  - Lectura de entidades: /opt/airflow/data/reporte_completo.xlsx
    (generado por el DockerOperator de Playwright)
  - Escritura: df.to_sql() hacia el Data Warehouse en SQL Server
  - Estructura: función ejecutar_transformacion() invocable por PythonOperator
"""

import logging
import unicodedata

import numpy as np
import pandas as pd
import pymssql
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

# =========================================================
# UTILIDADES  (conservadas del original v5_ET.py)
# =========================================================


def quitar_tildes(texto):
    if pd.isna(texto):
        return texto
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    )


def normalizar_codigo_proyecto(codigo):
    if pd.isna(codigo):
        return []

    codigo = str(codigo).strip()

    # Caso 045-1, 045-2...
    if "-" in codigo:
        return [codigo.split("-")[0].strip().zfill(3)]

    # Caso múltiple: 137, 140 y 157
    if "," in codigo or " Y " in codigo.upper():
        codigo = codigo.upper().replace(" Y ", ",")
        return [
            c.strip().zfill(3)
            for c in codigo.split(",")
            if c.strip()
        ]

    return [codigo.zfill(3)]


# =========================================================
# CONEXIONES
# =========================================================


def _get_pymssql_conn(conn_id: str):
    """Obtiene una conexión pymssql a partir de un Airflow Connection ID."""
    conn = BaseHook.get_connection(conn_id)
    return pymssql.connect(
        server=conn.host,
        port=int(conn.port or 1433),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
    )


# =========================================================
# MAPAS DE NEGOCIO  (conservados del original v5_ET.py)
# =========================================================

MAP_POG = {
    "024": 206834.48,
    "062": 9162000.00,
    "066": 11000000.00,
    "067": 192696.37,
    "074": 9027778.00,
    "084": 920000.00,
    "090": 24131175.00,
    "094": 7136627.96,
    "097": 2288405.38,
    "101": 44451.38,
    "107": 120000.00,
    "113": 632080.52,
    "125": 252123.51,
    "126": 3634.05,
    "128": 1796320.00,
    "130": 15599083.00,
    "131": 7735493.56,
    "133": 400000.00,
    "135": 10000000.00,
    "136": 1798910.00,
    "137": 640419.30,
    "140": 929492.26,
    "142": 1880894.73,
    "144": 1212410.00,
    "145": 10717792.00,
    "147": 150000.00,
    "151": 32904.93,
    "152": 598341.00,
    "154": 3387638.00,
    "155": 580025.00,
    "156": 19577480.87,
    "157": 767478.22,
    "158": 52500.00,
    "160": 58563.92,
    "161": 24392500.00,
    "162": 186273.18,
    "164": 5000000.00,
    "165": 1375000.00,
    "168": 1139542.33,
    "169": 1141355.69,
    "170": 393748.57,
    "172": 4746620.00,
    "173": 634053.43,
    "175": 734620.13,
    "177": 850000.00,
    "178": 1085000.00,
    "179": 99000.00,
    "182": 2000000.00,
    "183": 450000.00,
    "184": 250000.00,
    "185": 280353.53,
    "186": 113973.27,
    "188": 150002.00,
    "191": 134464.60,
    "193": 300000.00,
}

POG_ACUMULADO = {
    "015", "045", "064", "069", "089",
    "099", "056",
    "115", "116", "118", "132", "134",
    "139", "159", "163", "167", "174",
    "187",
}

MAP_RESPONSABILIDAD = {
    # Responsabilidad Técnica
    "107": "Responsabilidad Técnica",
    "130": "Responsabilidad Técnica",
    "131": "Responsabilidad Técnica",
    "133": "Responsabilidad Técnica",
    "135": "Responsabilidad Técnica",
    "136": "Responsabilidad Técnica",
    "144": "Responsabilidad Técnica",
    "145": "Responsabilidad Técnica",
    "154": "Responsabilidad Técnica",
    "156": "Responsabilidad Técnica",
    "161": "Responsabilidad Técnica",
    "164": "Responsabilidad Técnica",
    "165": "Responsabilidad Técnica",
    "175": "Responsabilidad Técnica",
    "177": "Responsabilidad Técnica",
    "178": "Responsabilidad Técnica",
    "183": "Responsabilidad Técnica",
    "184": "Responsabilidad Técnica",
    "188": "Responsabilidad Técnica",
    "191": "Responsabilidad Técnica",
    # Asistencia Técnica
    "015": "Asistencia Técnica",
    "090": "Asistencia Técnica",
    "099": "Asistencia Técnica",
    "125": "Asistencia Técnica",
    "168": "Asistencia Técnica",
    "169": "Asistencia Técnica",
    # Responsabilidad Administrativa
    "024": "Responsabilidad Administrativa",
    "045": "Responsabilidad Administrativa",
    "066": "Responsabilidad Administrativa",
    "067": "Responsabilidad Administrativa",
    "084": "Responsabilidad Administrativa",
    "094": "Responsabilidad Administrativa",
    "097": "Responsabilidad Administrativa",
    "101": "Responsabilidad Administrativa",
    "115": "Responsabilidad Administrativa",
    "116": "Responsabilidad Administrativa",
    "117": "Responsabilidad Administrativa",
    "118": "Responsabilidad Administrativa",
    "139": "Responsabilidad Administrativa",
    "142": "Responsabilidad Administrativa",
    "151": "Responsabilidad Administrativa",
    "152": "Responsabilidad Administrativa",
    "155": "Responsabilidad Administrativa",
    "159": "Responsabilidad Administrativa",
    "160": "Responsabilidad Administrativa",
    "167": "Responsabilidad Administrativa",
    "170": "Responsabilidad Administrativa",
    "172": "Responsabilidad Administrativa",
    "174": "Responsabilidad Administrativa",
    "179": "Responsabilidad Administrativa",
    "182": "Responsabilidad Administrativa",
    "185": "Responsabilidad Administrativa",
    "186": "Responsabilidad Administrativa",
    "187": "Responsabilidad Administrativa",
    "193": "Responsabilidad Administrativa",
}

MAP_AREA_TEMATICA = {
    # ANP
    "015": "ANP", "056": "ANP", "066": "ANP", "074": "ANP",
    "084": "ANP", "090": "ANP", "097": "ANP", "099": "ANP",
    "107": "ANP", "125": "ANP", "133": "ANP", "151": "ANP",
    "160": "ANP", "162": "ANP", "168": "ANP", "169": "ANP",
    # Ecosistemas y servicios ecosistémicos
    "067": "Ecosistemas y servicios ecosistémicos",
    "069": "Ecosistemas y servicios ecosistémicos",
    "113": "Ecosistemas y servicios ecosistémicos",
    "130": "Ecosistemas y servicios ecosistémicos",
    "131": "Ecosistemas y servicios ecosistémicos",
    "132": "Ecosistemas y servicios ecosistémicos",
    "139": "Ecosistemas y servicios ecosistémicos",
    "140": "Ecosistemas y servicios ecosistémicos",
    "142": "Ecosistemas y servicios ecosistémicos",
    "144": "Ecosistemas y servicios ecosistémicos",
    "145": "Ecosistemas y servicios ecosistémicos",
    "154": "Ecosistemas y servicios ecosistémicos",
    "156": "Ecosistemas y servicios ecosistémicos",
    "157": "Ecosistemas y servicios ecosistémicos",
    "165": "Ecosistemas y servicios ecosistémicos",
    "167": "Ecosistemas y servicios ecosistémicos",
    "170": "Ecosistemas y servicios ecosistémicos",
    "172": "Ecosistemas y servicios ecosistémicos",
    "175": "Ecosistemas y servicios ecosistémicos",
    "177": "Ecosistemas y servicios ecosistémicos",
    "178": "Ecosistemas y servicios ecosistémicos",
    # Mitigación y adaptación al cambio climático
    "062": "Mitigación y adaptación al cambio climático",
    "128": "Mitigación y adaptación al cambio climático",
    "135": "Mitigación y adaptación al cambio climático",
    "136": "Mitigación y adaptación al cambio climático",
    "137": "Mitigación y adaptación al cambio climático",
    "155": "Mitigación y adaptación al cambio climático",
    "161": "Mitigación y adaptación al cambio climático",
    "164": "Mitigación y adaptación al cambio climático",
    "173": "Mitigación y adaptación al cambio climático",
    # Otro
    "024": "Otro", "045": "Otro", "064": "Otro", "089": "Otro",
    "094": "Otro", "126": "Otro", "134": "Otro", "147": "Otro",
    "152": "Otro", "158": "Otro", "159": "Otro", "163": "Otro",
    "174": "Otro", "179": "Otro", "183": "Otro", "186": "Otro",
    "187": "Otro",
    # Pasivos Ambientales
    "101": "Pasivos Ambientales",
    "115": "Pasivos Ambientales",
    "116": "Pasivos Ambientales",
    "118": "Pasivos Ambientales",
}

MAP_DEPARTAMENTOS = {
    'AMAZONAS': '01', 'ANCASH': '02', 'APURIMAC': '03', 'AREQUIPA': '04',
    'AYACUCHO': '05', 'CAJAMARCA': '06', 'CALLAO': '07', 'CUSCO': '08',
    'HUANCAVELICA': '09', 'HUANUCO': '10', 'ICA': '11', 'JUNIN': '12',
    'LA LIBERTAD': '13', 'LAMBAYEQUE': '14', 'LIMA': '15', 'LORETO': '16',
    'MADRE DE DIOS': '17', 'MOQUEGUA': '18', 'PASCO': '19', 'PIURA': '20',
    'PUNO': '21', 'SAN MARTIN': '22', 'TACNA': '23', 'TUMBES': '24',
    'UCAYALI': '25',
}


# =========================================================
# EXTRACCIÓN
# =========================================================


def _extraer_finanzas(conn_id: str = "sqlserver_origen") -> pd.DataFrame:
    """
    Extrae datos financieros de SQL Server.
    Reemplaza la lectura original de archivos .xlsb.
    """
    query = """
        SELECT
            FS_COD_PART,
            FS_DES_PART,
            FI_NUM_ANNO,
            FN_IMP_PRAN,
            FN_IMP_TOTA_EJEC,
            FN_IMP_COAN,
            FN_IMP_TOTA,
            FN_IMP_SALD
        FROM dbo.FinanzasProyectos
    """
    logger.info("Extrayendo datos financieros de SQL Server (%s)...", conn_id)
    conn = _get_pymssql_conn(conn_id)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    logger.info("Registros extraídos: %d", len(df))
    return df


def _extraer_entidades(ruta: str = "/opt/airflow/data/reporte_completo.xlsx") -> pd.DataFrame:
    """
    Lee el Excel de entidades generado por el scraper de vtrack.cl.
    Reemplaza la lectura original de 'Profonanpe - entidades.xlsx'.
    """
    logger.info("Leyendo entidades desde %s...", ruta)
    df = pd.read_excel(ruta)
    df.columns = df.columns.str.strip()
    logger.info("Entidades leídas: %d filas", len(df))
    return df


# =========================================================
# TRANSFORMACIÓN  (lógica conservada del original v5_ET.py)
# =========================================================


def _limpiar_finanzas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza de columnas financieras (sección 2 del original)."""
    df = df.copy()
    df.columns = df.columns.str.strip()

    df["FS_COD_PART"] = (
        df["FS_COD_PART"]
        .astype(str)
        .str.strip()
        .str.extract(r"(\d+)")[0]
        .str.zfill(3)
    )

    df["FS_DES_PART"] = df["FS_DES_PART"].astype(str).str.strip()
    df["FI_NUM_ANNO"] = df["FI_NUM_ANNO"].astype(int)

    columnas_numericas = [
        "FN_IMP_PRAN",
        "FN_IMP_TOTA_EJEC",
        "FN_IMP_COAN",
        "FN_IMP_TOTA",
        "FN_IMP_SALD",
    ]
    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def _construir_dim_proyecto(df_finanzas: pd.DataFrame) -> pd.DataFrame:
    """Construye la dimensión Proyecto (secciones 4-7 del original)."""
    anio_actual = df_finanzas["FI_NUM_ANNO"].max()

    proyectos_activos = (
        df_finanzas[
            (df_finanzas["FI_NUM_ANNO"] == anio_actual)
            & (df_finanzas["FN_IMP_PRAN"] > 0)
        ]["FS_COD_PART"]
        .astype(str)
        .str.strip()
        .str.extract(r"(\d+)")[0]
        .str.zfill(3)
        .unique()
    )

    dim = (
        df_finanzas[["FS_COD_PART", "FS_DES_PART"]]
        .drop_duplicates()
        .rename(columns={
            "FS_COD_PART": "CodigoProyecto",
            "FS_DES_PART": "Proyecto",
        })
    )
    dim["CodigoProyecto"] = dim["CodigoProyecto"].astype(str).str.zfill(3)

    # Tipo POG
    dim["TipoPOG"] = np.select(
        [
            dim["CodigoProyecto"].isin(POG_ACUMULADO),
            dim["CodigoProyecto"].isin(MAP_POG.keys()),
        ],
        ["Acumulado", "Fijo"],
        default="Sin POG Conocido",
    )

    # Estado del proyecto
    dim["EstadoProyecto"] = np.where(
        dim["CodigoProyecto"].isin(proyectos_activos),
        "En ejecución",
        "Cerrado",
    )

    # Tipo Responsabilidad
    dim["TipoResponsabilidad"] = (
        dim["CodigoProyecto"]
        .map(MAP_RESPONSABILIDAD)
        .fillna("Sin Clasificar")
    )

    # Área Temática
    dim["AreaTematica"] = (
        dim["CodigoProyecto"]
        .map(MAP_AREA_TEMATICA)
        .fillna("Sin Clasificar")
    )

    dim["IdProyecto"] = range(1, len(dim) + 1)

    dim = dim[
        [
            "CodigoProyecto", "Proyecto", "EstadoProyecto",
            "TipoPOG", "TipoResponsabilidad", "AreaTematica",
            "IdProyecto",
        ]
    ]
    return dim


def _construir_fact_finanzas(
    df_finanzas: pd.DataFrame,
    dim_proyecto: pd.DataFrame,
) -> pd.DataFrame:
    """Construye la tabla de hechos financieros (secciones 5-8 del original)."""
    fact = df_finanzas.copy()

    fact["Fecha"] = pd.to_datetime(
        dict(year=fact["FI_NUM_ANNO"], month=1, day=1)
    )

    # Merge con dim_proyecto
    fact = fact.merge(
        dim_proyecto,
        left_on="FS_COD_PART",
        right_on="CodigoProyecto",
        how="left",
    )

    fact = fact.sort_values(
        ["FS_COD_PART", "FI_NUM_ANNO"]
    ).reset_index(drop=True)

    # POG fijo
    fact["POG"] = np.nan
    mask_fijo = fact["FS_COD_PART"].isin(MAP_POG.keys())
    fact.loc[mask_fijo, "POG"] = fact.loc[mask_fijo, "FS_COD_PART"].map(MAP_POG)

    # POG acumulado
    mask_acum = fact["FS_COD_PART"].isin(POG_ACUMULADO)
    fact.loc[mask_acum, "POG"] = (
        fact.loc[mask_acum]
        .groupby("FS_COD_PART")["FN_IMP_TOTA_EJEC"]
        .cumsum()
    )

    # Selección y renombrado final
    fact_final = fact[
        [
            "IdProyecto", "Fecha", "FI_NUM_ANNO",
            "FN_IMP_PRAN", "FN_IMP_TOTA_EJEC", "FN_IMP_COAN",
            "FN_IMP_TOTA", "FN_IMP_SALD", "POG",
        ]
    ].rename(columns={
        "FI_NUM_ANNO": "Anio",
        "FN_IMP_PRAN": "POA",
        "FN_IMP_TOTA_EJEC": "EjecutadoAnual",
        "FN_IMP_COAN": "Comprometido",
        "FN_IMP_TOTA": "Total",
        "FN_IMP_SALD": "Saldo",
    })

    return fact_final


def _construir_dim_fecha(fact_finanzas: pd.DataFrame) -> pd.DataFrame:
    """Construye la dimensión Fecha (sección 9 del original)."""
    dim = (
        fact_finanzas[["Fecha"]]
        .drop_duplicates()
        .sort_values("Fecha")
        .reset_index(drop=True)
    )
    dim["IdFecha"] = range(1, len(dim) + 1)
    dim["Anio"] = dim["Fecha"].dt.year
    return dim


def _construir_dim_ubicacion_y_bridge(
    df_entidades: pd.DataFrame,
    dim_proyecto: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construye dim_ubicacion y bridge_proyecto_ubicacion
    (secciones 10-12 del original).
    """
    ent = df_entidades.copy()

    ent["CodigoNormalizado"] = ent["Código"].apply(normalizar_codigo_proyecto)
    ent = ent.explode("CodigoNormalizado")
    ent["CodigoNormalizado"] = ent["CodigoNormalizado"].astype(str).str.zfill(3)

    # Departamentos
    ent["Departamento"] = (
        ent["Departamento"]
        .astype(str)
        .str.upper()
        .str.split(";")
    )
    ent = ent.explode("Departamento")
    ent["Departamento"] = ent["Departamento"].str.strip().apply(quitar_tildes)
    ent = ent[ent["Departamento"].ne("")]

    # Dim Ubicación
    dim_ubi = (
        ent[["Departamento"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_ubi["Ubigeo"] = dim_ubi["Departamento"].map(MAP_DEPARTAMENTOS)
    dim_ubi = dim_ubi.dropna(subset=["Ubigeo"]).reset_index(drop=True)
    dim_ubi["IdUbicacion"] = range(1, len(dim_ubi) + 1)

    # Bridge
    bridge = ent[["CodigoNormalizado", "Departamento"]].copy()
    bridge = bridge.rename(columns={"CodigoNormalizado": "CodigoProyecto"})
    bridge = bridge.merge(dim_proyecto, on="CodigoProyecto", how="left")
    bridge["Ubigeo"] = bridge["Departamento"].map(MAP_DEPARTAMENTOS)
    bridge = bridge.merge(dim_ubi, on="Ubigeo", how="left")
    bridge_final = bridge[["IdProyecto", "IdUbicacion"]].drop_duplicates()

    return dim_ubi, bridge_final


# =========================================================
# CARGA (LOAD)
# =========================================================


def _cargar_al_dw(
    tablas: dict[str, pd.DataFrame],
    conn_id: str = "sqlserver_dw",
):
    """
    Escribe cada DataFrame como tabla en el Data Warehouse.
    Usa if_exists='replace' para reemplazar en cada ejecución.
    """
    logger.info("Conectando al Data Warehouse (%s)...", conn_id)
    conn = _get_pymssql_conn(conn_id)
    cursor = conn.cursor()

    for nombre_tabla, df in tablas.items():
        logger.info("Cargando tabla '%s' (%d filas)...", nombre_tabla, len(df))

        # Eliminar tabla existente si existe
        cursor.execute(f"""
            IF OBJECT_ID('dbo.{nombre_tabla}', 'U') IS NOT NULL
                DROP TABLE dbo.{nombre_tabla}
        """)
        conn.commit()

        # Crear tabla e insertar datos
        if df.empty:
            logger.warning("Tabla '%s' está vacía, se omite.", nombre_tabla)
            continue

        # Generar DDL dinámico basado en los tipos del DataFrame
        col_defs = []
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "FLOAT"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "DATETIME"
            else:
                sql_type = "NVARCHAR(500)"
            col_defs.append(f"[{col_name}] {sql_type} NULL")

        create_sql = f"CREATE TABLE dbo.{nombre_tabla} ({', '.join(col_defs)})"
        cursor.execute(create_sql)
        conn.commit()

        # Insertar filas
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join([f"[{c}]" for c in columns])
        insert_sql = f"INSERT INTO dbo.{nombre_tabla} ({col_list}) VALUES ({placeholders})"

        # Convertir NaT/NaN a None para pymssql
        df_insert = df.copy()
        for col in df_insert.columns:
            # Siempre convertir a object primero para que pandas no vuelva a castear None a NaN
            df_insert[col] = df_insert[col].astype(object).where(
                pd.notna(df_insert[col]), None
            )

        rows = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        logger.info("Tabla '%s' cargada exitosamente.", nombre_tabla)

    cursor.close()
    conn.close()
    logger.info("Carga al DW completada.")


# =========================================================
# FUNCIÓN PRINCIPAL  (entry point para PythonOperator)
# =========================================================


def ejecutar_transformacion(**kwargs):
    """
    Función principal que orquesta todo el pipeline ETL:
    Extract → Transform → Load.

    Invocada por el PythonOperator del DAG.
    """
    logger.info("=" * 60)
    logger.info("INICIO ETL FINANZAS PROFONANPE")
    logger.info("=" * 60)

    # ----- EXTRACT -----
    df_finanzas = _extraer_finanzas(conn_id="sqlserver_origen")
    df_entidades = _extraer_entidades(ruta="/opt/airflow/data/reporte_completo.xlsx")

    # ----- TRANSFORM -----
    logger.info("Limpiando datos financieros...")
    df_finanzas = _limpiar_finanzas(df_finanzas)

    logger.info("Construyendo dimensión Proyecto...")
    dim_proyecto = _construir_dim_proyecto(df_finanzas)

    logger.info("Construyendo tabla de hechos Financieros...")
    fact_finanzas = _construir_fact_finanzas(df_finanzas, dim_proyecto)

    logger.info("Construyendo dimensión Fecha...")
    dim_fecha = _construir_dim_fecha(fact_finanzas)

    logger.info("Construyendo dimensión Ubicación y Bridge...")
    dim_ubicacion, bridge_proyecto_ubicacion = _construir_dim_ubicacion_y_bridge(
        df_entidades, dim_proyecto
    )

    # ----- LOAD -----
    tablas = {
        "FactFinanciero": fact_finanzas,
        "DimProyecto": dim_proyecto,
        "DimFecha": dim_fecha,
        "DimUbicacion": dim_ubicacion,
        "BridgeProyectoUbicacion": bridge_proyecto_ubicacion,
    }

    _cargar_al_dw(tablas, conn_id="sqlserver_dw")

    logger.info("=" * 60)
    logger.info("ETL FINANZAS PROFONANPE — COMPLETADO EXITOSAMENTE")
    logger.info("=" * 60)
