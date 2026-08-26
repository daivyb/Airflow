
import pandas as pd
import numpy as np
import glob
import unicodedata

# =========================================================
# UTILIDADES
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
# 1. FINANZAS
# =========================================================

archivos = [
    f for f in glob.glob("Finanzas/*.xlsb")
    if "~$" not in f
]

df_list = []

for archivo in archivos:
    print(f"Leyendo: {archivo}")
    df = pd.read_excel(archivo, engine="pyxlsb")
    df.columns = df.columns.str.strip()
    df_list.append(df)

df_finanzas = pd.concat(df_list, ignore_index=True)

# =========================================================
# 2. LIMPIEZA FINANZAS
# =========================================================

df_finanzas["FS_COD_PART"] = (
    df_finanzas["FS_COD_PART"]
    .astype(str)
    .str.strip()
    .str.extract(r"(\d+)")[0]
    .str.zfill(3)
)

df_finanzas["FS_DES_PART"] = df_finanzas["FS_DES_PART"].astype(str).str.strip()
df_finanzas["FI_NUM_ANNO"] = df_finanzas["FI_NUM_ANNO"].astype(int)

# Asegurar numéricos
columnas_numericas = [
    "FN_IMP_PRAN",
    "FN_IMP_TOTA_EJEC",
    "FN_IMP_COAN",
    "FN_IMP_TOTA",
    "FN_IMP_SALD"
]

for col in columnas_numericas:
    df_finanzas[col] = pd.to_numeric(df_finanzas[col], errors="coerce").fillna(0)

# =========================================================
# 3. POG
# =========================================================

map_pog = {
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
    "193": 300000.00
}

pog_acumulado = {
    "015","045","064","069","089",
    "099", "056" # Validados al final.
    "115","116","118","132","134",
    "139","159","163","167","174",
    "187"
}


# =========================================================
# ESTADO DEL PROYECTO
# =========================================================

anio_actual = df_finanzas["FI_NUM_ANNO"].max()

proyectos_activos = (
    df_finanzas[
        (df_finanzas["FI_NUM_ANNO"] == anio_actual)
        &
        (df_finanzas["FN_IMP_PRAN"] > 0)
    ]["FS_COD_PART"]
    .astype(str)
    .str.strip()
    .str.extract(r"(\d+)")[0]
    .str.zfill(3)
    .unique()
)

# =========================================================
# 4. DIM PROYECTO
# =========================================================

dim_proyecto = (
    df_finanzas[["FS_COD_PART", "FS_DES_PART"]]
    .drop_duplicates()
    .rename(columns={
        "FS_COD_PART": "CodigoProyecto",
        "FS_DES_PART": "Proyecto"
    })
)

dim_proyecto["CodigoProyecto"] = (
    dim_proyecto["CodigoProyecto"]
    .astype(str)
    .str.zfill(3)
)

# =========================================================
# TIPO POG
# =========================================================

dim_proyecto["TipoPOG"] = np.select(
    [
        dim_proyecto["CodigoProyecto"].isin(pog_acumulado),
        dim_proyecto["CodigoProyecto"].isin(map_pog.keys())
    ],
    [
        "Acumulado",
        "Fijo"
    ],
    default="Sin POG Conocido"
)

dim_proyecto["EstadoProyecto"] = np.where(
    dim_proyecto["CodigoProyecto"].isin(proyectos_activos),
    "En ejecución",
    "Cerrado"
)

# =========================================================
# TIPO RESPONSABILIDAD
# =========================================================

map_responsabilidad = {

    # Responsabilidad Técnica
    "107":"Responsabilidad Técnica",
    "130":"Responsabilidad Técnica",
    "131":"Responsabilidad Técnica",
    "133":"Responsabilidad Técnica",
    "135":"Responsabilidad Técnica",
    "136":"Responsabilidad Técnica",
    "144":"Responsabilidad Técnica",
    "145":"Responsabilidad Técnica",
    "154":"Responsabilidad Técnica",
    "156":"Responsabilidad Técnica",
    "161":"Responsabilidad Técnica",
    "164":"Responsabilidad Técnica",
    "165":"Responsabilidad Técnica",
    "175":"Responsabilidad Técnica",
    "177":"Responsabilidad Técnica",
    "178":"Responsabilidad Técnica",
    "183":"Responsabilidad Técnica",
    "184":"Responsabilidad Técnica",
    "188":"Responsabilidad Técnica",
    "191":"Responsabilidad Técnica",

    # Asistencia Técnica
    "015":"Asistencia Técnica",
    "090":"Asistencia Técnica",
    "099":"Asistencia Técnica",
    "125":"Asistencia Técnica",
    "168":"Asistencia Técnica",
    "169":"Asistencia Técnica",

    # Responsabilidad Administrativa
    "024":"Responsabilidad Administrativa",
    "045":"Responsabilidad Administrativa",
    "066":"Responsabilidad Administrativa",
    "067":"Responsabilidad Administrativa",
    "084":"Responsabilidad Administrativa",
    "094":"Responsabilidad Administrativa",
    "097":"Responsabilidad Administrativa",
    "101":"Responsabilidad Administrativa",
    "115":"Responsabilidad Administrativa",
    "116":"Responsabilidad Administrativa",
    "117":"Responsabilidad Administrativa",
    "118":"Responsabilidad Administrativa",
    "139":"Responsabilidad Administrativa",
    "142":"Responsabilidad Administrativa",
    "151":"Responsabilidad Administrativa",
    "152":"Responsabilidad Administrativa",
    "155":"Responsabilidad Administrativa",
    "159":"Responsabilidad Administrativa",
    "160":"Responsabilidad Administrativa",
    "167":"Responsabilidad Administrativa",
    "170":"Responsabilidad Administrativa",
    "172":"Responsabilidad Administrativa",
    "174":"Responsabilidad Administrativa",
    "179":"Responsabilidad Administrativa",
    "182":"Responsabilidad Administrativa",
    "185":"Responsabilidad Administrativa",
    "186":"Responsabilidad Administrativa",
    "187":"Responsabilidad Administrativa",
    "193":"Responsabilidad Administrativa"
}

dim_proyecto["TipoResponsabilidad"] = (
    dim_proyecto["CodigoProyecto"]
        .map(map_responsabilidad)
        .fillna("Sin Clasificar")
)

# =========================================================
# ÁREA TEMÁTICA
# =========================================================

map_area_tematica = {

    # ANP
    "015":"ANP",
    "056":"ANP",
    "066":"ANP",
    "074":"ANP",
    "084":"ANP",
    "090":"ANP",
    "097":"ANP",
    "099":"ANP",
    "107":"ANP",
    "125":"ANP",
    "133":"ANP",
    "151":"ANP",
    "160":"ANP",
    "162":"ANP",
    "168":"ANP",
    "169":"ANP",

    # Ecosistemas y servicios ecosistémicos
    "067":"Ecosistemas y servicios ecosistémicos",
    "069":"Ecosistemas y servicios ecosistémicos",
    "113":"Ecosistemas y servicios ecosistémicos",
    "130":"Ecosistemas y servicios ecosistémicos",
    "131":"Ecosistemas y servicios ecosistémicos",
    "132":"Ecosistemas y servicios ecosistémicos",
    "139":"Ecosistemas y servicios ecosistémicos",
    "140":"Ecosistemas y servicios ecosistémicos",
    "142":"Ecosistemas y servicios ecosistémicos",
    "144":"Ecosistemas y servicios ecosistémicos",
    "145":"Ecosistemas y servicios ecosistémicos",
    "154":"Ecosistemas y servicios ecosistémicos",
    "156":"Ecosistemas y servicios ecosistémicos",
    "157":"Ecosistemas y servicios ecosistémicos",
    "165":"Ecosistemas y servicios ecosistémicos",
    "167":"Ecosistemas y servicios ecosistémicos",
    "170":"Ecosistemas y servicios ecosistémicos",
    "172":"Ecosistemas y servicios ecosistémicos",
    "175":"Ecosistemas y servicios ecosistémicos",
    "177":"Ecosistemas y servicios ecosistémicos",
    "178":"Ecosistemas y servicios ecosistémicos",

    # Mitigación y adaptación al cambio climático
    "062":"Mitigación y adaptación al cambio climático",
    "128":"Mitigación y adaptación al cambio climático",
    "135":"Mitigación y adaptación al cambio climático",
    "136":"Mitigación y adaptación al cambio climático",
    "137":"Mitigación y adaptación al cambio climático",
    "155":"Mitigación y adaptación al cambio climático",
    "161":"Mitigación y adaptación al cambio climático",
    "164":"Mitigación y adaptación al cambio climático",
    "173":"Mitigación y adaptación al cambio climático",

    # Otro
    "024":"Otro",
    "045":"Otro",
    "064":"Otro",
    "089":"Otro",
    "094":"Otro",
    "126":"Otro",
    "134":"Otro",
    "147":"Otro",
    "152":"Otro",
    "158":"Otro",
    "159":"Otro",
    "163":"Otro",
    "174":"Otro",
    "179":"Otro",
    "183":"Otro",
    "186":"Otro",
    "187":"Otro",

    # Pasivos Ambientales
    "101":"Pasivos Ambientales",
    "115":"Pasivos Ambientales",
    "116":"Pasivos Ambientales",
    "118":"Pasivos Ambientales"
}

dim_proyecto["AreaTematica"] = (
    dim_proyecto["CodigoProyecto"]
        .map(map_area_tematica)
        .fillna("Sin Clasificar")
)

dim_proyecto["IdProyecto"] = range(1, len(dim_proyecto) + 1)

dim_proyecto = dim_proyecto[
    [
        "CodigoProyecto",
        "Proyecto",
        "EstadoProyecto",
        "TipoPOG",
        "TipoResponsabilidad",
        "AreaTematica",
        "IdProyecto"
    ]
]

# =========================================================
# 5. FACT ANUAL
# =========================================================

fact = df_finanzas.copy()

fact["Fecha"] = pd.to_datetime(
    dict(
        year=fact["FI_NUM_ANNO"],
        month=1,
        day=1
    )
)

# =========================================================
# 6. MERGE PROYECTO
# =========================================================

fact = fact.merge(
    dim_proyecto,
    left_on="FS_COD_PART",
    right_on="CodigoProyecto",
    how="left"
)

fact = fact.sort_values(
    ["FS_COD_PART", "FI_NUM_ANNO"]
).reset_index(drop=True)

# =========================================================
# 7. POG EN FACT
# =========================================================

fact["POG"] = np.nan

# POG fijo
mask_fijo = fact["FS_COD_PART"].isin(map_pog.keys())

fact.loc[mask_fijo, "POG"] = (
    fact.loc[mask_fijo, "FS_COD_PART"]
    .map(map_pog)
)

# POG acumulado
mask_acum = fact["FS_COD_PART"].isin(pog_acumulado)

fact.loc[mask_acum, "POG"] = (
    fact.loc[mask_acum]
    .groupby("FS_COD_PART")["FN_IMP_TOTA_EJEC"]
    .cumsum()
)

# =========================================================
# 8. FACT FINAL
# =========================================================

fact_finanzas = fact[
    [
        "IdProyecto",
        "Fecha",
        "FI_NUM_ANNO",
        "FN_IMP_PRAN",
        "FN_IMP_TOTA_EJEC",
        "FN_IMP_COAN",
        "FN_IMP_TOTA",
        "FN_IMP_SALD",
        "POG"
    ]
].rename(columns={
    "FI_NUM_ANNO": "Anio",
    "FN_IMP_PRAN": "POA",
    "FN_IMP_TOTA_EJEC": "EjecutadoAnual",
    "FN_IMP_COAN": "Comprometido",
    "FN_IMP_TOTA": "Total",
    "FN_IMP_SALD": "Saldo"
})

# =========================================================
# 9. DIM FECHA
# =========================================================

dim_fecha = (
    fact_finanzas[["Fecha"]]
    .drop_duplicates()
    .sort_values("Fecha")
    .reset_index(drop=True)
)

dim_fecha["IdFecha"] = range(1, len(dim_fecha) + 1)
dim_fecha["Anio"] = dim_fecha["Fecha"].dt.year

# =========================================================
# 10. ENTIDADES
# =========================================================

df_entidades = pd.read_excel("Profonanpe -  entidades.xlsx")
df_entidades.columns = df_entidades.columns.str.strip()

df_entidades["CodigoNormalizado"] = (
    df_entidades["Código"]
    .apply(normalizar_codigo_proyecto)
)

df_entidades = df_entidades.explode("CodigoNormalizado")

df_entidades["CodigoNormalizado"] = (
    df_entidades["CodigoNormalizado"]
    .astype(str)
    .str.zfill(3)
)

# =========================================================
# DEPARTAMENTOS
# =========================================================

df_entidades["Departamento"] = (
    df_entidades["Departamento"]
    .astype(str)
    .str.upper()
    .str.split(";")
)

df_entidades = df_entidades.explode("Departamento")

df_entidades["Departamento"] = (
    df_entidades["Departamento"]
    .str.strip()
    .apply(quitar_tildes)
)

df_entidades = df_entidades[
    df_entidades["Departamento"].ne("")
]

# =========================================================
# MAPA
# =========================================================

map_departamentos = {
    'AMAZONAS':'01','ANCASH':'02','APURIMAC':'03','AREQUIPA':'04',
    'AYACUCHO':'05','CAJAMARCA':'06','CALLAO':'07','CUSCO':'08',
    'HUANCAVELICA':'09','HUANUCO':'10','ICA':'11','JUNIN':'12',
    'LA LIBERTAD':'13','LAMBAYEQUE':'14','LIMA':'15','LORETO':'16',
    'MADRE DE DIOS':'17','MOQUEGUA':'18','PASCO':'19','PIURA':'20',
    'PUNO':'21','SAN MARTIN':'22','TACNA':'23','TUMBES':'24','UCAYALI':'25'
}

# =========================================================
# 11. DIM UBICACION
# =========================================================

dim_ubicacion = (
    df_entidades[["Departamento"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

dim_ubicacion["Ubigeo"] = (
    dim_ubicacion["Departamento"]
    .map(map_departamentos)
)

dim_ubicacion = dim_ubicacion.dropna(
    subset=["Ubigeo"]
).reset_index(drop=True)

dim_ubicacion["IdUbicacion"] = range(
    1,
    len(dim_ubicacion) + 1
)

# =========================================================
# 12. BRIDGE
# =========================================================

bridge = df_entidades[
    ["CodigoNormalizado", "Departamento"]
].copy()

bridge = bridge.rename(
    columns={"CodigoNormalizado": "CodigoProyecto"}
)

bridge = bridge.merge(
    dim_proyecto,
    on="CodigoProyecto",
    how="left"
)

bridge["Ubigeo"] = bridge["Departamento"].map(map_departamentos)

bridge = bridge.merge(
    dim_ubicacion,
    on="Ubigeo",
    how="left"
)

bridge_proyecto_ubicacion = bridge[
    ["IdProyecto", "IdUbicacion"]
].drop_duplicates()

# =========================================================
# 13. EXPORT
# =========================================================

with pd.ExcelWriter("modelo_estrella_finanzas.xlsx") as writer:
    fact_finanzas.to_excel(
        writer,
        sheet_name="FactFinanciero",
        index=False
    )

    dim_proyecto.to_excel(
        writer,
        sheet_name="DimProyecto",
        index=False
    )

    dim_fecha.to_excel(
        writer,
        sheet_name="DimFecha",
        index=False
    )

    dim_ubicacion.to_excel(
        writer,
        sheet_name="DimUbicacion",
        index=False
    )

    bridge_proyecto_ubicacion.to_excel(
        writer,
        sheet_name="BridgeProyectoUbicacion",
        index=False
    )

print("Modelo estrella generado correctamente")
