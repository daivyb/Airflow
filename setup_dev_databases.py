"""
setup_dev_databases.py
======================
Script de setup para desarrollo local.
Crea las bases de datos de origen y DW en SQL Server Express,
y carga los archivos .xlsb (2019-2026) en la tabla de origen.

Ejecución:
    pip install pymssql pandas pyxlsb
    python setup_dev_databases.py

Requisitos:
    - SQL Server Express corriendo en LAPTOP-60BVA02D\\SQLEXPRESS
    - Autenticación mixta habilitada (ver instrucciones abajo)
    - Un usuario SQL Server creado para que Docker pueda conectarse
"""

import glob
import os
import sys

import pandas as pd
import pymssql

# =========================================================
# CONFIGURACIÓN — Ajustar si es necesario
# =========================================================

SQL_SERVER = "localhost"
SQL_PORT = 1433

# Usuario SQL Server para desarrollo (Docker lo usará)
# Si aún no lo has creado, el script intentará crearlo
SQL_USER = "airflow_dev"
SQL_PASSWORD = "Airflow2025!"

DB_ORIGEN = "ProfonanpeOrigen"
DB_DW = "ProfonanpeDW"

# Ruta a los archivos .xlsb
XLSB_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "BI-FINANCIERA", "Transformacíón", "Finanzas"
)

# Tabla destino en la BD de origen
TABLA_FINANZAS = "dbo.FinanzasProyectos"


# =========================================================
# FUNCIONES
# =========================================================


def get_connection(database: str = "master"):
    """Conexión a SQL Server Express usando autenticación Windows (trusted)."""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            database=database,
            # pymssql usa Kerberos/NTLM en Windows cuando no se pasa user/password
        )
        return conn
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a {SQL_SERVER}: {e}")
        print()
        print("Verifica que:")
        print("  1. SQL Server Express está corriendo")
        print("  2. El servicio 'SQL Server Browser' está iniciado")
        print("  3. TCP/IP está habilitado en SQL Server Configuration Manager")
        print(f"  4. El puerto {SQL_PORT} está configurado")
        sys.exit(1)


def ejecutar_sql(conn, sql, autocommit=True):
    """Ejecuta una sentencia SQL."""
    if autocommit:
        conn.autocommit(True)
    cursor = conn.cursor()
    cursor.execute(sql)
    if not autocommit:
        conn.commit()
    cursor.close()


def crear_bases_de_datos():
    """Crea las bases de datos de origen y DW si no existen."""
    print("=" * 60)
    print("1. CREANDO BASES DE DATOS")
    print("=" * 60)

    conn = get_connection("master")

    for db_name in [DB_ORIGEN, DB_DW]:
        print(f"   Creando '{db_name}'...", end=" ")
        ejecutar_sql(conn, f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}')
                CREATE DATABASE [{db_name}]
        """)
        print("OK")

    conn.close()


def crear_usuario_sql():
    """
    Crea un login y usuario SQL Server para que Docker pueda conectarse.
    Requiere que 'Mixed Mode Authentication' esté habilitado.
    """
    print()
    print("=" * 60)
    print("2. CREANDO USUARIO SQL SERVER PARA DOCKER")
    print("=" * 60)

    conn = get_connection("master")

    # Crear login a nivel de servidor
    print(f"   Creando login '{SQL_USER}'...", end=" ")
    try:
        ejecutar_sql(conn, f"""
            IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '{SQL_USER}')
            BEGIN
                CREATE LOGIN [{SQL_USER}] WITH PASSWORD = '{SQL_PASSWORD}',
                    DEFAULT_DATABASE = [{DB_ORIGEN}],
                    CHECK_POLICY = OFF
            END
        """)
        print("OK")
    except Exception as e:
        print(f"ADVERTENCIA: {e}")
        print("   Si falla, habilita 'Mixed Mode Authentication' en SQL Server.")
        print("   (SQL Server Management Studio > Propiedades del servidor > Security)")

    # Crear usuario en cada BD
    for db_name in [DB_ORIGEN, DB_DW]:
        print(f"   Creando usuario en '{db_name}'...", end=" ")
        try:
            conn_db = get_connection(db_name)
            ejecutar_sql(conn_db, f"""
                IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '{SQL_USER}')
                    CREATE USER [{SQL_USER}] FOR LOGIN [{SQL_USER}]
            """)
            ejecutar_sql(conn_db, f"""
                ALTER ROLE db_owner ADD MEMBER [{SQL_USER}]
            """)
            conn_db.close()
            print("OK")
        except Exception as e:
            print(f"ADVERTENCIA: {e}")

    conn.close()


def crear_tabla_finanzas():
    """Crea la tabla FinanzasProyectos en la BD de origen."""
    print()
    print("=" * 60)
    print("3. CREANDO TABLA DE FINANZAS")
    print("=" * 60)

    conn = get_connection(DB_ORIGEN)
    print(f"   Creando tabla {TABLA_FINANZAS}...", end=" ")
    ejecutar_sql(conn, f"""
        IF OBJECT_ID('{TABLA_FINANZAS}', 'U') IS NOT NULL
            DROP TABLE {TABLA_FINANZAS}
    """, autocommit=False)

    ejecutar_sql(conn, f"""
        CREATE TABLE {TABLA_FINANZAS} (
            FS_COD_PART    NVARCHAR(50),
            FS_DES_PART    NVARCHAR(500),
            FI_NUM_ANNO    INT,
            FN_IMP_PRAN    FLOAT,
            FN_IMP_TOTA_EJEC FLOAT,
            FN_IMP_COAN    FLOAT,
            FN_IMP_TOTA    FLOAT,
            FN_IMP_SALD    FLOAT
        )
    """, autocommit=False)
    print("OK")
    conn.close()


def cargar_excel_a_sql():
    """Lee los archivos .xlsb y los carga en la tabla FinanzasProyectos."""
    print()
    print("=" * 60)
    print("4. CARGANDO ARCHIVOS EXCEL (.xlsb)")
    print("=" * 60)

    archivos = [
        f for f in glob.glob(os.path.join(XLSB_DIR, "*.xlsb"))
        if "~$" not in f
    ]

    if not archivos:
        print(f"   [ERROR] No se encontraron archivos .xlsb en {XLSB_DIR}")
        sys.exit(1)

    print(f"   Archivos encontrados: {len(archivos)}")

    # Leer todos los Excel
    df_list = []
    for archivo in sorted(archivos):
        nombre = os.path.basename(archivo)
        print(f"   Leyendo: {nombre}...", end=" ")
        try:
            df = pd.read_excel(archivo, engine="pyxlsb")
            df.columns = df.columns.str.strip()
            df_list.append(df)
            print(f"{len(df)} filas")
        except Exception as e:
            print(f"ERROR: {e}")

    df_all = pd.concat(df_list, ignore_index=True)
    print(f"   Total combinado: {len(df_all)} filas")

    # Seleccionar solo las columnas esperadas
    columnas_esperadas = [
        "FS_COD_PART", "FS_DES_PART", "FI_NUM_ANNO",
        "FN_IMP_PRAN", "FN_IMP_TOTA_EJEC", "FN_IMP_COAN",
        "FN_IMP_TOTA", "FN_IMP_SALD"
    ]

    columnas_disponibles = [c for c in columnas_esperadas if c in df_all.columns]
    columnas_faltantes = [c for c in columnas_esperadas if c not in df_all.columns]

    if columnas_faltantes:
        print(f"   [ADVERTENCIA] Columnas no encontradas: {columnas_faltantes}")
        print(f"   Columnas disponibles en Excel: {list(df_all.columns)}")

    df_final = df_all[columnas_disponibles].copy()

    # Insertar en SQL Server
    print()
    print(f"   Insertando {len(df_final)} filas en {TABLA_FINANZAS}...")

    conn = get_connection(DB_ORIGEN)
    cursor = conn.cursor()

    # Convertir NaN a None
    df_insert = df_final.where(df_final.notna(), None)

    placeholders = ", ".join(["%s"] * len(columnas_disponibles))
    col_list = ", ".join([f"[{c}]" for c in columnas_disponibles])
    insert_sql = f"INSERT INTO {TABLA_FINANZAS} ({col_list}) VALUES ({placeholders})"

    rows = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"   ... {min(i + batch_size, len(rows))}/{len(rows)} filas insertadas")

    cursor.close()
    conn.close()
    print("   Carga completada.")


def verificar():
    """Verificación rápida de la carga."""
    print()
    print("=" * 60)
    print("5. VERIFICACIÓN")
    print("=" * 60)

    conn = get_connection(DB_ORIGEN)
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {TABLA_FINANZAS}")
    total = cursor.fetchone()[0]
    print(f"   Total filas en {TABLA_FINANZAS}: {total}")

    cursor.execute(f"""
        SELECT FI_NUM_ANNO, COUNT(*) as Filas
        FROM {TABLA_FINANZAS}
        GROUP BY FI_NUM_ANNO
        ORDER BY FI_NUM_ANNO
    """)
    print("   Desglose por año:")
    for row in cursor.fetchall():
        print(f"     {row[0]}: {row[1]} filas")

    cursor.close()
    conn.close()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    print()
    print("============================================================")
    print("  SETUP DESARROLLO — ETL PROFONANPE                     ")
    print("  SQL Server: {}  ".format(SQL_SERVER.ljust(40)))
    print("============================================================")
    print()

    crear_bases_de_datos()
    crear_usuario_sql()
    crear_tabla_finanzas()
    cargar_excel_a_sql()
    verificar()

    print()
    print("=" * 60)
    print("SETUP COMPLETADO")
    print("=" * 60)
    print()
    print("Conexiones para docker-compose.yaml:")
    print(f"  Origen: mssql+pymssql://{SQL_USER}:{SQL_PASSWORD}@host.docker.internal:{SQL_PORT}/{DB_ORIGEN}")
    print(f"  DW:     mssql+pymssql://{SQL_USER}:{SQL_PASSWORD}@host.docker.internal:{SQL_PORT}/{DB_DW}")
    print()
    print("IMPORTANTE: Si aún no lo has hecho, habilita:")
    print("  1. Mixed Mode Authentication en SQL Server")
    print("  2. TCP/IP en SQL Server Configuration Manager")
    print("  3. Reinicia el servicio de SQL Server")
    print()
