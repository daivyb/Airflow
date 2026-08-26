---
name: migracion-etl-airflow
description: >-
  Use this skill to implement the Airflow ETL migration for Profonanpe.
  It generates the Docker infrastructure, the DAGs, and the transformation scripts
  based on the architecture approved by the user (LocalExecutor, DockerOperator for Playwright, pymssql).
---

# Migración de ETL a Airflow

Esta skill contiene las directrices para escribir el código y configurar el entorno
de Airflow con Docker para la empresa Profonanpe, según el plan de implementación acordado.

## Resumen de la Arquitectura

1.  **Airflow**: Usar `LocalExecutor` vía `docker-compose.yaml` (una base de datos Postgres y el contenedor webserver/scheduler juntos).
2.  **Base de datos Origen y Destino**: SQL Server.
    - Se debe usar el paquete `pymssql` en Python para la conexión a la base de datos.
3.  **Scraping (vtrack.cl)**: 
    - Se debe utilizar el `DockerOperator` en el DAG de Airflow para levantar un contenedor temporal usando la imagen oficial `mcr.microsoft.com/playwright/python`.
    - El contenedor de Airflow debe tener acceso al socket de Docker (`/var/run/docker.sock`) para poder instanciar contenedores hermanos.

## Pasos de Implementación que debe ejecutar el Agente

Cuando actives esta skill, debes crear los siguientes componentes en el proyecto (directorio raíz: `C:\Users\Acer\dev\Project\Airflow`):

### 1. Entorno Docker
- **`Dockerfile`**: Debe usar como base `apache/airflow:2.10.0` (u otra versión 2.x reciente). 
  - Instalar herramientas del SO si es necesario para `pymssql` (usualmente `freetds-dev` en debian).
  - Instalar dependencias de Python: `pymssql`, `pandas`, `openpyxl`, `docker`.
- **`docker-compose.yaml`**: 
  - Configurar Postgres.
  - Configurar Airflow con `LocalExecutor`.
  - Mapear volúmenes:
    - `./dags:/opt/airflow/dags`
    - `./data:/opt/airflow/data` (volumen compartido para que Airflow y Playwright intercambien archivos).
    - `/var/run/docker.sock:/var/run/docker.sock` (para que el `DockerOperator` funcione).

### 2. Scripts de Tareas
- **`dags/scripts/scrape_vtrack.py`**:
  - Tomar el código de scraping original (el usuario lo proporcionó previamente) y adaptarlo para que se ejecute "headless" y guarde el reporte en `/data/reporte_completo.xlsx` (ruta dentro del contenedor de Playwright, que estará mapeada al volumen `./data`).
- **`dags/scripts/etl_finanzas.py`**:
  - Leer el script original ubicado en `C:\Users\Acer\dev\Project\Airflow\BI-FINANCIERA\Transformacíón\v5_ET.py`.
  - Refactorizarlo en una función (ej: `ejecutar_transformacion()`) que será llamada por el `PythonOperator`.
  - Cambiar la lectura de `.xlsb` para que lea desde el origen SQL Server mediante `pymssql` (puedes mockear la query asumiendo que extrae las mismas columnas que los excels).
  - Cambiar la lectura de las entidades para que lea `/opt/airflow/data/reporte_completo.xlsx`.
  - Cambiar la escritura final (que actualmente es a Excel) para usar `df.to_sql()` hacia la base de datos destino en SQL Server (Data Warehouse).

### 3. El DAG
- **`dags/finanzas_etl_dag.py`**:
  - Definir un DAG con programación diaria.
  - Tarea 1: `DockerOperator` que ejecute el script `scrape_vtrack.py`.
  - Tarea 2: `PythonOperator` que ejecute la transformación de `etl_finanzas.py`.
  - Definir las dependencias: Tarea 1 >> Tarea 2.

### Validaciones
- Asegúrate de crear el directorio `data/` localmente para que el mapeo de volúmenes no falle por permisos (Docker lo crea como root si no existe).
