# Proyecto ETL Finanzas - (Migración a Apache Airflow)

Este proyecto representa la migración automatizada de los procesos de transformación financiera (previamente ejecutados en scripts de Python sueltos y planillas `.xlsb` locales) hacia una arquitectura moderna, orquestada y automatizada utilizando **Apache Airflow sobre Docker**.

## Arquitectura del Sistema

1. **Apache Airflow**: Desplegado localmente utilizando Docker Compose (`LocalExecutor`).
2. **Scraper VTrack**: 
   - Utiliza **Playwright** para extraer el reporte de entidades en tiempo real desde la plataforma VTrack (simulando navegación, logueo y clic en botones asíncronos).
   - *Nota técnica:* Playwright y Chromium están instalados de forma **nativa** en el contenedor base de Airflow para eludir los errores de montaje cruzado de volúmenes que ocurren al tratar de lanzar `DockerOperator` anidados dentro de Windows WSL2.
3. **Data Warehouse (SQL Server)**:
   - Conexión: Airflow se comunica con las bases de datos en la máquina *host* utilizando el alias de red `host.docker.internal`.
   - Motor de Python: `pymssql` y `pandas` manejan la lógica de Modelado Dimensional (dimensiones, hechos y bridge tables).

---

## Instalación y Despliegue

### 1. Configuración de Variables de Entorno (`.env`)
Antes de iniciar los contenedores, crea un archivo `.env` en la raíz del proyecto copiando la plantilla provista:
```bash
cp .env.example .env
```
Edita `.env` con las credenciales correspondientes a tu entorno:
- `AIRFLOW_CONN_SQLSERVER_ORIGEN`: Cadena de conexión a la base de datos origen.
- `AIRFLOW_CONN_SQLSERVER_DW`: Cadena de conexión al Data Warehouse.
- `AIRFLOW_VAR_VTRACK_USER`, `AIRFLOW_VAR_VTRACK_PASS`, `AIRFLOW_VAR_VTRACK_URL`: Credenciales y URL de VTrack.

> **Seguridad y Cero Configuración Manual:**  
> Gracias a este archivo, las credenciales no quedan en el código fuente. Airflow inyecta automáticamente las conexiones (`sqlserver_origen`, `sqlserver_dw`) y las variables globales (`VTRACK_*`) directamente en su base de datos. **No es necesario configurar nada manualmente en la interfaz web.**

### 2. Construir e iniciar contenedores
Ubicarse en la raíz del proyecto y ejecutar:
```bash
docker compose up -d --build
```
Esto reconstruirá la imagen (instalando Playwright, Chromium y librerías necesarias) y levantará los servicios (`postgres` interno, `scheduler` y `webserver`).

### 3. Acceso a la interfaz web de Airflow
Accede desde tu navegador a **http://localhost:8080** (o la IP del servidor en el puerto 8080):
- **Usuario:** `admin`
- **Contraseña:** `admin`

---

## Ejecución y Operación

### Ejecución Manual del Pipeline
1. Ingresa a la interfaz web de Airflow (http://localhost:8080).
2. Localiza el DAG **`finanzas_etl_pipeline`**.
3. Haz clic en el botón de **Play (Trigger DAG)** a la derecha.
   - **`scrape_vtrack`**: Ejecuta el navegador headless, inicia sesión en VTrack y descarga el reporte en `data/reporte_completo.xlsx`.
   - **`transformar_finanzas`**: Extrae la data de SQL Server origen, procesa el cruce y actualiza de forma destructiva (`drop and replace`) el Data Warehouse con los datos más recientes.

### Pausar y Reanudar el Servicio
Para no consumir recursos en la máquina cuando no se esté utilizando, se recomienda pausar los contenedores en lugar de destruirlos (conservando el estado y la base de datos intacta):

- **Pausar los servicios:**
  ```bash
  docker compose stop
  ```
- **Reanudar los servicios:**
  ```bash
  docker compose start
  ```
*(Esto enciende el entorno en segundos sin necesidad de reconstruir imágenes ni reconfigurar nada).*

