# Proyecto ETL Finanzas - Profonanpe (Migración a Apache Airflow)

Este proyecto representa la migración automatizada de los procesos de transformación financiera (previamente ejecutados en scripts de Python sueltos y planillas `.xlsb` locales) hacia una arquitectura moderna, orquestada y automatizada utilizando **Apache Airflow sobre Docker**.

## 🏗️ Arquitectura del Sistema

1. **Apache Airflow**: Desplegado localmente utilizando Docker Compose (`LocalExecutor`).
2. **Scraper VTrack**: 
   - Utiliza **Playwright** para extraer el reporte de entidades en tiempo real desde la plataforma VTrack (simulando navegación, logueo y clic en botones asíncronos).
   - *Nota técnica:* Playwright y Chromium están instalados de forma **nativa** en el contenedor base de Airflow para eludir los errores de montaje cruzado de volúmenes que ocurren al tratar de lanzar `DockerOperator` anidados dentro de Windows WSL2.
3. **Data Warehouse (SQL Server)**:
   - Conexión: Airflow se comunica con las bases de datos en la máquina *host* utilizando el alias de red `host.docker.internal`.
   - Motor de Python: `pymssql` y `pandas` manejan la lógica de Modelado Dimensional (dimensiones, hechos y bridge tables).

---

## 🚀 Instalación y Despliegue

### 1. Construir e iniciar contenedores
Ubicarse en la raíz del proyecto y ejecutar:
```bash
docker compose up -d --build
```
Esto reconstruirá la imagen (instalando Playwright) y levantará los servicios (`postgres` interno, `scheduler` y `webserver`).

### 2. Configuración en la interfaz de Airflow
Accede a http://localhost:8080 (Admin: `airflow` / `airflow`).

**Conexiones de Bases de Datos (`Admin > Connections`)**:
¡No necesitas configurarlas manualmente! Las conexiones a tu SQL Server local (`sqlserver_origen` y `sqlserver_dw`) ya están inyectadas automáticamente desde el archivo `docker-compose.yaml` a través de variables de entorno (`AIRFLOW_CONN_...`). No tienes que hacer nada aquí.

**Variables Globales (`Admin > Variables`)**:
- `VTRACK_URL`: URL base de la plataforma (ej. `https://profonanpe.vtrack.cl`).
- `VTRACK_USER`: Correo de acceso a VTrack.
- `VTRACK_PASS`: Contraseña de VTrack.

---

## 🛠️ Lecciones Aprendidas y Solución de Errores Comunes

Durante el desarrollo e implementación, se solventaron obstáculos técnicos relevantes que quedan documentados aquí como referencia a futuro:

### 1. Error de `DockerOperator` en Windows (Volúmenes Mount/Bind)
**Problema:** Al ejecutar el scraper usando un contenedor hijo (`DockerOperator`), se intentaba montar el directorio `/opt/airflow/data` hacia el esclavo. Docker Desktop en Windows enviaba la petición buscando la ruta literalmente en `C:\opt\airflow\...`, arrojando errores de volumen inexistente.
**Solución:** Se abandonó el `DockerOperator`. Ahora, todas las dependencias del sistema operativo y de navegador para Playwright se instalan nativamente en el `Dockerfile` de Airflow mediante comandos ejecutados como usuario `root`. El scraper se invoca fluidamente con un `BashOperator` ligero y rápido.

### 2. Tiempos de espera asíncronos en menús desplegables (VTrack)
**Problema:** El scraper intentaba dar clic en "Descargar Excel" pero la animación del menú de VTrack demoraba un par de milisegundos en cargar. Al inyectar un comando crudo de JavaScript (`btn.click()`) inmediatamente, el DOM devolvía `null` y el clic fallaba silenciosamente, provocando un *Timeout* a los 2 minutos. Playwright, por su parte, se negaba a hacer clic usando `force=True` porque el botón aún no era "visible".
**Solución:** Se implementó una técnica híbrida. Se le indica a Playwright esperar firmemente que el elemento se **agregue** al DOM (`state="attached"`), tras lo cual forzamos el clic mediante Javascript:
```python
btn_locator.wait_for(state="attached", timeout=15000)
btn_locator.evaluate("node => node.click()")
```

### 3. Error Fatal de Pymssql: `Invalid column name 'nan'`
**Problema:** Al cruzar los dataframes de proyectos (que no todos tienen un presupuesto asignado), `pandas` rellena las celdas vacías con el valor matemático flotante `NaN`. Al intentar insertar masivamente a SQL Server, `pymssql` interpretó los `NaN` literalmente como un nombre de columna roto.
**Solución:** Se agregó un bucle de limpieza antes del insertado que castiga las columnas vacías. Específicamente, en Pandas, si no transformas antes la columna entera al tipo `object`, el comando de limpieza intentará auto-convertir los `None` (nulos de SQL) de regreso a `NaN`.
```python
df_insert[col] = df_insert[col].astype(object).where(pd.notna(df_insert[col]), None)
```

### 4. Error 10061 al conectar con SQL Server Express desde Docker
**Problema:** Al intentar establecer conexión a SQL Server nativo (Windows) usando el host `host.docker.internal` desde el contenedor de Airflow, pymssql lanzaba el error `10061` ("Connection refused").
**Solución:** SQL Server Express, por motivos de seguridad, viene con las conexiones TCP/IP **deshabilitadas** por defecto de fábrica. Para solucionarlo, es estrictamente necesario abrir "SQL Server Configuration Manager", ir a "Configuración de red de SQL Server" -> "Protocolos para SQLEXPRESS", y habilitar **TCP/IP**. Posteriormente, se debe reiniciar el servicio de SQL Server para que abra el puerto `1433`.

### 5. Configuración de Autenticación en SQL Server
**Problema:** Los scripts de Python (usando `pymssql`) que corrían dentro de Docker fallaban al intentar autenticarse en SQL Server ya que este únicamente esperaba credenciales automáticas de Windows (Windows Authentication), protocolo que un contenedor Linux no maneja nativamente de manera sencilla.
**Solución:** Se tuvo que habilitar el modo de **Autenticación Mixta** (SQL Server and Windows Authentication mode) en las propiedades del servidor (en SSMS). Esto permitió crear y utilizar un usuario local clásico de SQL Server (ej. usuario `sa` o crear uno de servicio dedicado) con el cual Airflow puede autenticarse de forma universal pasando un nombre de usuario y contraseña en texto plano en la cadena de conexión.

---

## 🏃 Ejecución
Para iniciar el pipeline, navega a la pestaña de `DAGs` en Airflow, y dale a **Trigger DAG (Play)** en el DAG `finanzas_etl_pipeline`.

1. **scrape_vtrack**: Irá de forma silenciosa a VTrack y descargará el archivo en el volumen persistente de la carpeta `data/`.
2. **transformar_finanzas**: Leerá SQL Server, procesará todo usando Python, y actualizará de forma destructiva (`drop and replace`) el Data Warehouse con los datos más recientes.
