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

### 6. Escape de caracteres especiales en contraseñas (`$` / `$$`) en Docker Compose
**Problema:** Si una contraseña contiene el símbolo de dólar (por ejemplo `Boss30$$PF`), Docker Compose interpreta el `$` como una variable de entorno a sustituir, devorando caracteres y enviando una clave errónea a VTrack (ej. `Boss30$PF`), lo que ocasionaba un bloqueo en el login.
**Solución:** En archivos `.env` consumidos por Docker Compose, el símbolo `$` debe escaparse con otro `$`. Por tanto, para enviar dos signos de dólar literales (`$$`), en el archivo `.env` debe escribirse como cuádruple dólar: `Boss30$$$$PF`.

### 7. Sensibilidad a Mayúsculas/Minúsculas en Variables de Airflow (Jinja)
**Problema:** Al inyectar variables de entorno con el prefijo `AIRFLOW_VAR_`, Airflow las registra automáticamente en su base de datos interna con claves en **MAYÚSCULAS** (ej. `VTRACK_USER`). Si en el DAG de Python se consulta mediante Jinja en minúsculas `{{ var.value.get('vtrack_user') }}`, Airflow devuelve una cadena vacía.
**Solución:** Mantener estricta coherencia en los identificadores de variables en los DAGs utilizando siempre mayúsculas cuando provengan de inyecciones del sistema: `{{ var.value.get('VTRACK_USER') }}`.

---

## 🏃 Ejecución y Operación

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

