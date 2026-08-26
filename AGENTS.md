# Reglas de Proyecto: Airflow Profonanpe

Bienvenido al proyecto de migración ETL para Profonanpe.

## Tarea Pendiente
Existe un plan de implementación detallado para migrar la antigua solución (basada en Excel y scripts sueltos) a Apache Airflow en Docker.

Si el usuario te solicita "implementar el ETL", "crear los archivos" o cualquier variante de iniciar el trabajo, **debes utilizar la skill `migracion-etl-airflow`** que contiene todos los detalles de la arquitectura acordada (está ubicada en `.agents/skills/implementar-etl-airflow/SKILL.md`).

## Lineamientos
- Las conexiones a SQL Server deben usar la librería `pymssql`.
- El entorno de Airflow usa `LocalExecutor` sobre `docker-compose`.
- No intentes ejecutar el script de Playwright localmente, siempre usa `DockerOperator` para mantener el entorno limpio.
