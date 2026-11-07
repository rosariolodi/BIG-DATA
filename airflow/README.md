# Orquestación con Apache Airflow (local) — Unidad 4, tarea T1.2

DAG `datamarca_pipeline`: ingestión → validación → transformación → persistencia → notificación
(12 tareas + `corrida_completa`; grafo en `docs/orquestacion/dependencias.md`).

## Archivos
- `dags/datamarca_pipeline.py`: el DAG (operadores, schedule, retries, timeout, backoff).
- `../src/orquestacion/tareas.py`: código que ejecuta cada tarea (reutiliza los módulos de la Unidad 3).
- `Dockerfile`, `docker-compose.yaml`, `requirements-airflow.txt`, `.env.example`: entorno local.

## Configuración (subtareas de T1.2)
| Aspecto | Valor | Dónde |
|---|---|---|
| Operador | `BashOperator` por tarea (un proceso Spark propio) | `tarea()` en el DAG |
| Schedule | `0 2 * * *` (diario, 02:00 Bogotá), `catchup=False`, `max_active_runs=1` | `DAG(...)` |
| Reintentos | `retries=3`, `retry_delay=60 s` | `DATAMARCA_RETRIES`, `DATAMARCA_RETRY_DELAY_SEG` |
| Backoff exponencial | `retry_exponential_backoff=True`, tope 600 s (≈1, 2, 4 min) | `DATAMARCA_MAX_RETRY_DELAY_SEG` |
| Timeout | 15 min por defecto; 5 min ingestión; 2 min compuerta; 20 min persistencia; 1 h la corrida | `execution_timeout`, `dagrun_timeout` (`DATAMARCA_DAGRUN_TIMEOUT_MIN`) |
| Retención de `data/_staging/` | Exitosas: se borran los intermedios al terminar; fallidas: se conservan; cualquier corrida: se borra a los 7 días | `DATAMARCA_LIMPIAR_STAGING`, `DATAMARCA_RETENCION_DIAS` (T1.3) |

Manejo de errores, trazas y retención: `docs/orquestacion/manejo_errores_logging.md` (tarea T1.3).

## Ejecutar con Docker (recomendado, también en Windows)
```bash
cd airflow
if [ ! -f .env ]; then
    cp .env.example .env
else
    echo "Ya existe .env; se conserva."
fi
docker compose build
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080 -> activar el DAG `datamarca_pipeline` y pulsar "Trigger"
```

En Windows PowerShell, desde la raíz del repositorio:

```powershell
Set-Location airflow
if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
}
docker compose build
docker compose up airflow-init
docker compose up -d
```
Resultados: `data/processed/` (silver/gold), `data/_staging/<corrida>/` (intermedios y estado por tarea),
`reports/orquestacion/resumen_<corrida>.md` (notificación).

En Linux, incluido GitHub Codespaces, antes de `docker compose up` ponga en `.env` `AIRFLOW_UID=` con el resultado
de `id -u`. Si no, los contenedores no pueden escribir en `data/` ni en `reports/` del repositorio.

## Sin Docker (Linux/macOS/WSL2)

Ejecuta los siguientes comandos desde la raíz del repositorio, porque las rutas de dependencias y del DAG son relativas a esa ubicación.

**Windows sin Docker no está soportado.** Las tareas de `src/orquestacion/tareas.py` escriben con Spark en disco
local, lo que en Windows exige `HADOOP_HOME` y `hadoop.dll`. A diferencia de `src/main.py`, no tienen respaldo con
pyarrow, así que fallan. En Windows use Docker Desktop (sección anterior) o WSL2.

```bash
python -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.2" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
pip install -r airflow/requirements-airflow.txt          # requiere Java 17+
export AIRFLOW_HOME=$PWD/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
airflow db migrate
airflow dags test datamarca_pipeline "$(date +%F)"     # una corrida completa en el terminal
```

## Probar el fallo en una tarea intermedia (insumo de T1.3)
```bash
export DATAMARCA_FALLAR_EN=limpiar_logs DATAMARCA_RETRY_DELAY_SEG=5 DATAMARCA_RETRIES=2
airflow dags test datamarca_pipeline "$(date +%F)"
```
`limpiar_logs` falla en todos sus intentos; `unir`, `agregar`, `persistir_silver` y `persistir_gold` quedan en
`upstream_failed`; `notificar` corre igual (`all_done`) y `corrida_completa` también queda en `upstream_failed`, por lo
que la corrida se marca fallida. `DATAMARCA_FALLAR_INTENTOS=1` hace fallar solo el primer intento para mostrar la recuperación.

Con Docker, estas variables van en `airflow/.env`. Después hay que recrear los contenedores que ejecutan y leen el DAG
para que las tomen: `docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor`. El api-server no
hace falta recrearlo, y si se recrea genera una contraseña nueva.

## Probar los timeouts (T1.3)
- **De una tarea:** `DATAMARCA_DEMORAR_EN=compuerta_calidad:300` (la compuerta tiene 2 min de timeout), con
  `DATAMARCA_RETRIES=1` y `DATAMARCA_RETRY_DELAY_SEG=5`. Airflow detiene la tarea al vencer el tiempo, la reintenta y la
  deja en `failed`; sus dependientes quedan en `upstream_failed` y el resumen la reporta como `interrumpida`.
- **De la corrida:** `DATAMARCA_DAGRUN_TIMEOUT_MIN=4` y `DATAMARCA_DEMORAR_EN=agregar:600`. A los 4 min Airflow marca la
  corrida como fallida y deja en `skipped` las tareas sin terminar, incluida `notificar` (brecha B1 del documento de T1.3).

## Exportar los logs de una corrida (evidencia)
```bash
docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py --listar
docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py \
  --run-id "<run_id>" --nombre fallo_forzado
```
Deja en `reports/orquestacion/evidencia/logs/<nombre>/` un archivo de texto por tarea e intento, con nombres válidos
también en Windows, y los estados de Airflow (`estados_airflow.txt`).

## Una tarea suelta (depuración)
```bash
python src/orquestacion/tareas.py validar_logs --run-id prueba1
```

---

<!-- T1.4-AIRFLOW-ENV-START -->
## Variables de entorno

Las variables del entorno de Airflow se configuran principalmente en `airflow/.env`, creado a partir de `airflow/.env.example`. El archivo `.env` contiene configuración local y no debe publicarse ni sustituirse si ya existe. Los valores predeterminados indicados a continuación corresponden al comportamiento del código cuando no se define la variable; cuando el archivo de ejemplo propone otro valor, se aclara en la descripción.

### Variables del pipeline DATAMARCA

| Variable | Predeterminado en el código | Función |
|---|---:|---|
| `DATAMARCA_RETRIES` | `3` | Número de reintentos adicionales permitidos por tarea. |
| `DATAMARCA_RETRY_DELAY_SEG` | `60` segundos | Tiempo inicial de espera antes de un reintento. |
| `DATAMARCA_MAX_RETRY_DELAY_SEG` | `600` segundos | Límite máximo de espera del backoff exponencial. |
| `DATAMARCA_DAGRUN_TIMEOUT_MIN` | `60` minutos | Tiempo máximo permitido para una corrida completa del DAG. |
| `UMBRAL_RECHAZO_PCT` | `0.1` (%) | Porcentaje máximo de registros rechazados permitido por fuente antes de que la compuerta de calidad detenga el flujo. |
| `DATAMARCA_STAGING_ROOT` | `data/_staging` | Directorio raíz donde se almacenan resultados intermedios, estados y métricas por corrida. |
| `DATAMARCA_SPARK_MASTER` | `local[2]` | Master de Spark utilizado por las tareas orquestadas. |
| `SHUFFLE_PARTITIONS` | `8` | Número de particiones de shuffle configurado para las tareas del pipeline orquestado. |
| `DATAMARCA_LIMPIAR_STAGING` | `false` | Si se establece en `true`, elimina los intermedios de una corrida exitosa y conserva `estado/` y `metricas/`. Las corridas fallidas se conservan. El archivo `.env.example` propone `true`. |
| `DATAMARCA_RETENCION_DIAS` | `0` o sin valor | Si es mayor que cero, elimina carpetas de corridas cuya última actividad supere ese número de días. El archivo `.env.example` propone `7`. |
| `DATAMARCA_WEBHOOK_URL` | Sin valor | URL opcional que recibe una notificación con el resumen de la corrida. |
| `DATAMARCA_FALLAR_EN` | Sin valor | Nombre de la tarea en la que se provoca un fallo controlado para las pruebas. |
| `DATAMARCA_FALLAR_INTENTOS` | `999` | Cantidad de intentos en los que se mantiene el fallo forzado. Se utiliza únicamente en pruebas. |
| `DATAMARCA_DEMORAR_EN` | Sin valor | Permite simular una demora con el formato `<tarea>:<segundos>` para probar los límites de tiempo. |

### Variables de rutas e intérprete

| Variable | Valor configurado en Docker Compose | Función |
|---|---|---|
| `DATAMARCA_HOME` | `/opt/datamarca` | Ruta raíz del repositorio dentro del contenedor. |
| `DATAMARCA_PYTHON` | `python` | Intérprete utilizado por el DAG para ejecutar `src/orquestacion/tareas.py`. |
| `AIRFLOW_UID` | `50000` en `.env.example` | Identificador de usuario de los contenedores. En Linux y GitHub Codespaces debe ajustarse al resultado de `id -u` para permitir la escritura en los directorios montados. |

`DATAMARCA_HOME` y `DATAMARCA_PYTHON` se definen directamente en `airflow/docker-compose.yaml`. Para cambiarlos se debe modificar la configuración de Compose; agregarlos a `.env` no sustituye esos valores.

### Variables internas de Airflow y PostgreSQL

Estas variables se establecen en `airflow/docker-compose.yaml` para configurar los servicios. No suelen necesitar cambios en una ejecución local habitual.

| Variable | Configuración | Función |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Define el ejecutor local de tareas de Airflow. |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Conexión al servicio `postgres` | Define la conexión de Airflow con su base de datos de metadatos. |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Evita cargar DAG de ejemplo. |
| `AIRFLOW__CORE__DAGS_FOLDER` | `/opt/datamarca/airflow/dags` | Ubicación de los DAG dentro del contenedor. |
| `AIRFLOW__CORE__DEFAULT_TIMEZONE` | `America/Bogota` | Establece la zona horaria predeterminada de Airflow. |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-apiserver:8080/execution/` | Dirección interna del API Server utilizada por el scheduler. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secreto definido para desarrollo local | Se utiliza para la autenticación. No se debe reutilizar el valor de desarrollo en producción ni publicar secretos. |
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` | `airflow:admin` | Define el usuario local de acceso al entorno de desarrollo. |
| `POSTGRES_USER` | Definido en Compose | Usuario de la base de datos PostgreSQL. |
| `POSTGRES_PASSWORD` | Definido en Compose | Contraseña de PostgreSQL para el entorno local. |
| `POSTGRES_DB` | Definido en Compose | Nombre de la base de datos utilizada por Airflow. |

Las variables de demostración `DATAMARCA_FALLAR_EN`, `DATAMARCA_FALLAR_INTENTOS` y `DATAMARCA_DEMORAR_EN` deben permanecer sin configurar durante las ejecuciones normales. No se deben guardar contraseñas, claves, tokens ni URL privadas de webhook en el repositorio.
<!-- T1.4-AIRFLOW-ENV-END -->
