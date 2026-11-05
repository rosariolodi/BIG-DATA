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
| Timeout | 15 min por defecto; 5 min ingestión; 2 min compuerta; 20 min persistencia; 1 h la corrida | `execution_timeout`, `dagrun_timeout` |

## Ejecutar con Docker (recomendado, también en Windows)
```bash
cd airflow
cp .env.example .env
docker compose build
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080 -> activar el DAG `datamarca_pipeline` y pulsar "Trigger"
```
Resultados: `data/processed/` (silver/gold), `data/_staging/<corrida>/` (intermedios y estado por tarea),
`reports/orquestacion/resumen_<corrida>.md` (notificación).

## Sin Docker (Linux/macOS/WSL2)
```bash
python -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.2" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
pip install -r airflow/requirements-airflow.txt          # requiere Java 17+
export AIRFLOW_HOME=$PWD/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
airflow db migrate
airflow dags test datamarca_pipeline 2026-11-05     # una corrida completa en el terminal
```

## Probar el fallo en una tarea intermedia (insumo de T1.3)
```bash
export DATAMARCA_FALLAR_EN=limpiar_logs DATAMARCA_RETRY_DELAY_SEG=5 DATAMARCA_RETRIES=2
airflow dags test datamarca_pipeline 2026-11-05
```
`limpiar_logs` falla en todos sus intentos; `unir`, `agregar`, `persistir_silver` y `persistir_gold` quedan en
`upstream_failed`; `notificar` corre igual (`all_done`) y `corrida_completa` también queda en `upstream_failed`, por lo
que la corrida se marca fallida. `DATAMARCA_FALLAR_INTENTOS=1` hace fallar solo el primer intento para mostrar la recuperación.

## Una tarea suelta (depuración)
```bash
python src/orquestacion/tareas.py validar_logs --run-id prueba1
```
