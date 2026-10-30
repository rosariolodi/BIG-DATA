# Logs de tiempos de ejecución (tarea 5.2)

Cada ejecución de `src/main.py` escribe aquí:

| Archivo | Contenido |
|---|---|
| `tiempos_<AAAAMMDD_HHMMSS>.json` | Detalle de la corrida: tiempo total, tiempo por etapa y subetapa, filas, estado, entorno (Spark, núcleos, zona horaria) y volumen leído |
| `historial_tiempos.csv` | Una fila por subetapa de todas las corridas, para comparar volúmenes (tarea 5.1) |

Estos archivos se regeneran en cada ejecución y no se versionan. Para dejar una corrida como **evidencia** en el repositorio, cópiala a `reports/tiempos/evidencia/`.

## Etapas

| Etapa | Subetapas |
|---|---|
| `ingestion` | `transacciones_csv`, `logs_json` (lectura con esquema estricto + validación de calidad, incluye guardar la cuarentena) |
| `transformacion` | `limpieza_transacciones`, `limpieza_logs`, `join`, `agregaciones` |
| `persistencia` | `kpis_negocio_categoria`, `agregaciones_adicionales` (escritura de los Parquet) |

Spark evalúa de forma perezosa, por eso cada subetapa termina forzando la ejecución de su resultado (`cache` + `count`). Así el tiempo se atribuye a la etapa que hizo el trabajo.
