# Evidencia de la orquestación con Airflow (Unidad 4)

Corridas del DAG `datamarca_pipeline` hechas en Docker (Airflow 3.3.2) el 6 de noviembre de 2026 (hora de Bogotá), desde la rama `ivanna`. Los resúmenes los genera la tarea `notificar` (`src/orquestacion/tareas.py`).

| Archivo | Qué muestra |
|---|---|
| `corrida_exitosa.md` | Corrida completa: las 12 tareas más `corrida_completa` en success. 5.000 filas leídas y 0 rechazadas por fuente |
| `fallo_forzado_limpiar_logs.md` | Fallo forzado en una tarea intermedia (`DATAMARCA_FALLAR_EN=limpiar_logs`). `limpiar_logs` falla tras sus reintentos, `unir`, `agregar`, `persistir_silver`, `persistir_gold` y `corrida_completa` quedan en `upstream_failed` y `notificar` corre igual |

## Backoff exponencial medido

Corrida del fallo forzado, con `DATAMARCA_RETRIES=3` y `DATAMARCA_RETRY_DELAY_SEG=60`. Hora de inicio de cada intento de `limpiar_logs`, tomada de los logs de Airflow:

| Intento | Inicio (UTC) | Espera desde el anterior |
|---|---|---|
| 1 | 02:23:21 | (primer intento) |
| 2 | 02:24:52 | 91 s |
| 3 | 02:27:54 | 183 s |
| 4 | 02:34:14 | 380 s |

Las esperas casi se duplican en cada reintento. No son exactamente 60, 120 y 240 s: cada una cae dentro de un rango que dobla al anterior (60 a 120, 120 a 240 y 240 a 480 s), lo que concuerda con un backoff exponencial con variación aleatoria. Esa explicación de la variación es la interpretación del equipo y no se verificó en la documentación de Airflow.

## Límites de esta evidencia

- Son corridas de un conjunto pequeño (5.000 filas por fuente), en un equipo local. No sirven para extrapolar tiempos a producción.
- Los tiempos varían entre corridas (la suma de las tareas fue de 148 s en la primera corrida exitosa y de 187 s en la última). No se investigó la causa.
- No se probaron los timeouts ni la ejecución del schedule de las 02:00 con un reloj distinto.
