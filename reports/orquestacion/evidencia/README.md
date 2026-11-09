# Evidencia de la orquestación con Airflow (Unidad 4)

## Cierre de Joel — 8 de noviembre de 2026

[Canal de Teams, prueba real de B1 y limpieza de archivos locales](cierre_joel_20261108/README.md). Incluye corridas de éxito, fallo y vencimiento, recibos filtrados y capturas. Las rondas anteriores se conservan como evidencia histórica. La integración en main queda pendiente por decisión de Joel.

Corridas del DAG `datamarca_pipeline` en Docker con Airflow 3.3.2. Las horas son de Bogotá (UTC−5), salvo que se indique UTC.

| Ronda | Fecha | Entorno y rama | Qué aporta |
|---|---|---|---|
| 1 | 6 de noviembre de 2026 | Docker local, rama `ivanna` | Resúmenes de `notificar` y backoff medido en los logs (tarea T1.2) |
| 2 | 7 de noviembre de 2026 | GitHub Codespaces, rama `kinan` | Capturas de Airflow: estados por tarea y Auditoría de Log de una corrida exitosa y una con fallo forzado (tarea T1.3) |

La ronda 2 no repite los resúmenes de la ronda 1. Agrega lo que faltaba: lo que Airflow registra de cada tarea.

## Ronda 1: resúmenes de `notificar`

| Archivo | Qué muestra |
|---|---|
| `corrida_exitosa.md` | Corrida completa: las 12 tareas más `corrida_completa` en success. 5.000 filas leídas y 0 rechazadas por fuente |
| `fallo_forzado_limpiar_logs.md` | Fallo forzado en una tarea intermedia (`DATAMARCA_FALLAR_EN=limpiar_logs`). `limpiar_logs` falla tras sus reintentos; `unir`, `agregar`, `persistir_silver`, `persistir_gold` y `corrida_completa` quedan en `upstream_failed`; `notificar` corre igual |

### Backoff exponencial medido

Corrida del fallo forzado, con `DATAMARCA_RETRIES=3` y `DATAMARCA_RETRY_DELAY_SEG=60`. Hora de inicio de cada intento de `limpiar_logs`, tomada de los logs de Airflow:

| Intento | Inicio (UTC) | Espera desde el anterior |
|---|---|---|
| 1 | 02:23:21 | (primer intento) |
| 2 | 02:24:52 | 91 s |
| 3 | 02:27:54 | 183 s |
| 4 | 02:34:14 | 380 s |

Las esperas casi se duplican en cada reintento. No son exactamente 60, 120 y 240 s: cada una cae en un rango que dobla al anterior (60 a 120, 120 a 240 y 240 a 480 s), lo que concuerda con un backoff exponencial con variación aleatoria. Esa explicación de la variación es la interpretación del equipo y no se verificó en la documentación de Airflow.

## Ronda 2: capturas de Airflow (T1.3)

Configuración: la de `airflow/.env.example`, que incluye `DATAMARCA_RETRIES=3` y `DATAMARCA_RETRY_DELAY_SEG=60`. La corrida con fallo agrega `DATAMARCA_FALLAR_EN=limpiar_logs`. Las dos corridas las disparó el usuario `airflow` con *Trigger*. En la Auditoría de Log, la columna *Usuario* muestra `datamarca`, que es el `owner` del DAG.

| Corrida | `run_id` | Inicio → fin | Duración | Resultado |
|---|---|---|---|---|
| Exitosa | `manual__2026-11-07T16:03:53.292541+00:00` | 11:05:07 → 11:06:41 | 1 min 33 s | **success**: las 13 tareas en success, todas en el primer intento |
| Con fallo forzado | `manual__2026-11-07T16:18:40.768797+00:00` | 11:18:41 → 11:29:05 | 10 min 24 s | **failed**: 7 tareas en success, 1 en failed y 5 en upstream_failed |

### Capturas (`capturas/`)

| Archivo | Vista de Airflow | Qué se ve |
|---|---|---|
| `corrida_exitosa_1_tareas.png`, `corrida_exitosa_2_tareas.png` | Instancias de Tarea, más la grilla a la izquierda | Las 13 tareas en *Exitoso*, con intento 1, operador y duración. En la grilla, los grupos en verde |
| `corrida_exitosa_3_auditoria.png` a `corrida_exitosa_6_auditoria.png` | Auditoría de Log | Cada tarea pasa de `running` a `success`, en orden y con su hora, de 11:05:08 a 11:06:41 |
| `corrida_fallida_1_tareas.png`, `corrida_fallida_2_tareas.png` | Instancias de Tarea, más la grilla | `limpiar_logs` en *Fallido* en el intento 4; las tareas que dependen de ella en *Fallido en Upstream* con intento 0; `notificar` y las seis tareas previas en *Exitoso*. En la grilla, `transformacion` en rojo y `persistencia` y `corrida_completa` en naranja |
| `corrida_fallida_3_auditoria.png` a `corrida_fallida_6_auditoria.png` | Auditoría de Log | Los cuatro intentos de `limpiar_logs` (tres `up_for_retry` y un `failed`), `limpiar_transacciones` terminando durante el fallo y `notificar` al final |

### Estado de cada tarea

Duración según Airflow (incluye el arranque del proceso). La duración de cada tarea en la corrida con fallo queda fuera de las capturas.

| Tarea | Exitosa | Duración | Con fallo forzado | Intento |
|---|---|---|---|---|
| `ingestion.ingestar_transacciones` | success | 0,4 s | success | 1 |
| `ingestion.ingestar_logs` | success | 0,2 s | success | 1 |
| `validacion.validar_transacciones` | success | 22,4 s | success | 1 |
| `validacion.validar_logs` | success | 21,8 s | success | 1 |
| `validacion.compuerta_calidad` | success | 0,2 s | success | 1 |
| `transformacion.limpiar_transacciones` | success | 15,5 s | success | 1 |
| `transformacion.limpiar_logs` | success | 14,9 s | **failed** | 4 |
| `transformacion.unir` | success | 19,4 s | upstream_failed | 0 |
| `transformacion.agregar` | success | 16,8 s | upstream_failed | 0 |
| `persistencia.persistir_silver` | success | 21,8 s | upstream_failed¹ | — |
| `persistencia.persistir_gold` | success | 13,5 s | upstream_failed | 0 |
| `notificar` | success | 0,2 s | success | 1 |
| `corrida_completa` | success | — | upstream_failed | 0 |

¹ Su fila queda cortada al pie de `corrida_fallida_1_tareas.png`. El estado se deduce del grupo `persistencia` en naranja en la grilla y de que son 13 instancias en total.

Intento 0 quiere decir que la tarea nunca se ejecutó: Airflow la canceló porque falló una tarea de la que depende.

### Línea de tiempo de la corrida con fallo

Tomada de la Auditoría de Log. Las horas de `upstream_failed` vienen de Instancias de Tarea.

| Hora | Qué pasó |
|---|---|
| 11:18:41 | `ingestar_transacciones` e `ingestar_logs` terminan |
| 11:18:42 → 11:19:05 | `validar_transacciones` y `validar_logs`, en paralelo |
| 11:19:06 | `compuerta_calidad` termina |
| 11:19:07 | Arrancan `limpiar_logs` (intento 1) y `limpiar_transacciones`. `limpiar_logs` pasa a `up_for_retry` en el mismo segundo, porque el fallo forzado ocurre antes de abrir Spark |
| 11:19:16 | `limpiar_transacciones` termina en success: la rama paralela completa su trabajo durante el fallo |
| 11:20:43 | `limpiar_logs`, intento 2 → `up_for_retry` |
| 11:24:15 | `limpiar_logs`, intento 3 → `up_for_retry` |
| 11:29:01 | `limpiar_logs`, intento 4 → `failed`. `unir` queda en `upstream_failed` |
| 11:29:02 → 11:29:03 | `agregar`, `corrida_completa` y `persistir_gold` quedan en `upstream_failed` |
| 11:29:04 | `notificar` corre y termina en success (`trigger_rule="all_done"`) |
| 11:29:05 | La corrida termina en failed |

### Backoff exponencial, segunda medición

| Intento de `limpiar_logs` | Inicio | Espera desde el fallo anterior | Rango de la ronda 1 |
|---|---|---|---|
| 1 | 11:19:07 | (primer intento) | — |
| 2 | 11:20:43 | 96 s | 60 a 120 s |
| 3 | 11:24:15 | 212 s | 120 a 240 s |
| 4 | 11:29:01 | 286 s | 240 a 480 s |

Las tres esperas vuelven a caer en los rangos de la ronda 1. No llegan al tope de 600 s. Los reintentos ocupan 9 min 54 s de los 10 min 24 s de la corrida (de 11:19:07 a 11:29:01).

### Qué demuestra para la T1.3

- **Estados controlados.** La tarea que falla queda en `failed` solo después de agotar sus reintentos. Las que dependen de ella no se ejecutan (intento 0). La corrida queda en failed por `corrida_completa`, aunque `notificar` termine bien. El error no se propaga en silencio.
- **Qué se canceló y qué terminó antes del fallo.** Canceladas: `unir`, `agregar`, `persistir_silver`, `persistir_gold` y `corrida_completa`. Terminadas: las dos de ingestión, las tres de validación y `limpiar_transacciones`, la rama paralela que terminó a las 11:19:16, durante el fallo.
- **Aviso ante el fallo.** `notificar` corre aunque haya un fallo (`all_done`) y lo hace una sola vez (`retries=0`).
- **Trazas auditables.** La Auditoría de Log registra cada cambio de estado con hora, tarea e intento. La cabecera de la corrida dice quién la disparó, cuándo y con qué versión del DAG.

### Lo que estas capturas no muestran

- **La vista *Graph*.** Las capturas usan *Instancias de Tarea* y la grilla, que muestran el mismo estado por tarea, pero no el grafo de dependencias.
- **El texto de los logs de cada tarea.** Falta, en `limpiar_logs`, el traceback con `FALLO FORZADO`, `REINTENTO` y `FALLO DEFINITIVO`, y en `notificar` el resumen con "No se ejecutaron por la falla". El resumen equivalente de la ronda 1 está en `fallo_forzado_limpiar_logs.md`.
- **La retención de `data/_staging/`, las pruebas con pyspark en el contenedor y los timeouts.**

Cómo completarlos: `docs/orquestacion/manejo_errores_logging.md`, secciones 7 y 10.

## Límites de esta evidencia

- Son corridas de un conjunto pequeño (5.000 filas por fuente) en entornos de desarrollo. No sirven para extrapolar tiempos a producción.
- Los tiempos varían entre corridas. En la ronda 1, el tiempo acumulado de las tareas según `notificar` fue de 148 s en la primera corrida exitosa y de 187 s en la última. En la ronda 2, las duraciones de Airflow de las 11 tareas de datos suman 147 s; no es la misma medida, porque incluye el arranque de cada proceso. No se investigó la causa de la variación.
- En la corrida exitosa de la ronda 2 pasan 1 min 34 s entre la fecha lógica (11:03:33) y el inicio (11:05:07). La grilla muestra una corrida anterior en verde. Con `max_active_runs=1`, una corrida manual espera a que termine la anterior; esta explicación es deducida, porque no hay captura de esa corrida.
- No se probaron los timeouts ni la ejecución del schedule de las 02:00 con un reloj distinto.
