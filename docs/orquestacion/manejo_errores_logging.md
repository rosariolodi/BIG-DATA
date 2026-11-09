# Manejo de errores y logging (Unidad 4, tarea T1.3)

## Actualización de cierre de Joel — 8 de noviembre de 2026

B5 tiene un canal externo operativo: Microsoft Teams, **DATAMARCA - Proyecto Big Data / Alertas - Pipeline DATAMARCA**, verificado con una corrida exitosa y otra con fallo. B1 tiene un respaldo implementado y probado con un vencimiento real de un minuto: `on_failure_callback` envía el aviso de `dagrun_timeout` aunque Airflow deje `notificar` en `skipped`. No se afirma que la tarea cancelada se ejecute ni que ese aviso genere el resumen completo. El límite normal de 60 minutos quedó restaurado.

La retirada del seguimiento de `job_listo.json` y `tmp_tiempos/` conserva las copias locales. Joel confirmó el visto bueno de Rosario. Los cambios se integraron en main mediante el PR #34, commit `1728d23`, el 8 de noviembre de 2026 (Bogotá). El resto del documento conserva el diagnóstico y pendientes históricos de T1.3: las referencias antiguas a B1 y B5 como no implementadas se leen a la luz de esta actualización.

Evidencia y alcance: [cierre de Joel](../../reports/orquestacion/evidencia/cierre_joel_20261108/README.md).


Proyecto Spark Pipeline - DATAMARCA S.A. · Responsable: Kinan

Este documento:
- revisa el manejo de errores y las trazas del DAG `datamarca_pipeline` frente a los pasos 3 y 4 de la guía de la Unidad 4;
- dice qué falta;
- fija e implementa la retención de la zona de trabajo (`data/_staging/`);
- explica cómo probar los timeouts y cómo reproducir la evidencia.

El código revisado es `src/orquestacion/tareas.py` y `airflow/dags/datamarca_pipeline.py` (tarea T1.2). La evidencia de las corridas está en `reports/orquestacion/evidencia/`.

Convención:
- **Verificado:** comprobado en una corrida de Airflow o en `tests/test_orquestacion.py`.
- **Confirmado en el código de Airflow:** leído en el código fuente de Airflow 3, sin ejecutarlo aquí.
- **Deducido:** leído del código del proyecto, sin ejecutarlo.

## 1. Qué pide la guía y dónde se cumple

| Requisito de la guía | Dónde está | Evidencia | Estado |
|---|---|---|---|
| Paso 3: reintentos con backoff exponencial | `default_args` del DAG: `retries=3`, `retry_delay=60 s`, `retry_exponential_backoff=True`, tope 600 s | `evidencia/README.md`: esperas de 91, 183 y 380 s entre intentos (ronda 1) y de 96, 212 y 286 s (ronda 2, Auditoría de Log) | Cumple (verificado dos veces) |
| Paso 3: alertas ante fallo | `on_retry_callback` y `on_failure_callback` (log WARNING/ERROR); `notificar` (log ERROR, resumen y webhook opcional) | `evidencia/fallo_forzado_limpiar_logs.md`; `notificar` en success al final de la corrida fallida (`evidencia/capturas/corrida_fallida_3_auditoria.png`) | Cumple en parte: sin canal externo configurado (B5) y sin aviso si vence el tiempo de la corrida (B1) |
| Paso 3: estados controlados, sin propagación silenciosa | `ejecutar()` devuelve código 1; dependientes en `upstream_failed`; `notificar` con `all_done`; `corrida_completa` marca la corrida como fallida | `evidencia/fallo_forzado_limpiar_logs.md`, `evidencia/capturas/corrida_fallida_*.png` y pruebas | Cumple (verificado en Airflow) |
| Paso 3: manejo explícito de excepciones | Sección 2 | `tests/test_orquestacion.py` | Cumple (verificado) |
| Paso 2: timeout | `execution_timeout` por tarea (2 a 20 min) y `dagrun_timeout` (60 min, configurable) | Sección 6 | Comportamiento confirmado en el código de Airflow; falta la corrida de prueba |
| Paso 4: trazas completas y auditables | Log por intento, estado por tarea, métricas y resumen por corrida (sección 4) | `evidencia/*.md` y Auditoría de Log de las dos corridas (`evidencia/capturas/`) | Cumple, con brechas B3, B7 y B8 |
| Paso 4: corrida exitosa y fallo forzado en una tarea intermedia | `DATAMARCA_FALLAR_EN=limpiar_logs` (tarea 7 de 12) | Ronda 1: `evidencia/corrida_exitosa.md` y `evidencia/fallo_forzado_limpiar_logs.md`. Ronda 2: `evidencia/capturas/corrida_exitosa_*.png` y `corrida_fallida_*.png` | Cumple. Faltan la vista *Graph* y el texto de los logs de `limpiar_logs` y `notificar` (sección 10) |
| Recomendación: el log muestra qué tareas se cancelaron y cuáles terminaron antes del fallo | Resumen de `notificar`: "No se ejecutaron por la falla" y "Completadas antes/durante el fallo". En Airflow: intento 0 y `upstream_failed` en las canceladas | `evidencia/fallo_forzado_limpiar_logs.md` y `evidencia/capturas/corrida_fallida_1_tareas.png`, `corrida_fallida_2_tareas.png` | Cumple (verificado) |

## 2. Cómo maneja los errores `tareas.py`

Cada tarea del DAG es un `BashOperator` que ejecuta `python src/orquestacion/tareas.py <tarea>` en su propio proceso.

| # | Mecanismo | Dónde | Qué evita |
|---|---|---|---|
| 1 | **Envoltorio único `ejecutar()`.** Captura cualquier excepción, la escribe en el log con su traceback, guarda el estado `fallida` con el tipo y el mensaje y devuelve código 1 | `ejecutar()` | Que una tarea termine en *success* con un error dentro. Con código 1, Airflow la marca fallida y aplica los reintentos |
| 2 | **Estado `en_ejecucion` antes de correr.** Si Airflow detiene el proceso (timeout), el estado queda sin cierre y `notificar` lo reporta como `interrumpida` | `ejecutar()`, `clasificar()` | Perder el rastro de una tarea terminada a la fuerza |
| 3 | **Fallar rápido en la ingestión.** Verifica que el origen exista, no esté vacío y tenga las columnas (CSV) o la estructura (JSON) esperadas, antes de abrir Spark | `ingestar_transacciones()`, `ingestar_logs()` | Gastar una sesión de Spark en un origen roto |
| 4 | **Excepción de negocio propia.** `CompuertaCalidadError` separa el fallo de calidad del fallo técnico | `compuerta_calidad()` | Confundir datos malos con una caída del sistema |
| 5 | **Error explícito si falta un resultado intermedio** (`FileNotFoundError` con la ruta) | `leer_stage()` | Un error opaco de Spark al leer una ruta inexistente |
| 6 | **`finally: spark.stop()`** en todas las tareas con Spark | Validación, transformación y persistencia | Dejar la JVM viva tras un fallo |
| 7 | **La notificación aísla sus errores.** Resumen, webhook y retención van cada uno en su propio `try`; además `trigger_rule="all_done"` y `retries=0` | `notificar()` y el DAG | Que un fallo al avisar cambie el resultado, que un paso impida los demás, o que no se avise justo cuando algo falla |
| 8 | **`corrida_completa`** (`EmptyOperator`) depende de silver y gold y queda en `upstream_failed` si falla cualquier etapa de datos | DAG | Que la corrida se vea exitosa porque `notificar` terminó bien |
| 9 | **Ganchos de demostración.** `DATAMARCA_FALLAR_EN` y `DATAMARCA_FALLAR_INTENTOS` fuerzan un fallo; `DATAMARCA_DEMORAR_EN` demora una tarea. Mientras alguno está activo, cada tarea lo avisa con un WARNING y el resumen lo indica | `_fallo_forzado()`, `_demora_forzada()` | Que una variable de prueba olvidada pase por un fallo real |

**Verificación.** `tests/test_orquestacion.py` prueba estos mecanismos y la retención (sección 5) sin Spark ni Airflow. Para eso reemplaza las funciones de negocio y redirige la zona de trabajo a una carpeta temporal.
- **Sin Spark** (Python 3.15, pytest 9.2): 33 de los 34 casos pasan.
- **El caso restante** (columnas faltantes del CSV) necesita pyspark. Se ejecuta en el contenedor de Airflow (sección 7, paso 3).

```bash
pytest tests/test_orquestacion.py -v
```

**Conclusión.** El manejo explícito de excepciones cumple lo que pide la guía: ningún error se pierde, cada fallo deja su traza y las tareas dependientes no se ejecutan. Lo que falta está en los bordes, en la sección 3.

## 3. Qué falta (brechas priorizadas)

| Id | Brecha | Efecto | Propuesta | Archivo (dueño) | Estado |
|---|---|---|---|---|---|
| B1 | **Sin aviso si vence `dagrun_timeout`.** Al vencer, el scheduler marca la corrida como fallida y deja en `skipped` todas las tareas sin terminar, incluida `notificar` (confirmado en el código de Airflow 3, `_schedule_dag_run`). Solo avisa si el DAG tiene `on_failure_callback`, y este DAG no lo tiene | Un cuelgue, justo el caso que el timeout busca atrapar, termina sin resumen ni aviso. Los timeouts de la ruta crítica suman 87 min (5 + 15 + 2 + 15 + 15 + 15 + 20), más que el límite de 60 min de la corrida | Agregar `on_failure_callback` en el `DAG(...)`. Airflow lo llama con la razón `timed_out`; desde ahí, ejecutar `tareas.py notificar` para la corrida | DAG (Rosario) | Alta. Se demuestra con la prueba B de la sección 6 |
| B2 | **Los errores permanentes se reintentan igual que los transitorios.** `compuerta_calidad` hereda `retries=3`, pero reintentar no cambia unas métricas ya calculadas | El aviso de un fallo de calidad llegaría unos 10 min tarde. Con la misma configuración, los cuatro intentos de `limpiar_logs` fueron de 02:23:21 a 02:34:14 (UTC) en la ronda 1 y de 11:19:07 a 11:29:01 en la ronda 2 | `retries=0` en `compuerta_calidad`. En la ingestión se mantienen los reintentos: un archivo que aún no llega es un fallo transitorio | DAG (Rosario) | Media. Propuesto |
| B3 | **El historial de intentos se sobrescribe.** `estado/<tarea>.json` guarda solo el último intento | Si una tarea falla y se recupera en el reintento, el resumen solo dice "exitosa, intento 2". El error del intento 1 queda únicamente en el log de Airflow | Una línea por intento en `estado/historial.jsonl` y, en el resumen, los intentos fallidos previos | `tareas.py` (Rosario) | Media. Propuesto |
| B4 | **La limpieza de la zona de trabajo compartía el `try` del webhook** | Si el webhook fallaba, una corrida exitosa no limpiaba sus intermedios | Cada paso de `notificar` en su propio `try` | `tareas.py` | **Resuelto en T1.3** (prueba `test_la_retencion_no_depende_de_que_el_webhook_funcione`) |
| B5 | **No hay canal de alerta externo por defecto.** Los callbacks solo escriben en el log y `DATAMARCA_WEBHOOK_URL` es opcional | En producción, un fallo solo se ve si alguien abre Airflow | Definir el webhook (Teams o Slack) como secreto del entorno de producción, o SMTP en Airflow | Decisión del equipo | Media. Propuesto |
| B6 | **El estado no se escribe de forma atómica y `notificar` no tolera un JSON ilegible** | Si un proceso muere mientras escribe su estado, `notificar` falla al leerlo y la corrida queda sin resumen | Escribir en un archivo temporal y renombrar (`os.replace`); en `leer_estados()`, capturar `JSONDecodeError` | `tareas.py` (Rosario) | Baja. Propuesto |
| B7 | **Los logs de Airflow viven en un volumen local de Docker** (`airflow-logs`) | `docker compose down -v` los borra | En producción, logging remoto de Airflow hacia el almacenamiento, con retención. Para la entrega, `reports/orquestacion/exportar_logs.py` (sección 7) | `docker-compose.yaml` (Rosario) | Mitigado para la entrega |
| B8 | **La cuarentena no es por corrida.** `guardar_resultado_validacion` escribe en `data/quarantine/cuarentena_<fuente>.parquet` y la corrida siguiente lo sobrescribe | Si la compuerta detiene una corrida, los registros rechazados se pierden en la próxima ejecución diaria | Guardar también la cuarentena en la zona de trabajo de la corrida, que ya tiene retención (sección 5) | `tareas.py` (Rosario) | Media. Propuesto |
| B9 | **Los ganchos de demostración no avisaban** | Una variable olvidada hacía fallar la tarea todos los días con un mensaje que parecía real | WARNING en cada tarea y línea en el resumen mientras estén activos | `tareas.py` | **Resuelto en T1.3** |

## 4. Trazas auditables

### 4.1 Dónde queda cada traza

| Traza | Dónde | Qué contiene | Quién la escribe | Hasta cuándo |
|---|---|---|---|---|
| Log de cada intento | Airflow: tarea → *Logs*. En disco: volumen `airflow-logs`, `dag_id=datamarca_pipeline/run_id=<corrida>/task_id=<tarea>/attempt=<n>.log` (una línea JSON por evento) | `INICIO`, `FIN ... OK` o `FALLO ...` con traceback, filas de cada resultado intermedio, métricas de calidad, avisos `REINTENTO`, `FALLO DEFINITIVO` y de modo demostración | `tareas.py`, callbacks del DAG y Airflow | Mientras exista el volumen. Para la evidencia se exportan con `exportar_logs.py` |
| Estado por tarea | `data/_staging/<corrida>/estado/<tarea>.json` | Estado, intento, inicio, segundos y error o detalle | `ejecutar()` | Según la retención (sección 5) |
| Métricas de calidad | `data/_staging/<corrida>/metricas/<fuente>.json` | Leídos, válidos, rechazados y % de rechazo | `validar_*` | Según la retención (sección 5) |
| Resumen de la corrida | `reports/orquestacion/resumen_<corrida>.md` y `.json`, y el log de `notificar` | Resultado, modo demostración, tareas fallidas o interrumpidas, no ejecutadas y completadas, tabla por tarea | `notificar()` | Permanente, fuera de la zona de trabajo. No se versiona salvo como evidencia |
| Estados y eventos de Airflow | UI: *Grid*, *Graph*, *Task Instances*, *Audit Log*; CLI `airflow tasks states-for-dag-run` | Estado de cada intento (`running`, `up_for_retry`, `failed`, `upstream_failed`, `skipped`, `success`), quién disparó la corrida y cuándo | Metastore de Airflow (PostgreSQL) | Mientras exista el volumen `pgdata`. `exportar_logs.py` guarda los estados finales |

### 4.2 Qué debe mostrar la corrida con fallo forzado

Lista para revisar los logs en Airflow. Configuración: `DATAMARCA_FALLAR_EN=limpiar_logs`. Todas las tareas muestran además el WARNING "Modo demostración activo".

| Tarea | Estado esperado | Qué debe verse en su log |
|---|---|---|
| `ingestar_*`, `validar_*`, `compuerta_calidad` | `success` | `INICIO` y `FIN ... OK`. En validación, las métricas de calidad y las filas escritas |
| `limpiar_transacciones` | `success` | `FIN limpiar_transacciones OK`: la rama paralela terminó antes o durante el fallo |
| `limpiar_logs`, intentos anteriores al último | `up_for_retry` | `FALLO limpiar_logs ... FALLO FORZADO`, el traceback, el código de salida 1 y `REINTENTO: tarea=limpiar_logs intento=<n>` |
| `limpiar_logs`, último intento | `failed` | Lo anterior y `FALLO DEFINITIVO: ... -> las tareas dependientes quedarán en upstream_failed` |
| `unir`, `agregar`, `persistir_silver`, `persistir_gold`, `corrida_completa` | `upstream_failed` | **Sin log**, porque nunca se ejecutaron. La cancelación se ve en el Graph, en `estados_airflow.txt` y en el resumen de `notificar` |
| `notificar` | `success` | ERROR `NOTIFICACIÓN` con `CON FALLO`, `Modo demostración: DATAMARCA_FALLAR_EN=limpiar_logs`, `FALLÓ limpiar_logs (intento <n>)`, "No se ejecutaron por la falla: unir, agregar, persistir_silver, persistir_gold" y "Completadas antes/durante el fallo: ... limpiar_transacciones" |
| La corrida | `failed` | Por `corrida_completa` |

En la corrida exitosa:
- Todas las tareas y la corrida quedan en `success`.
- El log de `notificar` muestra un INFO `NOTIFICACIÓN` con `EXITOSA` y las filas leídas, válidas y rechazadas de cada fuente.
- Con `DATAMARCA_LIMPIAR_STAGING=true`, el log muestra `Retención: intermedios borrados de la corrida ...`.

### 4.3 Revisión de la evidencia

**Ronda 1 (6 de noviembre, resúmenes de `notificar`).**
- `evidencia/fallo_forzado_limpiar_logs.md` coincide con la fila T6 o T7 de `docs/orquestacion/dependencias.md` (sección 5):
  - falla `limpiar_logs` en el intento 4;
  - no se ejecutan `unir`, `agregar`, `persistir_silver` ni `persistir_gold`;
  - las seis tareas previas, incluida la rama paralela `limpiar_transacciones`, quedan completadas.
- El resumen de `notificar` deriva el estado de cada tarea de sus archivos de estado, no del metastore de Airflow. Por eso usa "no_ejecutada" donde Airflow muestra `upstream_failed`. La equivalencia está en `clasificar()` y la verifican las pruebas.

**Ronda 2 (7 de noviembre, capturas de Airflow en GitHub Codespaces).** La lista de la sección 4.2, comparada con lo que muestran `evidencia/capturas/corrida_fallida_*.png`:

| Esperado (4.2) | Observado | Captura |
|---|---|---|
| `ingestar_*`, `validar_*` y `compuerta_calidad` en `success` | Sí, las cinco en el primer intento, de 11:18:41 a 11:19:06 | `_2_tareas`, `_5_auditoria`, `_6_auditoria` |
| `limpiar_transacciones` en `success`, durante el fallo | Sí, de 11:19:07 a 11:19:16, mientras `limpiar_logs` ya esperaba su reintento | `_2_tareas`, `_4_auditoria` |
| `limpiar_logs`: `up_for_retry` en los intentos 1 a 3 y `failed` en el 4 | Sí: `up_for_retry` a las 11:19:07, 11:20:43 y 11:24:15; `failed` a las 11:29:01 | `_1_tareas`, `_3_auditoria`, `_4_auditoria` |
| `unir`, `agregar`, `persistir_silver`, `persistir_gold` y `corrida_completa` en `upstream_failed`, sin log | Sí, con intento 0: nunca se ejecutaron. La fila de `persistir_silver` queda cortada; su estado se deduce de la grilla | `_1_tareas`, `_2_tareas` |
| `notificar` en `success` | Sí, a las 11:29:04, en el primer intento | `_1_tareas`, `_3_auditoria` |
| La corrida en `failed` | Sí, 10 min 24 s | Todas |
| En los logs: `FALLO FORZADO` con traceback, `REINTENTO`, `FALLO DEFINITIVO`, `NOTIFICACIÓN` y "Modo demostración activo" | **No capturado** | Pendiente (sección 10) |

En la corrida exitosa (`evidencia/capturas/corrida_exitosa_*.png`), las 13 tareas quedan en `success` en el primer intento y la corrida dura 1 min 33 s. No se capturaron el log de `notificar` ni la limpieza de `data/_staging/`.

Conclusión: los estados y su orden en Airflow coinciden con lo esperado en todas las filas. Falta la evidencia del contenido de los logs, que en la ronda 1 se cubrió con el resumen de `notificar`.

## 5. Retención de la zona de trabajo (`data/_staging/`)

Cada corrida deja en `data/_staging/<corrida>/`:
- sus archivos de estado y métricas, JSON de pocos KB;
- los resultados intermedios en Parquet (`validos_*`, `limpio_*`, `unida`, `gold_*`).

Los intermedios son copias de los datos de la corrida. Con 5.000 filas por fuente pesan poco, pero con el volumen de producción ocuparían varias veces la entrada en cada corrida.

### 5.1 Decisión

| Corrida | Qué se borra | Cuándo | Por qué |
|---|---|---|---|
| Exitosa | Los Parquet intermedios. Se conservan `estado/` y `metricas/` | Al terminar `notificar` | Se pueden regenerar: las tareas son idempotentes y el origen sigue en `data/raw/` o `bronze`. Los datos finales ya están en silver y gold |
| Fallida | Nada al terminar | Se conserva completa **7 días** | Se puede reanudar con *Clear* la tarea fallida dentro de la misma corrida, sin rehacer las anteriores, porque la zona de trabajo es por `run_id`. También deja investigar con los válidos y las métricas. 7 días cubren un fin de semana y dan margen sobre la frecuencia diaria |
| Cualquiera, con más de 7 días | La carpeta completa | En cada `notificar` (diario) | Lo que se debe conservar ya está fuera: el resumen en `reports/orquestacion/` y los logs en Airflow o en la evidencia exportada |

### 5.2 Implementación

| Parte | Cómo | Variable | Valor en `.env.example` | Defecto en el código |
|---|---|---|---|---|
| Limpiar los intermedios de una corrida exitosa | `aplicar_retencion()`. Ya existía; ahora tiene su propio `try` (B4) | `DATAMARCA_LIMPIAR_STAGING` | `true` | `false` |
| Purga por antigüedad | `aplicar_retencion()` borra las carpetas de **otras** corridas cuya última escritura en `estado/` tenga más de N días. Solo toca carpetas con `estado/`, para no borrar nada ajeno si `DATAMARCA_STAGING_ROOT` apunta a otro lugar | `DATAMARCA_RETENCION_DIAS` | `7` | `0` (no purga) |

Los valores por defecto del código no borran nada: la política se activa en el entorno, igual que antes. Cada borrado queda en el log de `notificar` (`Retención: ...`). Las pruebas cubren estos casos:
- la corrida exitosa se limpia;
- la fallida se conserva;
- se purga solo lo más antiguo que el plazo;
- la corrida actual y las carpetas ajenas nunca se purgan;
- sin plazo no se purga nada;
- la retención no depende del webhook.

En Azure, la purga por antigüedad se puede hacer sin código, con una regla de ciclo de vida del almacenamiento sobre el prefijo de la zona de trabajo.

## 6. Timeouts

| Nivel | Valor | Dónde |
|---|---|---|
| Ingestión | 5 min por tarea | `tarea(..., timeout_min=5)` |
| Compuerta | 2 min | `timeout_min=2` |
| Validación, limpieza, unión, agregación | 15 min | Valor por defecto de `tarea()` |
| Persistencia | 20 min | `timeout_min=20` |
| `notificar` | 5 min | `timeout_min=5` |
| Corrida completa | 60 min | `dagrun_timeout`, configurable con `DATAMARCA_DAGRUN_TIMEOUT_MIN` |

Las tareas reales tardan entre 0,08 y 46 s (`evidencia/corrida_exitosa.md`): los límites por tarea dejan un margen de más de 10 veces.

**Comportamiento confirmado en el código de Airflow 3:**
- **Timeout de una tarea.** Al vencer el `execution_timeout`, el task runner llama a `on_kill()`. El `BashOperator` envía SIGTERM al grupo de procesos de la tarea. La tarea respeta `retries` (pasa a `up_for_retry` mientras queden intentos) y luego queda en `failed`.
- **Timeout de la corrida.** Al vencer el `dagrun_timeout`, la corrida pasa a `failed` y las tareas sin terminar a `skipped`. Esto aplica también a las corridas manuales.

**Pruebas.** El gancho `DATAMARCA_DEMORAR_EN=<tarea>:<segundos>` demora una tarea, así que no hay que esperar los tiempos reales. Pasos en la sección 7.

| Prueba | Configuración | Resultado esperado |
|---|---|---|
| A. Timeout de una tarea | `DATAMARCA_DEMORAR_EN=compuerta_calidad:300`, `DATAMARCA_RETRIES=1`, `DATAMARCA_RETRY_DELAY_SEG=5` | La compuerta se detiene a los 2 min, se reintenta una vez y queda en `failed`. Lo posterior queda en `upstream_failed` y `notificar` corre. El resumen dice `FALLÓ compuerta_calidad (intento 2): sin cierre (timeout o proceso terminado)`. Unos 5 min en total |
| B. Timeout de la corrida | `DATAMARCA_DAGRUN_TIMEOUT_MIN=4`, `DATAMARCA_DEMORAR_EN=agregar:600` | A los 4 min la corrida queda en `failed`. `agregar`, `persistir_gold`, `notificar` y `corrida_completa` quedan en `skipped`, y `persistir_silver` probablemente en `success`. No se genera resumen: demuestra B1 |

Estado: las pruebas A y B están preparadas y falta ejecutarlas.

## 7. Evidencia: cómo reproducirla

Las capturas van en `reports/orquestacion/evidencia/capturas/` y los logs exportados en `reports/orquestacion/evidencia/logs/<nombre>/`. Complementan los resúmenes que ya están en `reports/orquestacion/evidencia/`, sin repetirlos.

Las capturas del 7 de noviembre (`corrida_exitosa_*` y `corrida_fallida_*`, de Instancias de Tarea y Auditoría de Log) están descritas en `reports/orquestacion/evidencia/README.md`. Los nombres de los pasos 2 y 4 son los de las capturas que faltan.

1. **Levantar Airflow.** Seguir "Ejecutar con Docker" en `airflow/README.md` y entrar a `http://localhost:8080`.
   - Los comandos `docker compose` se ejecutan desde la carpeta `airflow/`.
   - En Linux o Codespaces, antes de levantarlo, poner en `airflow/.env` `AIRFLOW_UID=` con el resultado de `id -u`.
   - El usuario es `airflow`. La contraseña se obtiene con `docker compose logs airflow-apiserver | grep -i password`.
2. **Corrida exitosa.** Con las variables de demostración comentadas, activar el DAG y pulsar *Trigger*. Al activarlo puede arrancar sola una corrida `scheduled__...`, que también sirve.
   - Abrir la corrida, cambiar a la vista *Graph* y expandir los grupos.
   - Capturas: `graph_corrida_exitosa.png` y `log_notificar_exitosa.png`.
   - Exportar los logs con `--nombre corrida_exitosa`.
3. **Pruebas automáticas completas.** En un contenedor de un solo uso, con Java 17 y pyspark 3.5.3, que se borra al terminar:
   ```bash
   docker compose run --rm --no-deps -u 0 -e PYTHONDONTWRITEBYTECODE=1 --entrypoint bash airflow-scheduler \
     -c "pip install -q --root-user-action=ignore pytest && cd /opt/datamarca && python -m pytest tests -v -p no:cacheprovider"
   ```
4. **Corrida con fallo forzado.** En `airflow/.env`, activar `DATAMARCA_FALLAR_EN=limpiar_logs`. Opcionalmente, `DATAMARCA_RETRIES=2` y `DATAMARCA_RETRY_DELAY_SEG=5` acortan la espera; el backoff ya está medido con los valores por defecto.
   - Recrear los contenedores que ejecutan y leen el DAG. El api-server no hace falta, y si se recrea cambia la contraseña:
     ```bash
     docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor
     ```
   - Pulsar *Trigger* y esperar a que termine.
   - Capturas:
     - `graph_corrida_fallida.png`
     - `log_limpiar_logs_fallo.png`: el último intento
     - `log_notificar_fallo.png`
     - Auditoría de Log: ya está (`corrida_fallida_3_auditoria.png` a `corrida_fallida_6_auditoria.png`)
   - Exportar los logs con `--nombre fallo_forzado`.
5. **Retención.** Con `ls ../data/_staging/*` se ve que la corrida exitosa conserva solo `estado/` y `metricas/`, y la fallida todo. Captura: `retencion_staging.png`.
6. **Timeouts (opcional).** Pruebas A y B de la sección 6, cambiando las variables y recreando los contenedores.
   - Capturas: `graph_timeout_tarea.png` y `graph_timeout_corrida.png`.
   - Exportar los logs con `--nombre timeout_tarea` y `--nombre timeout_corrida`.
7. **Restablecer.** Comentar las variables de demostración y recrear los contenedores.
8. **Anotar.** En `reports/orquestacion/evidencia/README.md`, la fecha, el entorno y la configuración de cada corrida.

Exportación de logs, desde `airflow/`:

```bash
docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py --listar
docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py --run-id "<run_id>" --nombre fallo_forzado
```

`exportar_logs.py` deja:
- un archivo de texto por tarea e intento (convierte el JSON de Airflow 3 en líneas legibles, sin descartar campos);
- `estados_airflow.txt`, con la salida de `airflow tasks states-for-dag-run`;
- `resumen.md`, con el resumen de `notificar`.

Los nombres de archivo no llevan `:` ni `+`, porque los `run_id` de Airflow sí, y así el repositorio se puede clonar en Windows.

No se versionan `data/_staging/` ni `reports/orquestacion/resumen_*`, que ya están en `.gitignore`. Tampoco los cambios que las corridas dejan en `data/processed/`.

## 8. Entorno: dónde corre y dónde no

| Entorno | Situación | Qué hacer |
|---|---|---|
| Windows sin Docker | **Las tareas no corren.** Spark necesita `HADOOP_HOME` y `hadoop.dll` para escribir en disco, y `tareas.py`, a diferencia de `main.py`, no tiene respaldo con pyarrow (verificado, `dependencias.md` sección 8) | Usar Docker Desktop o WSL2. Ya está aclarado en `airflow/README.md` |
| Linux y GitHub Codespaces con Docker | Los contenedores corren con `AIRFLOW_UID=50000` y no pueden escribir en `data/` ni en `reports/` del repositorio si los archivos son de otro usuario (deducido) | `AIRFLOW_UID` igual a `id -u`. Aclarado en `airflow/.env.example` y `airflow/README.md` |
| macOS con Apple Silicon y Docker | **Probablemente falla Spark** (deducido, no probado). La imagen se construye para arm64, donde Java queda en `/usr/lib/jvm/java-17-openjdk-arm64`, pero el `Dockerfile` fija `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` | Construir para amd64 con `DOCKER_DEFAULT_PLATFORM=linux/amd64`, más lento, o que el `Dockerfile` calcule `JAVA_HOME` según la arquitectura. Propuesto a Rosario |
| Azure Databricks | La orquestación actual es local: `ingestar_*` verifica el origen con rutas de disco (`Path.is_file()`), que no funcionan con `abfss://`, y la zona de trabajo usa rutas locales | Supuesto para el README (Julián y Joel): el DAG se ejecuta en Docker; llevarlo a Azure requiere adaptar esas verificaciones |

## 9. Cambios de la tarea T1.3 por archivo

| Archivo | Dueño | Cambio |
|---|---|---|
| `docs/orquestacion/manejo_errores_logging.md` | Kinan | Nuevo: este documento |
| `tests/test_orquestacion.py`, `tests/README.md` | Kinan | Nuevo: 34 pruebas del manejo de errores, las trazas y la retención |
| `reports/orquestacion/exportar_logs.py` | Kinan | Nuevo: exporta los logs y estados de Airflow de una corrida como evidencia |
| `reports/orquestacion/evidencia/capturas/` | Kinan | Nuevo: 12 capturas de Airflow de las corridas del 7 de noviembre |
| `reports/orquestacion/evidencia/README.md` | Ivanna (avisar) | Ronda 2: capturas, estado de cada tarea, línea de tiempo del fallo y segunda medición del backoff. La ronda 1 queda igual |
| `src/orquestacion/tareas.py` | Rosario (avisada) | `aplicar_retencion()` con purga por antigüedad; un `try` por paso en `notificar` (B4); gancho `DATAMARCA_DEMORAR_EN`; aviso de modo demostración (B9); mensaje claro para tareas interrumpidas |
| `airflow/dags/datamarca_pipeline.py` | Rosario (avisada) | `dagrun_timeout` configurable con `DATAMARCA_DAGRUN_TIMEOUT_MIN` (por defecto 60 min, sin cambio de comportamiento) |
| `airflow/.env.example` | Rosario (avisada) | `DATAMARCA_LIMPIAR_STAGING=true`, `DATAMARCA_RETENCION_DIAS=7`, variables de la prueba de timeouts y nota de `AIRFLOW_UID` en Linux |
| `airflow/README.md` | Rosario (avisada) | Windows sin Docker no soportado, `AIRFLOW_UID` en Linux, retención, prueba de timeouts y exportación de logs |
| `.gitignore` | Rosario (tarea 3.2, avisada) | Excluye `data/_staging/` y `reports/orquestacion/resumen_*` |
| `docs/orquestacion/dependencias.md` | Ivanna (avisada) | Sección 9: la retención queda decidida |

## 10. Estado de la tarea T1.3 y pendientes

Cada punto de lo asignado en la tarea T1.3, con su evidencia:

| Lo asignado | Estado | Evidencia |
|---|---|---|
| Revisar el manejo explícito de excepciones de `tareas.py` frente a la guía y decir qué más se necesita | Hecho | Secciones 1 a 3: nueve mecanismos, nueve brechas priorizadas (B4 y B9 resueltas) y 34 pruebas en `tests/test_orquestacion.py` |
| Validar las trazas auditables en Airflow, sobre todo en la corrida fallida: qué tareas se cancelaron y cuáles terminaron antes del fallo | Hecho en los estados de Airflow; falta el texto de los logs | Sección 4.3 y `evidencia/capturas/corrida_fallida_*.png` |
| Capturas del *Graph* de la corrida exitosa y de la fallida | En parte: Instancias de Tarea, grilla y Auditoría de Log de las dos corridas; falta la vista *Graph* | `evidencia/capturas/` y `evidencia/README.md` |
| Definir la retención de `data/_staging/` en producción | Hecho: decidida, implementada y probada. Falta verla en una corrida | Sección 5 y `dependencias.md`, sección 9 |
| `DATAMARCA_LIMPIAR_STAGING=true` en `airflow/.env.example` | Hecho | `airflow/.env.example` |
| Retención por días: cambio en `tareas.py` | Hecho: `DATAMARCA_RETENCION_DIAS=7` | Sección 5.2 |
| Opcional: probar los timeouts | Preparado (gancho `DATAMARCA_DEMORAR_EN` y `dagrun_timeout` configurable); sin ejecutar | Sección 6 |
| Aclarar en `airflow/README.md` que Windows sin Docker no funciona | Hecho | `airflow/README.md`, sección "Sin Docker" |
| Usar la evidencia existente sin duplicarla | Hecho: la ronda 2 agrega las capturas de Airflow y no repite los resúmenes de la ronda 1 | `evidencia/README.md` |

Pendientes:

| Pendiente | Responsable | Estado |
|---|---|---|
| Completar la evidencia de las corridas del 7 de noviembre: vista *Graph* de las dos, log del último intento de `limpiar_logs` y log de `notificar` de las dos (o `exportar_logs.py` con sus `run_id`), retención (paso 5) y pruebas con pyspark (paso 3) | Kinan | Pendiente. Si el Codespace sigue activo no hace falta repetir las corridas: los logs y `data/_staging/` siguen ahí |
| Visto bueno de Rosario a los cambios en sus archivos (sección 9) | Kinan → Rosario | Pendiente del aviso |
| `DATAMARCA_LIMPIAR_STAGING`, `DATAMARCA_RETENCION_DIAS` y la política de retención en el README principal | Julián y Joel (T1.4) | Pendiente del aviso |
| Brechas B1, B2, B3, B5, B6 y B8 | Rosario y el equipo | Propuesto, en orden de prioridad |
| `JAVA_HOME` del `Dockerfile` en Apple Silicon | Rosario | Propuesto |
