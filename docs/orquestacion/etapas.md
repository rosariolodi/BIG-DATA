# Etapas del pipeline para la orquestación (Unidad 4, paso 1)

Proyecto Spark Pipeline - DATAMARCA S.A.

Este documento define el pipeline batch de la Unidad 3 como cinco etapas orquestadas: **ingestión → validación → transformación → persistencia → notificación**. Para cada etapa se indican su propósito, sus entradas y salidas, el código que la implementa hoy, qué pasa si falla y si se puede reejecutar. Las dependencias entre las tareas de cada etapa están en `docs/orquestacion/dependencias.md`.

El documento es neutro respecto a la herramienta (Airflow o Step Functions). La elección y el DAG son pasos posteriores de la actividad.

## 1. Punto de partida: qué existe de la Unidad 3

`src/main.py` ejecuta todo en un solo proceso y una sola sesión de Spark. Mide cada subetapa con `registro.medir("etapa", "subetapa")` (`src/utils/tiempos.py`). Frente a las cinco etapas pedidas:

| Etapa pedida | Estado en el código actual |
|---|---|
| Ingestión | Existe. Se mide como `ingestion/transacciones_csv` y `ingestion/logs_json` |
| Validación | **Existe, pero unida a la ingestión.** `ingestar_transacciones` e `ingestar_logs_ecommerce` (`src/ingestion/reader.py`) leen y validan en una sola llamada, y su tiempo se mide junto |
| Transformación | Existe. `transformacion/limpieza_transacciones`, `limpieza_logs`, `join`, `agregaciones` |
| Persistencia | Existe. `persistencia/silver` y `persistencia/gold` |
| Notificación | **No existe.** Hoy solo hay mensajes por consola y el log de tiempos |

Hay dos hechos del código que condicionan el diseño:

- **Los datos pasan en memoria entre pasos.** `main.py` encadena DataFrames de Spark (por ejemplo `ingesta_tx.validos` pasa a la limpieza). Si cada tarea de la orquestación es un proceso aparte, cada tarea debe leer lo que dejó la anterior en almacenamiento. Esto se detalla en `dependencias.md`.
- **La validación ya es separable.** `reader.py` tiene `leer_transacciones_csv_crudo` / `leer_logs_ecommerce_json_crudo` (lectura) y `src/ingestion/validacion.py` tiene `validar_transacciones` / `validar_logs` (reglas). Solo la función `ingestar_*` las junta. Separar las etapas no exige reescribir las reglas.

## 2. Resumen de las cinco etapas

| # | Etapa | Entrada | Salida | Código de la Unidad 3 |
|---|---|---|---|---|
| 1 | Ingestión | CSV de transacciones y JSON de logs (carpeta `bronze` en Azure, `data/raw/` en local) | DataFrames crudos con esquema estricto y la columna `_corrupt_record` | `leer_transacciones_csv_crudo`, `leer_logs_ecommerce_json_crudo` |
| 2 | Validación | Datos crudos de la etapa 1 | Registros válidos (con `dq_warnings`), cuarentena y métricas de calidad | `validar_transacciones`, `validar_logs`, `guardar_resultado_validacion` |
| 3 | Transformación | Registros válidos de la etapa 2 | Transacciones y logs limpios, tabla unida y agregaciones de negocio | `ejecutar_limpieza_*`, `realizar_join_transacciones_logs`, `generar_agregaciones_negocio`, `ejecutar_agregaciones` |
| 4 | Persistencia | Limpios y agregaciones de la etapa 3 | Tablas silver y gold particionadas (Delta en Azure, Parquet en local) | `persistir_silver`, `persistir_gold` |
| 5 | Notificación | Estado de las etapas anteriores y métricas | Aviso de éxito o de fallo | **Nueva** (por construir) |

## 3. Detalle por etapa

### Etapa 1. Ingestión

- **Propósito:** leer las dos fuentes sin descartar nada, con esquema explícito (sin inferencia).
- **Fuentes:** transacciones (CSV) y logs de e-commerce (JSON en arreglo, `multiLine`). Son independientes entre sí.
- **Salida:** el DataFrame crudo de cada fuente. Los registros mal formados quedan marcados en `_corrupt_record` (modo PERMISSIVE) en vez de tumbar la lectura. En logs, `timestamp` y `session_duration_sec` llegan como texto y se convierten en la validación.
- **Si falla:** una ruta inexistente o ilegible hace fallar la tarea. No hay datos parciales que limpiar, porque esta etapa no escribe nada. Es seguro reintentar.
- **Reejecución:** idempotente. Solo lee.

### Etapa 2. Validación

- **Propósito:** aplicar el catálogo de 23 reglas (`docs/reglas_calidad.md`) y separar lo válido de lo rechazado.
- **Salida:**
  - `validos`: registros que no incumplen ninguna regla crítica, con `dq_warnings` para las advertencias.
  - `cuarentena`: registros rechazados con las reglas que incumplieron.
  - `metricas`: leídos, válidos, rechazados, porcentaje de rechazo y fallos por regla.
- **Dónde se escribe hoy:** cuarentena y métricas van a `DIR_CALIDAD` (`data/quarantine/` en local; `silver/quarantine` en Azure), desde `guardar_resultado_validacion`.
- **Si falla:** hay dos casos distintos.
  - *Fallo técnico* (por ejemplo, no se puede leer el origen): la tarea falla y se reintenta.
  - *Fallo de calidad* (muchos registros rechazados): hoy el pipeline sigue siempre, aunque rechace una fracción grande. Para la orquestación hay que decidir un umbral que detenga la corrida. Es una propuesta de `dependencias.md`, no algo implementado.
- **Reejecución:** la cuarentena se guarda con `overwrite`, así que repetir deja el mismo resultado.
- **Cambio de código necesario:** separar `ingestar_*` en lectura y validación para poder medirlas y ejecutarlas como tareas distintas. Hoy el tiempo de ingestión (11,4 s y 14,0 s en la corrida de Azure del 31/10 03:45) incluye también las reglas.

### Etapa 3. Transformación

- **Propósito:** dejar los datos listos para análisis.
- **Pasos:**
  1. Limpieza de transacciones.
  2. Limpieza de logs. Es independiente de la anterior.
  3. Unión de transacciones con el resumen de logs por usuario. Necesita los dos pasos anteriores.
  4. Agregaciones de negocio (SparkSQL y DataFrames). Necesitan la unión.
- **Salida:** `df_clean_tx`, `df_clean_logs` (para silver) y las seis tablas de agregaciones (para gold).
- **Si falla:** la tarea falla sin dejar nada escrito, porque esta etapa solo calcula. Es seguro reintentar.
- **Reejecución:** idempotente.
- **Requisito de sesión:** zona horaria `America/Bogota`. Con otra zona las fechas de partición salen mal (`docs/particionamiento.md`).

### Etapa 4. Persistencia

- **Propósito:** guardar los resultados finales particionados (`docs/persistencia.md`).
- **Salida:**
  - Silver: `silver/transacciones` y `silver/logs_ecommerce`, particionadas por `fecha`.
  - Gold: seis tablas de agregaciones, particionadas por `mes` o `fecha_proceso`.
- **Si falla:** puede quedar escritura parcial (algunas tablas o particiones sí, otras no).
- **Reejecución:** idempotente por la sobrescritura dinámica de particiones. Repetir la tarea reemplaza solo las particiones de esa corrida y no duplica datos. Esto es lo que permite reintentar sin limpiar a mano.
- **Silver y gold:** se escriben en destinos distintos y se pueden tratar como dos tareas.

### Etapa 5. Notificación (nueva)

- **Propósito:** avisar el resultado de la corrida para que un fallo no pase desapercibido.
- **Qué se notifica:**
  - *Éxito:* resumen con filas leídas, válidas y rechazadas, tiempo por etapa y tiempo total.
  - *Fallo:* tarea que falló, error, y tareas que quedaron sin ejecutarse por esa causa.
- **Fuente de la información:** las métricas de la etapa 2 y el log de tiempos (`RegistroTiempos`), que ya se guarda aunque falle una etapa y marca el estado `error`.
- **Canal:** se define en el paso 3 de la actividad (correo, Slack o solo log). Este documento no lo fija.
- **Debe ejecutarse siempre**, también cuando algo anterior falló. De lo contrario, el caso que más importa quedaría sin aviso. La regla de disparo está en `dependencias.md`.
- **Si falla la propia notificación:** no debe cambiar el estado del procesamiento. Los datos ya se guardaron. Se registra el error y se sigue.

## 4. Correspondencia con las mediciones de la Unidad 3

| Subetapa medida (`registro.medir`) | Etapa orquestada |
|---|---|
| `ingestion/transacciones_csv`, `ingestion/logs_json` | 1 Ingestión **y** 2 Validación (hoy juntas) |
| `transformacion/limpieza_transacciones`, `limpieza_logs`, `join`, `agregaciones` | 3 Transformación |
| `persistencia/silver`, `persistencia/gold` | 4 Persistencia |
| *(no existe)* | 5 Notificación |

Tiempos de la corrida de Azure del 31/10/2026 03:45 (`reports/tiempos/evidencia/azure/tiempos_20261031_034517.csv`), medidos con todo en una sola sesión:

| Subetapa | Duración (s) |
|---|---|
| transacciones_csv | 11,42 |
| logs_json | 14,04 |
| limpieza_transacciones | 6,21 |
| limpieza_logs | 6,56 |
| join | 11,90 |
| agregaciones | 25,53 |
| silver | 35,28 |
| gold | 29,81 |

Estas cifras son de una sola sesión de Spark. Al dividir en tareas con procesos separados, cada una tendrá su propio arranque y sus propias lecturas, así que la suma no será igual. Se medirá en la implementación.

## 5. Cambios de código que implican estas etapas

No se aplican en este commit. Quedan identificados para los pasos siguientes:

1. Separar lectura y validación en `reader.py` para medirlas y ejecutarlas por separado.
2. Hacer que cada tarea lea sus entradas de almacenamiento y escriba sus salidas, en lugar de recibir DataFrames en memoria.
3. Agregar la notificación y el umbral de calidad que detiene la corrida.
4. Dividir `main.py` en puntos de entrada por tarea, manteniendo el flujo completo como opción.
