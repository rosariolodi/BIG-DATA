# Orquestación con Apache Airflow (local) — Unidad 4, tarea T1.2

DAG `datamarca_pipeline`: ingestión → validación → transformación → persistencia → notificación
(12 tareas + `corrida_completa`; grafo en `docs/orquestacion/dependencias.md`).

## Archivos
- `dags/datamarca_pipeline.py`: el DAG (operadores, schedule, retries, timeout, backoff).
- `../src/orquestacion/alertas.py`: envío Teams, recibo por corrida y callback de fallo del DAG.
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
  corrida como fallida y deja en `skipped` las tareas sin terminar, incluida `notificar`. El callback del DAG cubre B1 enviando una alerta independiente del resumen.

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
| `DATAMARCA_LIMPIAR_STAGING` | `false` | Si se establece en `true`, elimina los intermedios de una corrida exitosa y conserva `estado/` y `metricas/`. El registro de alertas es independiente del staging. Las corridas fallidas se conservan. El archivo `.env.example` propone `true`. |
| `DATAMARCA_RETENCION_DIAS` | `0` o sin valor | Si es mayor que cero, elimina carpetas de corridas cuya última actividad supere ese número de días. El archivo `.env.example` propone `7`. |
| `DATAMARCA_WEBHOOK_URL` | Sin valor | URL real opcional de Teams Workflows; recibe el resumen o la alerta de respaldo del callback. |
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


## Teams Workflows y respaldo B1

1. En Teams, abra Workflows en el chat/canal destino y cree un flujo con el disparador
   **When a Teams webhook request is received**. Configure la acción de publicar un mensaje
   para usar el campo `text` del cuerpo (`triggerBody()?['text']`). Este emisor envía
   exclusivamente `{"text": "..."}`; si una plantilla espera `attachments`, adapte el flujo
   para leer `text`. El disparador debe permitir la llamada por URL sin autenticación adicional:
   este código no administra tokens de Microsoft. Guía oficial:
   [webhooks de Teams](https://learn.microsoft.com/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook).
2. Copie la URL real generada por el flujo en `DATAMARCA_WEBHOOK_URL` dentro de `airflow/.env`
   (o expórtela en el entorno sin Docker). No use URLs de ejemplo ni publique `.env`.
3. Recree scheduler y dag processor para cargarla:
   `docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor`.
   Ambos necesitan el mismo webhook y el mismo staging compartido.
4. Verifique una corrida controlada: el log debe indicar `TEAMS resultado=enviado` y
   el mensaje debe aparecer en Teams. HTTP 2xx confirma aceptación del webhook, no que
   las acciones posteriores del Workflow hayan terminado: revise también su historial.
   Sin URL se registra `sin_webhook`; eso no demuestra conexión con Teams.

`notificar` mantiene sus responsabilidades: clasificar las tareas, registrar el resumen,
guardarlo en Markdown/JSON y aplicar retención aunque falle el envío. El DAG añade
`on_failure_callback=alerta_fallo_dag`, independiente de los callbacks de tarea en
`default_args`. Airflow lo ejecuta al fallar la corrida, incluido `dagrun_timeout` aunque
`notificar` quede `skipped`. No cambia estados de tareas ni del DAG. Usa `dag_run.run_id`
y el motivo del contexto, sin depender de una TaskInstance ni de que exista resumen.

Los dos emisores usan el mismo mecanismo y tipo `resultado_corrida`. El identificador es
SHA-256 de `[dag_id, run_id, tipo]`; no depende del origen del emisor y se incluye como
texto en el mensaje, manteniendo el JSON `{"text": "..."}`. Teams no está obligado a
tratar ese identificador como clave idempotente.

### Almacenamiento persistente independiente

`DATAMARCA_ALERT_STATE_DIR` configura el directorio del registro. Sin Docker, el valor
predeterminado es `data/alert_state/` en la raíz del repositorio (ignorado por Git).
No puede estar dentro de `DATAMARCA_STAGING_ROOT`; la verificación rechaza esa configuración.
En Docker se monta el volumen nombrado **datamarca-alert-state**, independiente de staging,
PostgreSQL y logs, en `/opt/airflow/alert_state` por defecto. Scheduler y DAG Processor
heredan el mismo montaje y la misma variable desde `x-airflow-common`. Si cambia la ruta,
Compose utiliza `DATAMARCA_ALERT_STATE_DIR` tanto en el entorno como en el destino del volumen.

`airflow-init` ajusta el propietario del directorio a `AIRFLOW_UID` y los permisos a 0700.
Antes de arrancar scheduler y DAG Processor, la CLI comprueba creación, escritura y `fsync`
con su usuario efectivo. Si falla, el servicio no arranca: revise propietario, montaje y
permisos. Sin Docker, ejecute `verificar` con el mismo usuario que ejecutará ambos servicios.
Los recibos y los bloqueos se crean con permisos 0600. No ejecute `docker compose down -v`
si necesita conservar la deduplicación: elimina los volúmenes del proyecto.

Cada notificación ocupa `teams_<id>.json` y `teams_<id>.lock`. El JSON contiene únicamente
identificadores, estados, resultados HTTP, fechas y auditoría; no guarda URL del webhook,
credenciales, cuerpo de respuesta ni el texto original del mensaje. La recuperación genera
un aviso genérico con DAG, corrida y tipo para no persistir el resumen, que puede contener
información privada. No introduzca credenciales en nombres de corrida, usuario o motivos.
Las URLs de los motivos y el webhook configurado se redactan antes de guardar la auditoría.
El historial es auditable pero no inviolable frente a alguien con escritura sobre el volumen.

Los recibos no se purgan con `DATAMARCA_RETENCION_DIAS` ni con la limpieza del pipeline.
Las confirmaciones antiguas `teams.json` y `teams_<id>.json` del staging se migran al
registro nuevo cuando se consulta de nuevo esa corrida. Los antiguos `pendiente` y
`ambiguo` migran a `ambiguous`, porque no permiten saber si el POST comenzó. Detenga
los emisores de la versión anterior y recree ambos servicios antes de reanudar corridas:
las versiones antiguas no comparten los bloqueos nuevos.

### Estados, bloqueos y recuperación

| Estado | Significado y transición |
|---|---|
| `reserved` | Reserva durable antes del inicio del envío. Si queda abandonada por antigüedad y su bloqueo está libre, puede pasar a `expired` y recuperarse con la CLI. |
| `sending` | Se registra antes de llamar al emisor HTTP. Si desaparece el emisor, se transforma en `ambiguous`; nunca se reenvía automáticamente. |
| `confirmed` | HTTP 2xx o confirmación manual tras verificar Teams. No admite autorización de reenvío. |
| `failed` | Sin webhook o con respuesta HTTP de error. Una invocación posterior puede volver a intentarlo; también admite autorización manual. |
| `ambiguous` | Timeout, error de transporte o proceso interrumpido durante `sending`. Exige reconciliación manual. |
| `expired` | Reserva antigua sin POST o reenvío expresamente autorizado. Solo la recuperación operativa puede convertirla de nuevo en `reserved`. |

Un bloqueo del sistema operativo por identificador (`flock` en Linux/macOS y `msvcrt.locking` en Windows) se mantiene durante todas las transiciones y el POST.
El plazo máximo de espera es **2 segundos**, configurable con
`DATAMARCA_ALERT_LOCK_TIMEOUT_SEG`. Si vence, se registra `bloqueo_ocupado` y no se envía
fuera de la deduplicación. Si el registro persistente está inaccesible, corrupto o no se
puede guardar `reserved`/`sending`, se registra `error_registro` y tampoco se hace el POST.
Esto sustituye al respaldo sin recibo de la versión anterior. Un fallo en staging, por sí
solo, no impide alertar; un fallo del registro persistente sí requiere intervención.
Los resultados HTTP, del registro y de deduplicación se registran por separado.

La antigüedad predeterminada es **300 segundos**, configurable con
`DATAMARCA_ALERT_RESERVATION_TTL_SEG`. Se mide desde la entrada al estado, no desde el
inicio del DAG. Consultar antigüedad no basta para reclamar una reserva: las operaciones
vuelven a comprobar su estado bajo bloqueo. Una reserva activa no puede expirar ni ser
reconciliada por otro proceso. No hay un servicio de barrido periódico: el operador debe
ejecutar los comandos de inspección/recuperación. No se cambia ningún estado de Airflow.

### Comandos de recuperación y reconciliación

Desde la raíz del repositorio sin Docker:

```bash
export PYTHONPATH="$PWD/src"
.venv/bin/python -m orquestacion.recuperar_alertas verificar
.venv/bin/python -m orquestacion.recuperar_alertas listar --pendientes
.venv/bin/python -m orquestacion.recuperar_alertas listar --pendientes --abandonadas --antiguedad-seg 300
.venv/bin/python -m orquestacion.recuperar_alertas listar --estado ambiguous
.venv/bin/python -m orquestacion.recuperar_alertas consultar <id>
```

En Docker, desde `airflow/`, puede usar este ayudante para los mismos subcomandos:

```bash
alertas() { docker compose exec -T -e PYTHONPATH=/opt/datamarca/src airflow-scheduler python -m orquestacion.recuperar_alertas "$@"; }
alertas verificar
alertas listar --pendientes --abandonadas
# Expirar sin enviar, registrando quién tomó la decisión:
alertas expirar --id <id> --usuario <operador> --motivo "Reserva antigua sin emisor activo"
# Recuperar una reserva antigua o ya expirada (envía un aviso genérico):
alertas recuperar --id <id> --usuario <operador> --motivo "Recuperación operativa revisada"
```

`expirar` y `recuperar`, sin `--id`, inspeccionan los registros `reserved`, `sending` y
`expired`. Solo recuperan reservas seguras; un `sending` abandonado se vuelve `ambiguous`
y no se reenvía. `--antiguedad-seg` permite elegir explícitamente el umbral; use valores
bajos solo si ha comprobado que la reserva quedó abandonada. `listar --abandonadas`
excluye candidatos cuyo bloqueo siga ocupado.

Para reconciliar un ambiguo, busque su identificador en Teams y revise el historial del
Workflow antes de intervenir:

```bash
# El mensaje sí llegó: confirmar sin realizar otro POST.
alertas confirmar <id> --verificado-en-teams --usuario <operador> --motivo "Mensaje localizado en Teams e historial revisado"
# Se comprobó que no llegó: autorizar, todavía sin enviar.
alertas autorizar-reenvio <id> --no-entregado --usuario <operador> --motivo "Historial revisado; mensaje no entregado"
# Ejecutar después el reenvío autorizado.
alertas recuperar --id <id> --usuario <operador> --motivo "Ejecución de autorización registrada"
```

Cada intervención que cambia estado guarda usuario declarado, fecha UTC, motivo, acción
y estados anterior/nuevo en el mismo JSON atómico que el estado. El identificador permanece
igual y el historial se conserva entre intentos. El usuario identifica al operador; no es
un mecanismo de autenticación. No edite recibos a mano ni borre bloqueos para forzar envíos.
La CLI devuelve código 2 ante errores operativos o resultados de recuperación fallidos o
ambiguos; devuelve JSON y nunca imprime el webhook. `--help` describe los subcomandos.

No se promete entrega exactamente una vez. Hay una ventana entre persistir `sending`
y comenzar el POST: una muerte ahí requiere reconciliación aunque el POST no haya salido.
Un timeout o una respuesta perdida no prueba ausencia de entrega. HTTP 2xx acredita
aceptación del endpoint, no publicación final del Workflow. El timeout de red es de
10 segundos por operación bloqueante y no es un plazo total de entrega. Si falla guardar
el resultado después del POST, queda `sending`: no se repite el POST y la siguiente
inspección operativa puede clasificarlo como `ambiguous`. Un reenvío manual sin verificar
correctamente la entrega previa puede duplicar el mensaje. Borrar el volumen elimina
la protección histórica.

El callback no importa `tareas.py` ni evalúa `UMBRAL_RECHAZO_PCT`. La ausencia de
`DATAMARCA_WEBHOOK_URL` se registra como ERROR `sin_webhook`, nunca como entrega exitosa.
En el Compose habitual, `x-airflow-common.env_file` se hereda en scheduler y DAG Processor.
No basta con configurar la variable solo en el scheduler. La herencia se comprueba
inspeccionando `docker-compose.yaml`, sin abrir `.env` ni imprimir configuración resuelta.

Los logs de `notificar` siguen en su tarea en la UI. Los del callback van al **dag processor**,
no al log de una tarea. En Airflow 3.3.2 se consultan en
`$AIRFLOW_HOME/logs/dag_processor/latest/dags-folder/<ruta-del-DAG>/datamarca_pipeline.py.log`;
el detalle de ruta puede variar según el bundle. En Docker:

```bash
docker compose logs --since 15m airflow-dag-processor
docker compose exec airflow-dag-processor bash -lc 'find /opt/airflow/logs/dag_processor -type f -name "*datamarca_pipeline*"'
```

Consulte también los logs del scheduler para confirmar el timeout. Referencia:
[callbacks de Airflow 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/logging-monitoring/callbacks.html).

### Prueba B1 automática en Docker (receptor local, sin Teams)

Requiere Docker Compose 2.24.4+ y acceso a descargar las imágenes. Use un proyecto
separado para aislar PostgreSQL y logs; el repositorio se monta como en el entorno habitual.
No lee `.env`: el comando usa `--env-file /dev/null` y el override elimina `env_file`
de los servicios Airflow. Define un receptor HTTP de prueba exclusivamente dentro de
la red Docker. No verifica entrega a Teams. En Linux exporte antes `AIRFLOW_UID` con
el resultado de `id -u` para permitir escribir en el montaje.

Desde `airflow/` (Bash):

```bash
dc_b1() { docker compose --env-file /dev/null -p datamarca-b1 -f docker-compose.yaml -f docker-compose.b1.yaml "$@"; }
dc_b1 build
dc_b1 up airflow-init
dc_b1 up -d postgres teams-receptor airflow-apiserver airflow-scheduler airflow-dag-processor
dc_b1 exec -T airflow-scheduler python /opt/datamarca/tests/integration/comprobar_b1.py
# UI del entorno de prueba: http://localhost:8081
# Para investigar fallos:
dc_b1 logs --since 15m airflow-scheduler airflow-dag-processor
# Al terminar (conserva volúmenes y evidencia):
dc_b1 down
```

El script comprueba la versión exacta de Airflow y la importación real del DAG,
activa el DAG del entorno aislado y dispara una corrida con timeout de 1 minuto y una
demora de 180 segundos en `ingestar_logs` (antes de Spark). Espera como máximo 3 minutos
para cargar el DAG y 5 minutos para observar el resultado. Comprueba `failed` en el DAG,
`skipped` en `notificar`, ausencia de resumen y un único POST recibido con origen
`callback_dag` y motivo `dagrun_timeout`. Reinvoca el callback para comprobar que no
se repite el POST y compara los estados antes/después. Solo consulta estados de Airflow; no los fuerza. Al activar el DAG,
Airflow también puede crear una corrida del schedule; se identifica y cuenta únicamente
el `run_id` manual impreso por el script. La evidencia local queda en
`/opt/airflow/alert_state/teams_<id>.json` (volumen independiente del proyecto B1) y en los logs del proyecto de prueba.

Para una prueba real contra Teams, use el entorno habitual con la URL real configurada,
`DATAMARCA_DAGRUN_TIMEOUT_MIN=1`, `DATAMARCA_DEMORAR_EN=ingestar_logs:180` y
`DATAMARCA_RETRIES=0`; recree scheduler/dag processor, dispare una corrida en la UI y
verifique los mismos estados, el recibo y el mensaje en el canal. Después retire las
variables de demostración y recree ambos servicios. `airflow dags test` y los cambios
manuales de estado en la UI no sustituyen la prueba con scheduler para B1.

Pruebas unitarias desde la raíz: `python -m pytest tests/test_alertas.py tests/test_orquestacion.py`.
No hacen peticiones reales al webhook.


### Teams real en el entorno local del puerto 8081

Para conservar el destino real de Teams en el proyecto local `airflow`, incluya
`docker-compose.teams.yaml` **después** del override B1. Desde esta carpeta:

```bash
docker compose -p airflow --env-file .env -f docker-compose.yaml -f docker-compose.b1.yaml -f docker-compose.teams.yaml up -d --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver
```

El webhook se toma de `DATAMARCA_WEBHOOK_URL` en `.env`; no se copia en los archivos
Compose ni en las evidencias. El puerto sigue siendo `8081`. Sin el último override,
B1 envía al receptor local de pruebas aunque `.env` contenga la URL real.

Después de una prueba de fallo, retire cualquier override temporal que configure
`DATAMARCA_FALLAR_EN` o `DATAMARCA_DEMORAR_EN` y vuelva a crear los servicios afectados
con los tres archivos anteriores. Use un identificador nuevo para cada corrida de
verificación: una alerta anterior ya confirmada se omite por deduplicación.
Compruebe tanto el resultado de Airflow como la ejecución del Workflow y el mensaje
en el canal; HTTP 202 por sí solo no acredita publicación en Teams.

### Archivos locales generados

`job_listo.json` y `tmp_tiempos/` quedan fuera del seguimiento de Git y están ignorados.
Los comandos del README raíz siguen generándolos: sustitución del JSON de Databricks
y descarga con `az storage blob download-batch --destination .\tmp_tiempos`. El cambio
solo afecta al índice de Git; conserva las copias locales. `test_ejecuciones_airflow/`
conserva sus evidencias versionadas.
