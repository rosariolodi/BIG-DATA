"""
DAG de orquestación del pipeline batch de DATAMARCA S.A. (Unidad 4, tarea T1.2).

Convierte el pipeline de la Unidad 3 (src/main.py, un solo proceso manual) en un flujo
automatizado de 12 tareas organizado en las cinco etapas del paso 1 de la actividad:

    ingestión -> validación -> transformación -> persistencia -> notificación

El grafo sigue docs/orquestacion/dependencias.md:

    ingestar_transacciones -> validar_transacciones --+
                                                      +-> compuerta_calidad -+-> limpiar_transacciones --+-> unir -> agregar -> persistir_gold --+
    ingestar_logs          -> validar_logs -----------+                      +-> limpiar_logs ----------+                                      |
                                                                                    +--> persistir_silver (desde ambas limpiezas) -------------+
                                                                                                                                              v
                                                                  (todas las tareas, aunque fallen) ----------------------------------> notificar

Cada tarea es un BashOperator que ejecuta `src/orquestacion/tareas.py <tarea>` en su propio
proceso. Reintentos, backoff exponencial y timeouts están en default_args y se ajustan con
variables de entorno (ver airflow/README.md).
"""
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

# Compatible con Airflow 3.x (SDK) y con Airflow 2.x
try:
    from airflow.sdk import DAG, TaskGroup
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow import DAG
    from airflow.utils.task_group import TaskGroup
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

# airflow/dags/ -> airflow/ -> [RAÍZ DEL PROYECTO]
RAIZ = Path(os.getenv("DATAMARCA_HOME", Path(__file__).resolve().parents[2]))
if str(RAIZ / "src") not in sys.path:
    sys.path.insert(0, str(RAIZ / "src"))
from orquestacion.alertas import alerta_fallo_dag

SCRIPT = RAIZ / "src" / "orquestacion" / "tareas.py"
PYTHON = os.getenv("DATAMARCA_PYTHON", "python")

# --- Política de reintentos --------------------------------------------------------------
# Los fallos esperables son transitorios (límite de lectura del almacenamiento, red, arranque
# de Spark). Con backoff exponencial las esperas son ~1, 2 y 4 minutos (tope 10), con lo que
# no se satura el servicio y se da tiempo a que se recupere. Todas las tareas son idempotentes
# (docs/orquestacion/dependencias.md, sección 4), así que reintentar no duplica datos.
REINTENTOS = int(os.getenv("DATAMARCA_RETRIES", "3"))
ESPERA_REINTENTO_S = int(os.getenv("DATAMARCA_RETRY_DELAY_SEG", "60"))
ESPERA_MAXIMA_S = int(os.getenv("DATAMARCA_MAX_RETRY_DELAY_SEG", "600"))
# Límite de la corrida completa. Se puede bajar solo para probar el timeout de la corrida (T1.3);
# al vencer, Airflow marca la corrida fallida y deja en skipped las tareas sin terminar.
TIMEOUT_CORRIDA_MIN = int(os.getenv("DATAMARCA_DAGRUN_TIMEOUT_MIN", "60"))


def _alerta_reintento(context):
    ti = context["task_instance"]
    log.warning("REINTENTO: tarea=%s intento=%s error=%s", ti.task_id, ti.try_number, context.get("exception"))


def _alerta_fallo(context):
    """Se ejecuta cuando una tarea agota sus reintentos. El aviso formal lo envía `notificar`."""
    ti = context["task_instance"]
    log.error("FALLO DEFINITIVO: dag=%s tarea=%s intento=%s error=%s -> las tareas dependientes "
              "quedarán en upstream_failed", ti.dag_id, ti.task_id, ti.try_number, context.get("exception"))


default_args = {
    "owner": "datamarca",
    "retries": REINTENTOS,
    "retry_delay": timedelta(seconds=ESPERA_REINTENTO_S),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=ESPERA_MAXIMA_S),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": _alerta_reintento,
    "on_failure_callback": _alerta_fallo,
}


def tarea(nombre: str, timeout_min: int = 15, **kwargs) -> BashOperator:
    """Tarea que ejecuta una función de src/orquestacion/tareas.py en su propio proceso."""
    return BashOperator(
        task_id=nombre,
        bash_command=(f'{PYTHON} "{SCRIPT}" {nombre} --run-id "{{{{ run_id }}}}" '
                      f'--intento {{{{ ti.try_number }}}}'),
        execution_timeout=timedelta(minutes=timeout_min),
        **kwargs,
    )


with DAG(
    dag_id="datamarca_pipeline",
    description="Pipeline batch DATAMARCA: ingestión, validación, transformación, persistencia y notificación",
    doc_md=__doc__,
    # Diario a las 02:00 (Bogotá): el pipeline particiona por día (docs/particionamiento.md) y es
    # batch (docs/Justificacion_batch_vs_streaming.md); a esa hora el día anterior está completo.
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,          # una corrida a la vez: silver/gold se sobrescriben por partición
    max_active_tasks=4,
    dagrun_timeout=timedelta(minutes=TIMEOUT_CORRIDA_MIN),
    on_failure_callback=alerta_fallo_dag,
    default_args=default_args,
    tags=["datamarca", "unidad4", "batch"],
) as dag:

    with TaskGroup("ingestion", tooltip="Etapa 1: verificar orígenes") as g_ingestion:
        ingestar_transacciones = tarea("ingestar_transacciones", timeout_min=5)
        ingestar_logs = tarea("ingestar_logs", timeout_min=5)

    with TaskGroup("validacion", tooltip="Etapa 2: reglas de calidad y compuerta") as g_validacion:
        validar_transacciones = tarea("validar_transacciones", timeout_min=15)
        validar_logs = tarea("validar_logs", timeout_min=15)
        compuerta_calidad = tarea("compuerta_calidad", timeout_min=2)

    with TaskGroup("transformacion", tooltip="Etapa 3: limpieza, unión y agregaciones") as g_transf:
        limpiar_transacciones = tarea("limpiar_transacciones")
        limpiar_logs = tarea("limpiar_logs")
        unir = tarea("unir")
        agregar = tarea("agregar")

    with TaskGroup("persistencia", tooltip="Etapa 4: silver y gold particionados") as g_persistencia:
        persistir_silver = tarea("persistir_silver", timeout_min=20)
        persistir_gold = tarea("persistir_gold", timeout_min=20)

    # Etapa 5. Corre SIEMPRE (all_done), también si algo falló, para que un fallo no pase
    # desapercibido. Sin reintentos: reenviar el aviso no justifica esperar, y su fallo no
    # debe alterar el resultado del procesamiento (tareas.py captura sus propias excepciones).
    notificar = tarea("notificar", timeout_min=5, trigger_rule="all_done", retries=0)

    # Resultado de la corrida. Como `notificar` termina bien aunque falle una etapa, Airflow
    # marcaría la corrida como exitosa; esta tarea se queda en upstream_failed si falla
    # cualquier etapa de datos y así la corrida completa se reporta como fallida.
    corrida_completa = EmptyOperator(task_id="corrida_completa")

    # --- Dependencias (docs/orquestacion/dependencias.md, sección 2) ---
    ingestar_transacciones >> validar_transacciones
    ingestar_logs >> validar_logs
    [validar_transacciones, validar_logs] >> compuerta_calidad
    compuerta_calidad >> [limpiar_transacciones, limpiar_logs]
    [limpiar_transacciones, limpiar_logs] >> unir >> agregar >> persistir_gold
    [limpiar_transacciones, limpiar_logs] >> persistir_silver      # silver no espera la unión

    [ingestar_transacciones, ingestar_logs, validar_transacciones, validar_logs, compuerta_calidad,
     limpiar_transacciones, limpiar_logs, unir, agregar, persistir_silver, persistir_gold] >> notificar
    [persistir_silver, persistir_gold] >> corrida_completa
