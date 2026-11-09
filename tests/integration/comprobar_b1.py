"""Ejecutar dentro del scheduler del proyecto Compose datamarca-b1.

El scheduler produce el timeout real; este script solo dispara una corrida y
consulta metadatos. No modifica estados de tareas ni de corridas.
"""
import json
import runpy
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import airflow
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from sqlalchemy import select

sys.path.insert(0, "/opt/datamarca/src")
from orquestacion import alertas, tareas

DAG_ID = "datamarca_pipeline"
RUN = "manual__b1_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")


def mensajes():
    with urllib.request.urlopen("http://teams-receptor:8765/", timeout=5) as respuesta:
        return [m for m in json.load(respuesta) if RUN in m["text"]]


def main():
    assert airflow.__version__ == "3.3.2", airflow.__version__
    dag = runpy.run_path("/opt/datamarca/airflow/dags/datamarca_pipeline.py")["dag"]
    assert dag.on_failure_callback is alertas.alerta_fallo_dag
    assert dag.get_task("notificar").trigger_rule == "all_done"
    assert dag.dagrun_timeout.total_seconds() == 60
    # Activar solo el DAG de este proyecto Compose aislado.
    limite = time.monotonic() + 180
    while True:
        subprocess.run(["airflow", "dags", "unpause", DAG_ID], capture_output=True, check=False)
        intento = subprocess.run(["airflow", "dags", "trigger", DAG_ID, "--run-id", RUN],
                                 capture_output=True, text=True)
        if intento.returncode == 0:
            break
        if time.monotonic() > limite:
            raise AssertionError("No se pudo disparar el DAG: revisar import-errors y servicios")
        time.sleep(5)
    print("Corrida de integración:", RUN, flush=True)
    limite = time.monotonic() + 300
    while time.monotonic() < limite:
        with create_session() as sesion:
            estado = sesion.scalar(select(DagRun.state).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN))
            tareas_db = dict(sesion.execute(select(TaskInstance.task_id, TaskInstance.state).where(
                TaskInstance.dag_id == DAG_ID, TaskInstance.run_id == RUN)).all())
        recibo = alertas.ruta_recibo(RUN)
        if estado == "failed" and recibo.exists():
            datos = json.loads(recibo.read_text())
            if datos["resultado"] == "enviado":
                break
        time.sleep(5)
    else:
        raise AssertionError("No se observó timeout y envío confirmado; revisar logs del dag processor")
    assert tareas_db["notificar"] == "skipped", tareas_db
    assert datos["origen"] == "callback_dag", datos
    assert datos["estado_envio"] == "confirmed", datos
    assert datos["notificacion_id"] == alertas.identificador(RUN)
    assert "dagrun_timeout" in mensajes()[0]["text"]
    assert len(mensajes()) == 1
    resumen = Path("/opt/datamarca/reports/orquestacion") / f"resumen_{tareas.limpiar_run_id(RUN)}.json"
    assert not resumen.exists(), "La prueba exige que notificar no genere resumen"
    antes = (estado, tareas_db)
    assert alertas.alerta_fallo_dag({"run_id": RUN, "reason": "timed_out"})["resultado"] == "duplicado_omitido"
    assert len(mensajes()) == 1
    with create_session() as sesion:
        despues = (sesion.scalar(select(DagRun.state).where(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN)),
                   dict(sesion.execute(select(TaskInstance.task_id, TaskInstance.state).where(
                       TaskInstance.dag_id == DAG_ID, TaskInstance.run_id == RUN)).all()))
    assert despues == antes, "El callback no debe modificar estados reales de Airflow"
    print("B1 OK: timeout real, DAG failed, notificar skipped, una alerta local, sin resumen ni duplicados")


if __name__ == "__main__":
    main()
