"""Pruebas sin red: URLs reservadas son fixtures, nunca configuración real."""
import ast
import json
import logging
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from orquestacion import alertas, tareas
from orquestacion import estado_alertas as estado

RUN = "manual__b1:prueba+00:00"


@pytest.fixture(autouse=True)
def entorno(tmp_path, monkeypatch):
    monkeypatch.delenv("DATAMARCA_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(tareas, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setenv("DATAMARCA_STAGING_ROOT", str(tmp_path / "staging"))
    monkeypatch.setenv("DATAMARCA_ALERT_STATE_DIR", str(tmp_path / "alert_state"))
    monkeypatch.setattr(tareas, "REPORTES_DIR", tmp_path / "reportes")
    monkeypatch.delenv("DATAMARCA_RETENCION_DIAS", raising=False)
    monkeypatch.setenv("DATAMARCA_WEBHOOK_URL", "https://teams.invalid/secreto-de-prueba")


@pytest.fixture
def post(monkeypatch):
    respuesta = MagicMock()
    respuesta.__enter__.return_value.status = 202
    enviar = MagicMock(return_value=respuesta)
    monkeypatch.setattr(alertas.urllib.request, "urlopen", enviar)
    return enviar


def contexto():
    return {"dag_run": SimpleNamespace(run_id=RUN, dag_id="datamarca_pipeline"), "reason": "timed_out"}


def recibo():
    return json.loads(alertas.ruta_recibo(RUN).read_text())


def test_envio_correcto(post, caplog):
    with caplog.at_level(logging.INFO):
        assert alertas.enviar_teams("resumen español") == {"resultado": "enviado", "codigo_http": 202}
    req = post.call_args.args[0]
    assert json.loads(req.data) == {"text": "resumen español"}
    assert req.get_method() == "POST"
    assert req.get_header("Content-type") == "application/json"
    assert post.call_args.kwargs["timeout"] == 10
    assert post.return_value.__exit__.called
    assert "resultado=enviado" in caplog.text
    assert "secreto-de-prueba" not in caplog.text


def test_sin_webhook(post, monkeypatch):
    monkeypatch.delenv("DATAMARCA_WEBHOOK_URL")
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "sin_webhook"
    assert recibo()["resultado"] == "sin_webhook"
    post.assert_not_called()


@pytest.mark.parametrize("error, resultado", [
    (urllib.error.HTTPError("https://teams.invalid/secreto-de-prueba", 403,
                            "secreto-de-prueba", None, None), "error_http"),
    (TimeoutError("secreto-de-prueba"), "timeout"),
    (urllib.error.URLError(TimeoutError("secreto-de-prueba")), "timeout"),
    (urllib.error.URLError("https://teams.invalid/secreto-de-prueba"), "error_red"),
    (ValueError("https://teams.invalid/secreto-de-prueba"), "error_envio"),
])
def test_errores_no_exponen_webhook(post, caplog, error, resultado):
    post.side_effect = error
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == resultado
    assert recibo()["resultado"] == resultado
    assert "secreto-de-prueba" not in caplog.text
    assert "https://teams.invalid" not in caplog.text


def test_callback_sin_resumen_ni_task_instance(post):
    assert not tareas.REPORTES_DIR.exists()
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "enviado"
    texto = json.loads(post.call_args.args[0].data)["text"]
    assert RUN in texto and "dagrun_timeout" in texto
    assert recibo()["origen"] == "callback_dag"
    assert not tareas.REPORTES_DIR.exists()
    assert not (tareas.dir_corrida(RUN) / "estado").exists()


def test_callback_no_duplica_resumen_enviado_por_notificar(post):
    tareas.notificar(RUN)
    assert recibo()["origen"] == "notificar"
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "duplicado_omitido"
    post.assert_called_once()
    assert recibo()["origen"] == "notificar"


def test_callback_repetido_y_notificar_tardio_no_duplican(post):
    alertas.alerta_fallo_dag(contexto())
    alertas.alerta_fallo_dag(contexto())
    tareas.notificar(RUN)
    post.assert_called_once()
    assert recibo()["origen"] == "callback_dag"


def test_resumen_existente_no_implica_envio_exitoso(post):
    post.side_effect = urllib.error.HTTPError("https://teams.invalid", 500, "fallo", None, None)
    tareas.notificar(RUN)
    assert tareas.REPORTES_DIR.exists()
    assert recibo()["resultado"] == "error_http"
    post.side_effect = None
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "enviado"
    assert post.call_count == 2


def test_emisores_concurrentes_no_duplican(post):
    with ThreadPoolExecutor(max_workers=2) as executor:
        resultados = list(executor.map(lambda _: alertas.alerta_fallo_dag(contexto()), range(2)))
    assert {r["resultado"] for r in resultados} == {"enviado", "duplicado_omitido"}
    post.assert_called_once()


@pytest.mark.parametrize("contenido", ["{", "[]", "null"])
def test_recibo_corrupto_bloquea_envio_sin_deduplicacion(post, contenido):
    estado.preparar()
    alertas.ruta_recibo(RUN).write_text(contenido)
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "error_registro"
    post.assert_not_called()


def test_callback_contexto_minimo_y_sin_id(post):
    assert alertas.alerta_fallo_dag({"run_id": RUN})["resultado"] == "enviado"
    assert alertas.alerta_fallo_dag({})["resultado"] == "contexto_sin_run_id"
    post.assert_called_once()


def test_dag_conserva_callbacks_de_tarea_y_notificar():
    # Verifica el cableado sin importar Airflow; la importación real se valida en Docker.
    ruta = Path(__file__).resolve().parents[1] / "airflow/dags/datamarca_pipeline.py"
    arbol = ast.parse(ruta.read_text())
    llamada = next(n for n in ast.walk(arbol) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Name) and n.func.id == "DAG")
    parametros = {kw.arg: kw.value for kw in llamada.keywords}
    assert parametros["on_failure_callback"].id == "alerta_fallo_dag"
    assert parametros["default_args"].id == "default_args"
    notificar = next(n.value for n in ast.walk(arbol) if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == "notificar" for t in n.targets))
    assert {kw.arg: ast.literal_eval(kw.value) for kw in notificar.keywords}["trigger_rule"] == "all_done"


def test_identidad_determinista_por_corrida_tipo_y_dag():
    assert alertas.identificador(RUN) == alertas.identificador(RUN)
    assert alertas.identificador(RUN, "otro_tipo") != alertas.identificador(RUN)
    assert alertas.identificador(RUN, dag_id="otro_dag") != alertas.identificador(RUN)


def test_pendiente_se_guarda_antes_del_post(post):
    def observar(*args, **kwargs):
        datos = recibo()
        assert datos["estado_envio"] == "sending"
        assert datos["notificacion_id"] == alertas.identificador(RUN)
        assert datos["notificacion_id"] in json.loads(args[0].data)["text"]
        return post.return_value
    post.side_effect = observar
    resultado = alertas.alerta_fallo_dag(contexto())
    assert resultado["estado_envio"] == "confirmed"
    assert resultado["registro_recibo"] == "guardado"
    assert resultado["deduplicacion"] == "disponible"


def test_timeout_durante_envio_no_reintenta_desde_callback(post):
    post.side_effect = TimeoutError("fixture secreto-de-prueba")
    tareas.notificar(RUN)
    assert recibo()["estado_envio"] == "ambiguous"
    post.side_effect = None
    resultado = alertas.alerta_fallo_dag(contexto())
    assert resultado["resultado"] == "ambiguo_omitido"
    assert resultado["estado_envio"] == "ambiguous"
    post.assert_called_once()


def test_interrupcion_despues_del_post_no_duplica(post, monkeypatch):
    guardar = alertas._guardar_recibo
    def interrumpir(ruta, datos):
        if datos["estado_envio"] == "confirmed":
            raise SystemExit("muerte del emisor tras aceptar el POST")
        guardar(ruta, datos)
    monkeypatch.setattr(alertas, "_guardar_recibo", interrumpir)
    with pytest.raises(SystemExit):
        tareas.notificar(RUN)
    assert recibo()["estado_envio"] == "sending"
    monkeypatch.setattr(alertas, "_guardar_recibo", guardar)
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "ambiguo_omitido"
    assert recibo()["estado_envio"] == "ambiguous"
    post.assert_called_once()


def test_callback_concurrente_con_notificar(post):
    from threading import Event
    en_post, liberar = Event(), Event()
    def lento(*args, **kwargs):
        en_post.set()
        assert liberar.wait(5), "no se liberó el POST simulado"
        return post.return_value
    post.side_effect = lento
    with ThreadPoolExecutor(max_workers=2) as executor:
        tarea = executor.submit(tareas.notificar, RUN)
        assert en_post.wait(5)
        callback = executor.submit(alertas.alerta_fallo_dag, contexto())
        liberar.set()
        assert "exito" in tarea.result(timeout=5)
        assert callback.result(timeout=5)["resultado"] == "duplicado_omitido"
    post.assert_called_once()


def test_respaldo_sin_permisos_de_staging(post, monkeypatch, caplog):
    mkdir = Path.mkdir
    def sin_permiso(ruta, *args, **kwargs):
        if ruta == tareas.dir_corrida(RUN):
            raise PermissionError("secreto-de-prueba")
        return mkdir(ruta, *args, **kwargs)
    monkeypatch.setattr(Path, "mkdir", sin_permiso)
    with caplog.at_level(logging.INFO):
        resultado = alertas.alerta_fallo_dag(contexto())
    assert resultado["resultado"] == "enviado"
    assert resultado["estado_envio"] == "confirmed"
    assert resultado["registro_recibo"] == "guardado"
    assert resultado["deduplicacion"] == "disponible"
    assert "envio_http=enviado" in caplog.text
    assert "registro_recibo=guardado" in caplog.text
    assert "deduplicacion=disponible" in caplog.text
    assert "secreto-de-prueba" not in caplog.text
    post.assert_called_once()


def test_fallo_del_almacen_persistente_impide_post_sin_deduplicacion(post, monkeypatch):
    monkeypatch.setattr(alertas, "_guardar_recibo", MagicMock(side_effect=OSError("sin espacio")))
    resultado = alertas.alerta_fallo_dag(contexto())
    assert resultado["resultado"] == "error_registro"
    assert resultado["registro_recibo"] == "fallido"
    assert resultado["deduplicacion"] == "no_disponible"
    post.assert_not_called()


def test_fallo_al_confirmar_recibo_no_repite_post(post, monkeypatch):
    guardar = alertas._guardar_recibo
    def sin_espacio(ruta, datos):
        if datos["estado_envio"] == "confirmed":
            raise OSError("sin espacio")
        guardar(ruta, datos)
    monkeypatch.setattr(alertas, "_guardar_recibo", sin_espacio)
    resultado = alertas.alerta_fallo_dag(contexto())
    assert resultado["estado_envio"] == "confirmed"
    assert resultado["registro_recibo"] == "fallido"
    assert resultado["deduplicacion"] == "degradada"
    assert recibo()["estado_envio"] == "sending"
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "ambiguo_omitido"
    post.assert_called_once()


def test_confirmacion_legacy_no_se_reenvia(post):
    carpeta = tareas.dir_corrida(RUN)
    carpeta.mkdir(parents=True)
    (carpeta / "teams.json").write_text(json.dumps({"run_id": RUN, "resultado": "enviado"}))
    assert alertas.alerta_fallo_dag(contexto())["resultado"] == "duplicado_omitido"
    post.assert_not_called()


def test_callback_no_importa_configuracion_de_negocio(tmp_path):
    import os
    import subprocess
    import sys
    codigo = '''
import sys
from unittest.mock import MagicMock, patch
from orquestacion import alertas
respuesta = MagicMock()
respuesta.__enter__.return_value.status = 202
with patch.object(alertas.urllib.request, "urlopen", return_value=respuesta):
    assert alertas.alerta_fallo_dag({"run_id": "config_invalida"})["resultado"] == "enviado"
assert "orquestacion.tareas" not in sys.modules
'''
    entorno = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
               "UMBRAL_RECHAZO_PCT": "invalido", "DATAMARCA_STAGING_ROOT": str(tmp_path / "otro")}
    subprocess.run([sys.executable, "-B", "-c", codigo], env=entorno, check=True,
                   capture_output=True, timeout=15)


def test_timeout_no_modifica_estados_reales(post):
    ctx = contexto()
    ctx["dag_run"].state = "failed"
    ctx["task_instance"] = SimpleNamespace(state="skipped", task_id="notificar")
    antes = (vars(ctx["dag_run"]).copy(), vars(ctx["task_instance"]).copy())
    alertas.alerta_fallo_dag(ctx)
    assert (vars(ctx["dag_run"]), vars(ctx["task_instance"])) == antes


def test_fallo_http_es_fallido_y_permite_otro_intento(post):
    post.side_effect = urllib.error.HTTPError("https://teams.invalid", 503, "fixture", None, None)
    assert alertas.alerta_fallo_dag(contexto())["estado_envio"] == "failed"
    assert recibo()["estado_envio"] == "failed"
    post.side_effect = None
    assert alertas.alerta_fallo_dag(contexto())["estado_envio"] == "confirmed"
    assert post.call_count == 2


def test_tipos_distintos_no_se_bloquean(post):
    carpeta = tareas.dir_corrida(RUN)
    alertas.enviar_una_vez("resumen", RUN, "notificar")
    alertas.enviar_una_vez("otro evento", RUN, "callback_dag", tipo="otro_tipo")
    assert post.call_count == 2


def test_deduplicacion_entre_procesos(tmp_path):
    import os
    import subprocess
    import sys
    codigo = '''
import json, time
from orquestacion import alertas

def post(texto):
    print("POST", flush=True)
    time.sleep(0.1)
    return {"resultado": "enviado", "codigo_http": 202}

alertas.enviar_teams = post
print("LISTO", flush=True)
input()
resultado = alertas.enviar_una_vez("fixture", "procesos", "fixture")
print(json.dumps(resultado), flush=True)
'''
    entorno = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
               "DATAMARCA_ALERT_STATE_DIR": str(tmp_path / "procesos")}
    procesos = [subprocess.Popen([sys.executable, "-B", "-u", "-c", codigo], env=entorno,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True) for _ in range(2)]
    try:
        for proceso in procesos:
            assert proceso.stdout.readline().strip() == "LISTO"
        for proceso in procesos:
            proceso.stdin.write("enviar\n")
            proceso.stdin.flush()
        salidas = [p.communicate(timeout=15) for p in procesos]
        assert all(p.returncode == 0 for p in procesos)
        lineas = [salida.splitlines() for salida, _ in salidas]
        assert sum(filas.count("POST") for filas in lineas) == 1
        assert {json.loads(filas[-1])["resultado"] for filas in lineas} == {"enviado", "duplicado_omitido"}
    finally:
        for proceso in procesos:
            if proceso.poll() is None:
                proceso.kill()
                proceso.communicate()
