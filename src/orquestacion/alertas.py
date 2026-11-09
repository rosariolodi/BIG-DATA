"""Envío Teams con registro persistente; no importa Airflow ni configuración de negocio."""
import hashlib
import json
import logging
import os
import re
import socket
import urllib.error
import urllib.request
from pathlib import Path

from orquestacion import estado_alertas as estado

log = logging.getLogger("datamarca.alertas")
DAG_ID = "datamarca_pipeline"
TIPO_NOTIFICACION = "resultado_corrida"
# Punto único de escritura, también utilizado por la CLI y las pruebas de interrupción.
_guardar_recibo = estado.guardar


def dir_corrida(run_id):
    """Solo para migrar recibos anteriores de staging; no es el almacén actual."""
    raiz = Path(os.getenv("DATAMARCA_HOME", Path(__file__).resolve().parents[2]))
    staging = Path(os.getenv("DATAMARCA_STAGING_ROOT", raiz / "data" / "_staging"))
    return staging / re.sub(r"[^A-Za-z0-9_.-]", "-", run_id)


def identificador(run_id, tipo=TIPO_NOTIFICACION, dag_id=DAG_ID):
    clave = json.dumps([dag_id, run_id, tipo], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(clave.encode("utf-8")).hexdigest()


def ruta_recibo(run_id, tipo=TIPO_NOTIFICACION, dag_id=DAG_ID):
    return estado.ruta(identificador(run_id, tipo, dag_id))


def enviar_teams(texto: str) -> dict:
    """POST de Teams Workflows; nunca registrar URL, cuerpo ni texto de excepciones."""
    url = os.getenv("DATAMARCA_WEBHOOK_URL", "").strip()
    if not url:
        log.error("TEAMS resultado=sin_webhook")
        return {"resultado": "sin_webhook"}
    try:
        req = urllib.request.Request(
            url, data=json.dumps({"text": texto}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as respuesta:
            codigo = respuesta.status
        if not 200 <= codigo < 300:
            log.warning("TEAMS resultado=error_http codigo=%s", codigo)
            return {"resultado": "error_http", "codigo_http": codigo}
        log.info("TEAMS resultado=enviado codigo=%s", codigo)
        return {"resultado": "enviado", "codigo_http": codigo}
    except urllib.error.HTTPError as error:
        error.close()
        log.warning("TEAMS resultado=error_http codigo=%s", error.code)
        return {"resultado": "error_http", "codigo_http": error.code}
    except (TimeoutError, socket.timeout):
        log.warning("TEAMS resultado=timeout")
        return {"resultado": "timeout"}
    except urllib.error.URLError as error:
        resultado = "timeout" if isinstance(error.reason, TimeoutError) else "error_red"
        log.warning("TEAMS resultado=%s", resultado)
        return {"resultado": resultado}
    except Exception as error:
        log.warning("TEAMS resultado=error_envio tipo=%s", type(error).__name__)
        return {"resultado": "error_envio"}



def _estado_http(resultado):
    if resultado["resultado"] == "enviado":
        return "confirmed"
    if resultado["resultado"] in ("sin_webhook", "error_http"):
        return "failed"
    return "ambiguous"


def _registrar(resultado):
    nivel = log.info if resultado.get("estado_envio") == "confirmed" else log.warning
    nivel("TEAMS id=%s envio_http=%s estado_envio=%s registro_recibo=%s deduplicacion=%s",
          resultado["notificacion_id"], resultado["envio_http"], resultado.get("estado_envio"),
          resultado["registro_recibo"], resultado["deduplicacion"])
    return resultado


def _error(id_, error):
    ocupado = isinstance(error, estado.BloqueoOcupado)
    resultado = {"notificacion_id": id_, "resultado": "bloqueo_ocupado" if ocupado else "error_registro",
                 "envio_http": "no_intentado", "estado_envio": None,
                 "registro_recibo": "no_modificado" if ocupado else "fallido",
                 "deduplicacion": "bloqueada" if ocupado else "no_disponible"}
    log.error("TEAMS resultado=%s tipo=%s; no se enviará sin deduplicación", resultado["resultado"],
              type(error).__name__)
    return _registrar(resultado)


def _migrar(base, carpeta):
    """Migración bajo el bloqueo persistente. Nunca copia texto del mensaje ni URL."""
    id_ = base["notificacion_id"]
    opciones = [carpeta / f"teams_{id_}.json"]
    if base["tipo"] == TIPO_NOTIFICACION and base["dag_id"] == DAG_ID:
        opciones.append(carpeta / "teams.json")
    for archivo in opciones:
        if not archivo.exists():
            continue
        viejo = json.loads(archivo.read_text(encoding="utf-8"))
        if not isinstance(viejo, dict):
            raise ValueError("recibo anterior inválido")
        if viejo.get("run_id") != base["run_id"]:
            continue
        anterior = viejo.get("estado_envio")
        if anterior == "confirmado" or viejo.get("resultado") == "enviado":
            nuevo = "confirmed"
        elif anterior in ("pendiente", "ambiguo"):
            nuevo = "ambiguous"  # El formato antiguo no distingue antes/después del POST.
        else:
            nuevo = "failed"
        datos = estado.transicion(base, nuevo, "migracion", motivo="recibo anterior de staging")
        _guardar_recibo(estado.ruta(id_), datos)
        return datos
    return None


def _omitir(datos, resultado):
    return _registrar({**datos, "resultado": resultado, "envio_http": "no_intentado",
                       "registro_recibo": "sin_cambios", "deduplicacion": "disponible"})


def _enviar_bajo_bloqueo(datos, texto, usuario="sistema", motivo=""):
    """Transición durable a sending ANTES del POST. El llamador mantiene flock."""
    archivo = estado.ruta(datos["notificacion_id"])
    enviando = estado.transicion(datos, "sending", "iniciar_envio", usuario, motivo,
                                 envio_http="desconocido")
    _guardar_recibo(archivo, enviando)
    http = enviar_teams(f"[notificacion_id={datos['notificacion_id']}]\n{texto}")
    resultado = estado.transicion(enviando, _estado_http(http), "resultado_http", **http,
                                  envio_http=http["resultado"], registro_recibo="guardado",
                                  deduplicacion="disponible")
    try:
        _guardar_recibo(archivo, resultado)
    except Exception as error:
        # Persiste sending: la próxima invocación lo tratará como ambiguous; no repetir POST.
        resultado.update(registro_recibo="fallido", deduplicacion="degradada")
        log.error("TEAMS registro_recibo=fallido tipo=%s", type(error).__name__)
    return _registrar(resultado)


def enviar_una_vez(texto, run_id, origen, tipo=TIPO_NOTIFICACION, dag_id=DAG_ID, carpeta_legacy=None):
    id_ = identificador(run_id, tipo, dag_id)
    base = {"notificacion_id": id_, "run_id": run_id, "dag_id": dag_id,
            "tipo": tipo, "origen": origen, "creada": estado.ahora()}
    try:
        with estado.bloquear(id_):
            datos = estado.cargar(id_) or _migrar(base, carpeta_legacy or dir_corrida(run_id))
            if datos:
                actual = datos["estado_envio"]
                if actual == "confirmed":
                    return _omitir(datos, "duplicado_omitido")
                if actual == "sending":
                    datos = estado.transicion(datos, "ambiguous", "emisor_interrumpido",
                                               motivo="sending sin emisor que mantenga el bloqueo")
                    _guardar_recibo(estado.ruta(id_), datos)
                elif actual == "reserved" and estado.antiguedad(datos) >= estado.ttl():
                    datos = estado.transicion(datos, "expired", "reserva_abandonada")
                    _guardar_recibo(estado.ruta(id_), datos)
                if datos["estado_envio"] in ("ambiguous", "reserved", "expired"):
                    return _omitir(datos, "ambiguo_omitido" if datos["estado_envio"] == "ambiguous"
                                   else "reserva_pendiente")
            datos = estado.transicion(datos or base, "reserved", "reservar", envio_http="no_iniciado", codigo_http=None)
            _guardar_recibo(estado.ruta(id_), datos)
            return _enviar_bajo_bloqueo(datos, texto)
    except Exception as error:
        return _error(id_, error)


def alerta_fallo_dag(context):
    """Respaldo sin resumen ni imports de tareas.py; no altera estados de Airflow."""
    corrida = context.get("dag_run")
    run_id = getattr(corrida, "run_id", None) or context.get("run_id")
    dag_id = getattr(corrida, "dag_id", None) or getattr(context.get("dag"), "dag_id", DAG_ID)
    if not run_id:
        log.error("TEAMS resultado=contexto_sin_run_id")
        return {"resultado": "contexto_sin_run_id"}
    motivo = "dagrun_timeout" if context.get("reason") in ("timed_out", "dagrun_timeout") else "fallo de la corrida"
    texto = (f"DATAMARCA: DAG {dag_id} FALLIDO\nCorrida: {run_id}\nMotivo: {motivo}.\n"
             "Alerta de respaldo del callback del DAG. Consultar estados y logs en Airflow.")
    return enviar_una_vez(texto, run_id, "callback_dag", dag_id=dag_id)
