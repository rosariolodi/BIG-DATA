"""Registro persistente de notificaciones; no contiene payloads ni credenciales."""
import fcntl
import json
import math
import os
import re
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

ESTADOS = {"reserved", "sending", "confirmed", "failed", "ambiguous", "expired"}


class BloqueoOcupado(TimeoutError):
    """No se adquirió el bloqueo dentro del plazo; nunca autoriza saltar deduplicación."""


def ahora():
    return datetime.now(timezone.utc).isoformat()


def directorio():
    raiz = Path(os.getenv("DATAMARCA_HOME", Path(__file__).resolve().parents[2]))
    destino = Path(os.getenv("DATAMARCA_ALERT_STATE_DIR", raiz / "data" / "alert_state"))
    staging = Path(os.getenv("DATAMARCA_STAGING_ROOT", raiz / "data" / "_staging"))
    if destino.resolve().is_relative_to(staging.resolve()):
        raise ValueError("el registro persistente debe estar fuera del staging")
    return destino


def numero_entorno(nombre, defecto, minimo=0):
    valor = float(os.getenv(nombre, defecto))
    if not math.isfinite(valor) or valor < minimo:
        raise ValueError("configuración numérica inválida")
    return valor


def antiguedad(datos):
    return max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(datos["fecha"])).total_seconds())


def ttl():
    return numero_entorno("DATAMARCA_ALERT_RESERVATION_TTL_SEG", "300", minimo=0.001)


def ruta(id_):
    if not re.fullmatch(r"[0-9a-f]{64}", id_):
        raise ValueError("identificador inválido")
    return directorio() / f"teams_{id_}.json"


def preparar():
    directorio().mkdir(parents=True, exist_ok=True, mode=0o700)


def verificar():
    """Prueba efectiva de creación/escritura/fsync; no usa os.access como garantía."""
    preparar()
    with tempfile.TemporaryFile(dir=directorio()) as prueba:
        prueba.write(b"verificacion")
        prueba.flush()
        os.fsync(prueba.fileno())
    return {"almacenamiento": "disponible", "deduplicacion": "disponible"}


@contextmanager
def bloquear(id_, espera=None):
    archivo = ruta(id_).with_suffix(".lock")
    limite = numero_entorno("DATAMARCA_ALERT_LOCK_TIMEOUT_SEG", "2") if espera is None else espera
    if not math.isfinite(limite) or limite < 0:
        raise ValueError("plazo de bloqueo inválido")
    preparar()
    fd = os.open(archivo, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "a+") as bloqueo:
        fin = time.monotonic() + limite
        while True:
            try:
                fcntl.flock(bloqueo, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                restante = fin - time.monotonic()
                if restante <= 0:
                    raise BloqueoOcupado("bloqueo ocupado") from None
                time.sleep(min(0.05, restante))
        try:
            yield
        finally:
            fcntl.flock(bloqueo, fcntl.LOCK_UN)


def cargar(id_):
    archivo = ruta(id_)
    if not archivo.exists():
        return None
    datos = json.loads(archivo.read_text(encoding="utf-8"))
    if (not isinstance(datos, dict) or datos.get("notificacion_id") != id_
            or datos.get("estado_envio") not in ESTADOS):
        raise ValueError("registro inválido")
    return datos


def guardar(archivo, datos):
    """El llamador mantiene el bloqueo. Publica JSON y auditoría en una sola operación."""
    temporal = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=archivo.parent, prefix=".teams-",
                                         encoding="utf-8", delete=False) as salida:
            temporal = Path(salida.name)
            json.dump(datos, salida, ensure_ascii=False, indent=2)
            salida.flush()
            os.fsync(salida.fileno())
        temporal.replace(archivo)
        # También sincronizar el directorio para persistir el rename ante caída del host.
        fd = os.open(archivo.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporal is not None:
            temporal.unlink(missing_ok=True)


def transicion(datos, nuevo, accion, usuario="sistema", motivo="", **extra):
    if nuevo not in ESTADOS:
        raise ValueError("estado inválido")
    fecha = ahora()
    evento = {"usuario": usuario, "fecha": fecha, "motivo": motivo, "accion": accion,
              "estado_anterior": datos.get("estado_envio"), "estado_nuevo": nuevo}
    return {**datos, **extra, "estado_envio": nuevo, "fecha": fecha,
            "historial": [*datos.get("historial", []), evento]}


def datos_intervencion(usuario, motivo):
    if not usuario.strip() or not motivo.strip():
        raise ValueError("usuario y motivo son obligatorios")
    # No conservar URLs ni el secreto del webhook en motivos aportados por operadores.
    def limpiar(texto):
        secreto = os.getenv("DATAMARCA_WEBHOOK_URL", "").strip()
        if secreto:
            texto = texto.replace(secreto, "[secreto omitido]")
        return re.sub(r"https?://\S+", "[URL omitida]", texto.strip())[:1000]
    return limpiar(usuario), limpiar(motivo)


def listar(estados=None, edad=None):
    if not directorio().is_dir():
        raise FileNotFoundError("almacenamiento de alertas no inicializado")
    salida = []
    for archivo in sorted(directorio().glob("teams_*.json")):
        id_ = archivo.stem.removeprefix("teams_")
        datos = cargar(id_)
        if estados and datos["estado_envio"] not in estados:
            continue
        if edad is not None and antiguedad(datos) < edad:
            continue
        # La antigüedad es una candidatura, no prueba de que el emisor haya muerto.
        salida.append(datos)
    return salida
