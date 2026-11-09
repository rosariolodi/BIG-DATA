"""CLI operativa del registro Teams. No accede a Airflow ni cambia sus estados.

PYTHONPATH=src python -m orquestacion.recuperar_alertas --help
"""
import argparse
import json
import logging
import math

from orquestacion import alertas
from orquestacion import estado_alertas as estado


def consultar(estados=None, edad=None, abandonadas=False):
    edad = estado.ttl() if edad is None else edad
    salida = []
    for datos in estado.listar(estados, edad if abandonadas else None):
        candidato = datos["estado_envio"] in ("reserved", "sending") and estado.antiguedad(datos) >= edad
        activo = None
        if candidato:
            try:
                with estado.bloquear(datos["notificacion_id"], espera=0):
                    actual = estado.cargar(datos["notificacion_id"])
                    candidato = (actual["estado_envio"] in ("reserved", "sending")
                                 and estado.antiguedad(actual) >= edad)
                    activo = False
                    datos = actual
            except estado.BloqueoOcupado:
                activo = True
                candidato = False
        if abandonadas and not candidato:
            continue
        salida.append({**datos, "abandonada": candidato, "emisor_activo": activo})
    return salida


def reconciliar(id_, accion, usuario, motivo, verificado=False):
    usuario, motivo = estado.datos_intervencion(usuario, motivo)
    if not verificado:
        raise ValueError("debe confirmar la verificación de entrega en Teams")
    with estado.bloquear(id_):
        datos = estado.cargar(id_)
        if datos is None:
            raise ValueError("notificación inexistente")
        if accion == "confirmar":
            nuevo = "confirmed"
        elif accion == "autorizar-reenvio":
            if datos["estado_envio"] == "confirmed":
                raise ValueError("no se autoriza reenviar una notificación confirmada")
            nuevo = "expired"
        else:
            raise ValueError("acción inválida")
        resultado = estado.transicion(datos, nuevo, accion, usuario, motivo,
                                      reenvio_autorizado=accion == "autorizar-reenvio")
        alertas._guardar_recibo(estado.ruta(id_), resultado)
        return resultado


def recuperar(id_, usuario, motivo, edad=None, solo_expirar=False):
    """Recupera únicamente una reserva segura o un reenvío autorizado explícitamente."""
    usuario, motivo = estado.datos_intervencion(usuario, motivo)
    edad = estado.ttl() if edad is None else edad
    if not math.isfinite(edad) or edad < 0:
        raise ValueError("antigüedad inválida")
    with estado.bloquear(id_):
        datos = estado.cargar(id_)
        if datos is None:
            raise ValueError("notificación inexistente")
        actual = datos["estado_envio"]
        if actual in ("reserved", "sending") and estado.antiguedad(datos) >= edad:
            nuevo = "expired" if actual == "reserved" else "ambiguous"
            datos = estado.transicion(datos, nuevo, "expirar_reserva" if actual == "reserved"
                                      else "reconciliar_envio_interrumpido", usuario, motivo)
            alertas._guardar_recibo(estado.ruta(id_), datos)
        if solo_expirar or datos["estado_envio"] != "expired":
            return {**datos, "resultado": "sin_reenvio", "envio_http": "no_intentado"}
        # expired solo procede de reserved (sin POST) o de autorización manual verificada.
        datos = estado.transicion(datos, "reserved", "recuperar", usuario, motivo,
                                  reenvio_autorizado=False, origen="recuperacion_cli", envio_http="no_iniciado", codigo_http=None)
        alertas._guardar_recibo(estado.ruta(id_), datos)
        texto = (f"DATAMARCA: notificación recuperada. DAG: {datos['dag_id']}\n"
                 f"Corrida: {datos['run_id']}\nTipo: {datos['tipo']}\n"
                 "Consulte los estados reales de la corrida en Airflow. Este aviso no reemplaza el resumen original.")
        return alertas._enviar_bajo_bloqueo(datos, texto, usuario, motivo)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    comandos = parser.add_subparsers(dest="comando", required=True)
    comandos.add_parser("verificar", help="comprobar escritura efectiva del almacenamiento")
    lista = comandos.add_parser("listar")
    lista.add_argument("--estado", action="append", choices=sorted(estado.ESTADOS))
    lista.add_argument("--pendientes", action="store_true")
    lista.add_argument("--abandonadas", action="store_true")
    lista.add_argument("--antiguedad-seg", type=float)
    vista = comandos.add_parser("consultar")
    vista.add_argument("id")
    for nombre in ("confirmar", "autorizar-reenvio", "recuperar", "expirar"):
        comando = comandos.add_parser(nombre)
        if nombre in ("confirmar", "autorizar-reenvio"):
            comando.add_argument("id")
            comando.add_argument("--verificado-en-teams" if nombre == "confirmar" else "--no-entregado",
                                 action="store_true", required=True, dest="verificado")
        else:
            comando.add_argument("--id")
            comando.add_argument("--antiguedad-seg", type=float)
        comando.add_argument("--usuario", required=True)
        comando.add_argument("--motivo", required=True)
    args = parser.parse_args(argv)
    try:
        if args.comando == "verificar":
            resultado = estado.verificar()
        elif args.comando == "listar":
            edad = estado.ttl() if args.antiguedad_seg is None else args.antiguedad_seg
            if not math.isfinite(edad) or edad < 0:
                raise ValueError("antigüedad inválida")
            estados = args.estado
            if args.pendientes:
                estados = ["reserved", "sending", "expired", "failed"]
            resultado = consultar(estados, edad, args.abandonadas)
        elif args.comando == "consultar":
            resultado = estado.cargar(args.id)
            if resultado is None:
                raise ValueError("notificación inexistente")
        elif args.comando in ("confirmar", "autorizar-reenvio"):
            resultado = reconciliar(args.id, args.comando, args.usuario, args.motivo, args.verificado)
        else:
            ids = [args.id] if args.id else [d["notificacion_id"] for d in estado.listar(
                {"reserved", "sending", "expired"})]
            resultado = []
            errores = False
            for id_ in ids:
                try:
                    resultado.append(recuperar(id_, args.usuario, args.motivo, args.antiguedad_seg,
                                                args.comando == "expirar"))
                except Exception as error:
                    errores = True
                    resultado.append({"notificacion_id": id_, "error": type(error).__name__})
            print(json.dumps(resultado, ensure_ascii=False, indent=2))
            return 2 if errores or any(r.get("estado_envio") in ("ambiguous", "failed")
                                      or r.get("registro_recibo") == "fallido" for r in resultado) else 0
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
        return 0
    except Exception as error:
        # No volcar mensajes de excepción: pueden incluir configuración privada.
        logging.getLogger("datamarca.alertas").error("CLI alertas: operación fallida tipo=%s", type(error).__name__)
        print(json.dumps({"error": type(error).__name__, "resultado": "operacion_no_completada"}))
        return 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
