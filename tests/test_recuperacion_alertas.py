"""Recuperación y reconciliación: filesystem temporal real, HTTP siempre simulado."""
import json
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from orquestacion import alertas, tareas
from orquestacion import estado_alertas as estado
from orquestacion import recuperar_alertas as cli

RUN = "manual__recuperacion"
ID = alertas.identificador(RUN)


@pytest.fixture(autouse=True)
def entorno(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAMARCA_ALERT_STATE_DIR", str(tmp_path / "registro"))
    monkeypatch.setenv("DATAMARCA_STAGING_ROOT", str(tmp_path / "staging"))
    monkeypatch.setenv("DATAMARCA_ALERT_LOCK_TIMEOUT_SEG", "0.1")
    monkeypatch.setenv("DATAMARCA_ALERT_RESERVATION_TTL_SEG", "300")
    monkeypatch.setenv("DATAMARCA_WEBHOOK_URL", "https://teams.invalid/secreto-fixture")
    monkeypatch.setattr(tareas, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(tareas, "REPORTES_DIR", tmp_path / "reportes")
    monkeypatch.delenv("DATAMARCA_RETENCION_DIAS", raising=False)
    monkeypatch.delenv("DATAMARCA_LIMPIAR_STAGING", raising=False)


@pytest.fixture
def post(monkeypatch):
    respuesta = MagicMock()
    respuesta.__enter__.return_value.status = 202
    post = MagicMock(return_value=respuesta)
    monkeypatch.setattr(alertas.urllib.request, "urlopen", post)
    return post


def sembrar(actual, segundos=600):
    datos = {"notificacion_id": ID, "run_id": RUN, "dag_id": alertas.DAG_ID,
             "tipo": alertas.TIPO_NOTIFICACION, "origen": "fixture"}
    datos = estado.transicion(datos, actual, "fixture")
    datos["fecha"] = (datetime.now(timezone.utc) - timedelta(seconds=segundos)).isoformat()
    with estado.bloquear(ID):
        estado.guardar(estado.ruta(ID), datos)
    return datos


def emitir():
    return alertas.alerta_fallo_dag({"run_id": RUN, "reason": "timed_out"})


def test_reserva_abandonada_antes_del_post_se_recupera(post, monkeypatch):
    guardar = alertas._guardar_recibo
    def morir_antes_de_sending(ruta, datos):
        guardar(ruta, datos)
        if datos["estado_envio"] == "reserved":
            raise SystemExit()
    monkeypatch.setattr(alertas, "_guardar_recibo", morir_antes_de_sending)
    with pytest.raises(SystemExit):
        emitir()
    assert estado.cargar(ID)["estado_envio"] == "reserved"
    post.assert_not_called()
    monkeypatch.setattr(alertas, "_guardar_recibo", guardar)
    assert emitir()["resultado"] == "reserva_pendiente"  # No robar reservas recientes.
    datos = cli.recuperar(ID, "operador", "reserva sin POST verificada", edad=0)
    assert datos["estado_envio"] == "confirmed"
    assert any(h["estado_nuevo"] == "expired" for h in datos["historial"])
    assert datos["historial"][-2]["usuario"] == "operador"
    assert emitir()["resultado"] == "duplicado_omitido"
    post.assert_called_once()


def test_interrumpido_durante_post_nunca_se_recupera_automaticamente(post):
    post.side_effect = SystemExit()
    with pytest.raises(SystemExit):
        emitir()
    assert estado.cargar(ID)["estado_envio"] == "sending"
    post.side_effect = None
    assert emitir()["estado_envio"] == "ambiguous"
    assert cli.recuperar(ID, "operador", "inspección", edad=0)["estado_envio"] == "ambiguous"
    post.assert_called_once()


def test_expirar_reserva_y_recuperar_con_auditoria(post):
    sembrar("reserved")
    datos = cli.recuperar(ID, "ana", "reserva antigua sin POST", solo_expirar=True)
    assert datos["estado_envio"] == "expired"
    assert datos["historial"][-1]["usuario"] == "ana"
    assert datos["historial"][-1]["motivo"] == "reserva antigua sin POST"
    assert datetime.fromisoformat(datos["historial"][-1]["fecha"]).tzinfo is not None
    post.assert_not_called()
    assert cli.recuperar(ID, "ana", "recuperación controlada")["estado_envio"] == "confirmed"
    post.assert_called_once()


def test_sending_abandonado_pasa_a_ambiguo_sin_post(post):
    sembrar("sending")
    assert cli.recuperar(ID, "ana", "proceso terminado", solo_expirar=True)["estado_envio"] == "ambiguous"
    post.assert_not_called()


def test_confirmado_no_admite_autorizacion_de_reenvio(post):
    sembrar("confirmed")
    with pytest.raises(ValueError, match="confirmada"):
        cli.reconciliar(ID, "autorizar-reenvio", "ana", "fixture", True)
    assert cli.recuperar(ID, "ana", "fixture")["estado_envio"] == "confirmed"
    assert emitir()["resultado"] == "duplicado_omitido"
    post.assert_not_called()


def test_ambiguo_requiere_verificacion_manual_y_guarda_historial(post):
    sembrar("ambiguous")
    antes = estado.ruta(ID).read_bytes()
    with pytest.raises(ValueError, match="verificación"):
        cli.reconciliar(ID, "confirmar", "ana", "fixture")
    assert estado.ruta(ID).read_bytes() == antes
    resultado = cli.reconciliar(ID, "confirmar", "ana", "mensaje localizado en Teams", True)
    assert resultado["estado_envio"] == "confirmed"
    auditoria = resultado["historial"][-1]
    assert auditoria["usuario"] == "ana" and auditoria["accion"] == "confirmar"
    assert auditoria["estado_anterior"] == "ambiguous"
    assert emitir()["resultado"] == "duplicado_omitido"
    post.assert_not_called()


def test_autorizar_no_envia_hasta_recuperacion_explicita(post):
    sembrar("ambiguous")
    resultado = cli.reconciliar(ID, "autorizar-reenvio", "ana", "historial confirma no entregado", True)
    assert resultado["estado_envio"] == "expired" and resultado["reenvio_autorizado"]
    assert emitir()["resultado"] == "reserva_pendiente"
    post.assert_not_called()
    resultado = cli.recuperar(ID, "ana", "ejecutar autorización")
    assert resultado["estado_envio"] == "confirmed"
    assert not resultado["reenvio_autorizado"]
    post.assert_called_once()


def test_listar_pendientes_antiguos_y_ambiguos(post):
    sembrar("reserved")
    assert len(cli.consultar({"reserved", "sending"}, edad=300, abandonadas=True)) == 1
    with estado.bloquear(ID):
        assert cli.consultar({"reserved"}, edad=300, abandonadas=True) == []
    sembrar("ambiguous")
    assert len(cli.consultar({"ambiguous"})) == 1
    assert cli.consultar({"reserved", "sending"}) == []
    post.assert_not_called()


def test_bloqueo_tiene_plazo_y_nunca_envia_sin_deduplicar(post):
    with estado.bloquear(ID):
        inicio = time.monotonic()
        resultado = emitir()
        assert time.monotonic() - inicio < 1.0
        assert resultado["resultado"] == "bloqueo_ocupado"
        assert resultado["deduplicacion"] == "bloqueada"
        post.assert_not_called()
        with pytest.raises(estado.BloqueoOcupado):
            cli.reconciliar(ID, "confirmar", "ana", "fixture", True)
    assert emitir()["estado_envio"] == "confirmed"
    post.assert_called_once()


def test_almacen_no_disponible_no_hace_post(post, tmp_path, monkeypatch, caplog):
    obstaculo = tmp_path / "archivo"
    obstaculo.write_text("no es directorio")
    monkeypatch.setenv("DATAMARCA_ALERT_STATE_DIR", str(obstaculo))
    resultado = emitir()
    assert resultado["resultado"] == "error_registro"
    assert resultado["envio_http"] == "no_intentado"
    assert resultado["deduplicacion"] == "no_disponible"
    assert "no se enviará sin deduplicación" in caplog.text
    assert cli.main(["verificar"]) == 2
    post.assert_not_called()


def test_verificacion_prueba_escritura_real(monkeypatch):
    estado.verificar()
    monkeypatch.setattr(estado.tempfile, "TemporaryFile", MagicMock(side_effect=PermissionError()))
    with pytest.raises(PermissionError):
        estado.verificar()


def test_registro_sobrevive_borrado_completo_de_staging(post):
    tareas.dir_corrida(RUN).mkdir(parents=True)
    emitir()
    shutil.rmtree(tareas.STAGING_ROOT)
    assert estado.cargar(ID)["estado_envio"] == "confirmed"
    assert emitir()["resultado"] == "duplicado_omitido"
    post.assert_called_once()


def test_retencion_no_borra_registro_persistente(post, monkeypatch):
    tareas.escribir_estado(RUN, "unir", {"estado": "fallida"})
    emitir()
    for p in (tareas.dir_corrida(RUN) / "estado").glob("*.json"):
        import os
        os.utime(p, (0, 0))
    monkeypatch.setenv("DATAMARCA_RETENCION_DIAS", "1")
    tareas.aplicar_retencion("otra_corrida", False)
    assert not tareas.dir_corrida(RUN).exists()
    assert estado.cargar(ID)["estado_envio"] == "confirmed"
    assert emitir()["resultado"] == "duplicado_omitido"
    post.assert_called_once()


def test_no_persiste_payload_ni_webhook_y_redacta_urls_de_auditoria(post):
    alertas.enviar_una_vez("credencial-fixture-que-no-debe-persistir", RUN, "fixture")
    cli.reconciliar(ID, "confirmar", "ana", "verificado https://teams.invalid/secreto-fixture", True)
    contenido = estado.ruta(ID).read_text()
    assert "credencial-fixture" not in contenido
    assert "secreto-fixture" not in contenido
    assert "https://" not in contenido
    assert "[secreto omitido]" in contenido


def test_cli_consulta_expira_recupera_y_reconcilia(post, capsys):
    sembrar("reserved")
    assert cli.main(["listar", "--pendientes", "--abandonadas"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["notificacion_id"] == ID
    comunes = ["--id", ID, "--usuario", "ana", "--motivo", "fixture"]
    assert cli.main(["expirar", *comunes]) == 0
    assert json.loads(capsys.readouterr().out)[0]["estado_envio"] == "expired"
    assert cli.main(["recuperar", *comunes]) == 0
    assert json.loads(capsys.readouterr().out)[0]["estado_envio"] == "confirmed"
    assert cli.main(["consultar", ID]) == 0
    assert json.loads(capsys.readouterr().out)["estado_envio"] == "confirmed"
    assert cli.main(["autorizar-reenvio", ID, "--usuario", "ana", "--motivo", "fixture", "--no-entregado"]) == 2
    capsys.readouterr()
    post.assert_called_once()


def test_cli_ambiguo_no_se_reenvia_y_confirmacion_exige_verificacion(post, capsys):
    sembrar("ambiguous")
    assert cli.main(["recuperar", "--id", ID, "--usuario", "ana", "--motivo", "fixture"]) == 2
    capsys.readouterr()
    with pytest.raises(SystemExit):
        cli.main(["confirmar", ID, "--usuario", "ana", "--motivo", "fixture"])
    assert cli.main(["confirmar", ID, "--usuario", "ana", "--motivo", "verificado", "--verificado-en-teams"]) == 0
    assert estado.cargar(ID)["estado_envio"] == "confirmed"
    post.assert_not_called()


def test_autorizacion_y_estado_se_guardan_atomicamente(post, monkeypatch):
    sembrar("ambiguous")
    antes = estado.ruta(ID).read_bytes()
    monkeypatch.setattr(alertas, "_guardar_recibo", MagicMock(side_effect=OSError()))
    with pytest.raises(OSError):
        cli.reconciliar(ID, "autorizar-reenvio", "ana", "no entregado", True)
    assert estado.ruta(ID).read_bytes() == antes
    post.assert_not_called()


@pytest.mark.parametrize("usuario,motivo", [("", "fixture"), ("ana", " ")])
def test_intervencion_exige_usuario_y_motivo(usuario, motivo):
    sembrar("ambiguous")
    with pytest.raises(ValueError):
        cli.reconciliar(ID, "confirmar", usuario, motivo, True)


def test_id_invalido_no_permite_salir_del_directorio():
    with pytest.raises(ValueError):
        estado.cargar("../../otro")


def test_rechaza_almacen_dentro_del_staging(post, monkeypatch):
    monkeypatch.setenv("DATAMARCA_ALERT_STATE_DIR", str(tareas.STAGING_ROOT / "alertas"))
    with pytest.raises(ValueError):
        estado.verificar()
    assert emitir()["resultado"] == "error_registro"
    post.assert_not_called()
