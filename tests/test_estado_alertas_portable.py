"""Contrato de Windows y regresión de importación; los POST no se ejecutan."""
import errno
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from orquestacion import estado_alertas as estado

ID = "a" * 64


@pytest.fixture
def windows(tmp_path, monkeypatch):
    api = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=Mock())
    monkeypatch.setattr(estado, "msvcrt", api)
    monkeypatch.setattr(estado, "fcntl", None)
    monkeypatch.setenv("DATAMARCA_ALERT_STATE_DIR", str(tmp_path / "registro"))
    monkeypatch.setenv("DATAMARCA_STAGING_ROOT", str(tmp_path / "staging"))
    return api


def test_bloqueo_windows_usa_mismo_byte_y_libera_tras_error(windows):
    posiciones = []
    windows.locking.side_effect = lambda fd, mode, size: posiciones.append(
        (os.lseek(fd, 0, os.SEEK_CUR), mode, size))
    with pytest.raises(RuntimeError, match="fixture"):
        with estado.bloquear(ID):
            raise RuntimeError("fixture")
    assert posiciones == [(0, windows.LK_NBLCK, 1), (0, windows.LK_UNLCK, 1)]


@pytest.mark.parametrize("codigo", [errno.EACCES, errno.EAGAIN, errno.EDEADLK])
def test_contencion_windows_respeta_plazo(windows, codigo):
    windows.locking.side_effect = OSError(codigo, "ocupado")
    with pytest.raises(estado.BloqueoOcupado):
        with estado.bloquear(ID, espera=0):
            pytest.fail("nunca continuar sin bloqueo")
    windows.locking.assert_called_once()


def test_error_windows_no_se_confunde_con_contencion(windows):
    windows.locking.side_effect = OSError(errno.EBADF, "descriptor invalido")
    with pytest.raises(OSError) as error:
        with estado.bloquear(ID):
            pytest.fail("nunca continuar sin bloqueo")
    assert error.value.errno == errno.EBADF


def test_guardado_windows_sincroniza_archivo_y_reemplaza(windows, monkeypatch):
    fsync = Mock(wraps=os.fsync)
    monkeypatch.setattr(estado.os, "fsync", fsync)
    with estado.bloquear(ID):
        datos = estado.transicion({"notificacion_id": ID}, "reserved", "fixture")
        estado.guardar(estado.ruta(ID), datos)
        datos = estado.transicion(datos, "confirmed", "fixture")
        estado.guardar(estado.ruta(ID), datos)
    assert fsync.call_count == 2  # Un fsync por archivo; ninguno sobre el directorio.
    assert estado.cargar(ID)["estado_envio"] == "confirmed"
    assert not list(estado.directorio().glob(".teams-*"))


def test_importaciones_sin_fcntl():
    codigo = """
import subprocess, sys
from types import SimpleNamespace
sys.modules['fcntl'] = None
sys.modules['msvcrt'] = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0)
from orquestacion import estado_alertas, tareas, recuperar_alertas
assert estado_alertas.fcntl is None
assert estado_alertas.msvcrt is sys.modules['msvcrt']
"""
    entorno = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    subprocess.run([sys.executable, "-B", "-c", codigo], env=entorno,
                   check=True, capture_output=True, timeout=15)
