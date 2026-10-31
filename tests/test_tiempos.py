"""Pruebas del registro de tiempos por etapa (tarea 5.2)."""
import csv
import json
import time

import pytest

from utils.tiempos import COLUMNAS_HISTORIAL, RegistroTiempos, materializar


def test_mide_duracion_y_filas_de_cada_subetapa():
    registro = RegistroTiempos()
    with registro.medir("ingestion", "csv") as m:
        time.sleep(0.05)
        m.filas = 5000
    sub = registro.subetapas[0]
    assert sub["etapa"] == "ingestion" and sub["subetapa"] == "csv"
    assert sub["duracion_s"] >= 0.05
    assert sub["filas"] == 5000
    assert sub["estado"] == "ok"


def test_suma_las_subetapas_por_etapa_en_orden():
    registro = RegistroTiempos()
    for etapa, sub in [("ingestion", "a"), ("ingestion", "b"), ("transformacion", "c")]:
        with registro.medir(etapa, sub):
            time.sleep(0.02)
    por_etapa = registro.por_etapa()
    assert list(por_etapa) == ["ingestion", "transformacion"]
    assert por_etapa["ingestion"] == pytest.approx(sum(s["duracion_s"] for s in registro.subetapas[:2]))
    assert por_etapa["ingestion"] > por_etapa["transformacion"]


def test_una_excepcion_se_registra_como_error_y_se_propaga():
    registro = RegistroTiempos()
    with pytest.raises(ValueError):
        with registro.medir("transformacion", "join"):
            raise ValueError("falla")
    assert registro.subetapas[0]["estado"] == "error"
    assert registro.estado == "error"


def test_guardar_escribe_json_y_acumula_el_historial(tmp_path):
    for corrida in range(2):
        registro = RegistroTiempos()
        registro.corrida_id = f"corrida_{corrida}"
        with registro.medir("ingestion", "csv") as m:
            m.filas = 10
        with registro.medir("persistencia", "parquet"):
            pass
        ruta = registro.guardar(tmp_path, extra={"transacciones_leidas": 5000, "logs_leidos": 4000})

    datos = json.loads(ruta.read_text(encoding="utf-8"))
    assert datos["corrida_id"] == "corrida_1" and datos["estado"] == "ok"
    assert set(datos["etapas_s"]) == {"ingestion", "persistencia"}
    assert datos["datos"] == {"transacciones_leidas": 5000, "logs_leidos": 4000}
    assert datos["total_s"] >= sum(datos["etapas_s"].values()) - 0.01
    assert datos["total_s"] == pytest.approx(sum(datos["etapas_s"].values()) + datos["sin_etapa_s"], abs=0.01)

    with open(tmp_path / "historial_tiempos.csv", encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))
    assert len(filas) == 4                                        # 2 corridas x 2 subetapas
    assert {f["corrida_id"] for f in filas} == {"corrida_0", "corrida_1"}
    assert all(f["transacciones_leidas"] == "5000" for f in filas)


def test_un_historial_con_menos_columnas_se_migra_sin_perder_filas(tmp_path):
    antiguo = tmp_path / "historial_tiempos.csv"
    antiguo.write_text("corrida_id,inicio,etapa,subetapa,duracion_s,filas,estado,transacciones_leidas,logs_leidos\n"
                       "vieja,2026-10-29T10:00:00,ingestion,csv,1.5,10,ok,5000,5000\n", encoding="utf-8")
    registro = RegistroTiempos()
    registro.corrida_id = "nueva"
    with registro.medir("ingestion", "csv"):
        pass
    registro.guardar(tmp_path)
    with open(antiguo, encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))
    assert [fila["corrida_id"] for fila in filas] == ["vieja", "nueva"]
    assert "shuffle_partitions" in filas[0] and filas[0]["shuffle_partitions"] == ""


def test_materializar_ejecuta_y_cuenta_las_filas(spark):
    df = spark.range(25)
    assert materializar(df) == 25
    assert df.is_cached


def test_el_log_incluye_el_entorno_de_spark(spark, tmp_path):
    registro = RegistroTiempos()
    with registro.medir("ingestion", "x"):
        pass
    entorno = json.loads(registro.guardar(tmp_path, spark).read_text(encoding="utf-8"))["entorno"]
    assert entorno["spark"] == spark.version
    assert entorno["zona_horaria"] == "America/Bogota"
    assert entorno["shuffle_partitions"] == spark.conf.get("spark.sql.shuffle.partitions")


def test_a_csv_texto_tiene_las_columnas_del_historial_y_una_fila_por_subetapa():
    registro = RegistroTiempos()
    registro.corrida_id = "corrida_azure"
    with registro.medir("ingestion", "csv") as m:
        m.filas = 10
    with registro.medir("persistencia", "delta") as m:
        m.filas = 3
    datos = registro.a_dict(extra={"transacciones_leidas": 5000, "logs_leidos": 5000})
    filas = list(csv.DictReader(registro.a_csv_texto(datos).splitlines()))
    assert list(filas[0].keys()) == COLUMNAS_HISTORIAL
    assert [f["subetapa"] for f in filas] == ["csv", "delta"]
    assert all(f["corrida_id"] == "corrida_azure" and f["transacciones_leidas"] == "5000" for f in filas)