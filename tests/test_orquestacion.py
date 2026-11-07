"""Pruebas del manejo de errores y de las trazas de la orquestación (Unidad 4, tarea T1.3).

Prueban src/orquestacion/tareas.py sin Spark ni Airflow: las funciones de negocio se reemplazan
por funciones de prueba y la zona de trabajo se redirige a una carpeta temporal. Así se verifica
lo que pide la guía (paso 3 y recomendación del fallo intermedio) en cualquier equipo:

  * una excepción en una tarea no se pierde: devuelve código 1 (Airflow la marca fallida y reintenta),
    queda en el log con su traceback y en el archivo de estado con su tipo y mensaje;
  * el fallo forzado (DATAMARCA_FALLAR_EN) y la demora para probar timeouts (DATAMARCA_DEMORAR_EN)
    solo afectan a la tarea indicada, y quedan avisados en el log y en el resumen;
  * la compuerta de calidad detiene la corrida por encima del umbral;
  * la notificación distingue las tareas fallidas, interrumpidas, no ejecutadas y completadas,
    y nunca cambia el resultado del procesamiento;
  * la retención de la zona de trabajo: limpieza de las corridas exitosas y purga por antigüedad.

Detalle en docs/orquestacion/manejo_errores_logging.md.
"""
import json
import logging
import os
import time

import pytest

from orquestacion import tareas

# run_id real de Airflow: trae ':' y '+', que tareas.limpiar_run_id reemplaza
RUN = "manual__2026-11-07T02:22:55.545687+00:00"
INTERMEDIOS = ("validos_transacciones", "validos_logs_ecommerce", "limpio_transacciones", "unida")


@pytest.fixture
def corrida(tmp_path, monkeypatch):
    """Zona de trabajo y reportes en una carpeta temporal, sin variables de la demostración."""
    monkeypatch.setattr(tareas, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(tareas, "REPORTES_DIR", tmp_path / "reportes")
    for variable in ("DATAMARCA_FALLAR_EN", "DATAMARCA_FALLAR_INTENTOS", "DATAMARCA_DEMORAR_EN",
                     "DATAMARCA_LIMPIAR_STAGING", "DATAMARCA_RETENCION_DIAS", "DATAMARCA_WEBHOOK_URL"):
        monkeypatch.delenv(variable, raising=False)
    return tmp_path


def _estado(tarea, estado="exitosa", **extra):
    tareas.escribir_estado(RUN, tarea, {"tarea": tarea, "estado": estado, "segundos": 1.0,
                                        "intento": 1, **extra})


def _metricas(fuente, pct_rechazo, leidos=5000):
    carpeta = tareas.dir_corrida(RUN) / "metricas"
    carpeta.mkdir(parents=True, exist_ok=True)
    rechazados = round(leidos * pct_rechazo / 100)
    (carpeta / f"{fuente}.json").write_text(json.dumps(
        {"leidos": leidos, "validos": leidos - rechazados, "cuarentena": rechazados,
         "pct_rechazo": pct_rechazo}), encoding="utf-8")


def _intermedios():
    for nombre in INTERMEDIOS:
        (tareas.dir_corrida(RUN) / nombre).mkdir(parents=True, exist_ok=True)


def _resumen():
    rid = tareas.limpiar_run_id(RUN)
    texto = (tareas.REPORTES_DIR / f"resumen_{rid}.md").read_text(encoding="utf-8")
    datos = json.loads((tareas.REPORTES_DIR / f"resumen_{rid}.json").read_text(encoding="utf-8"))
    return texto, datos


# ---------------------------------------------------------------------------
# Envoltorio de cada tarea: ejecutar()
# ---------------------------------------------------------------------------
def test_el_run_id_de_airflow_se_convierte_en_una_carpeta_valida(corrida):
    nombre = tareas.dir_corrida(RUN).name
    assert ":" not in nombre and "+" not in nombre
    assert nombre == "manual__2026-11-07T02-22-55.545687-00-00"


def test_una_excepcion_devuelve_codigo_1_y_queda_en_el_log_y_en_el_estado(corrida, monkeypatch, caplog):
    def explota(run_id):
        raise ValueError("origen corrupto")
    monkeypatch.setitem(tareas.TAREAS, "unir", explota)

    codigo = tareas.ejecutar("unir", RUN, 2)

    assert codigo == 1                                    # Airflow marca la tarea fallida y reintenta
    estado = tareas.leer_estados(RUN)["unir"]
    assert estado["estado"] == "fallida"
    assert estado["error"] == "ValueError: origen corrupto"
    assert estado["intento"] == 2 and "segundos" in estado
    assert "FALLO unir" in caplog.text
    assert "Traceback" in caplog.text                     # el log de la tarea trae la traza completa


def test_una_tarea_exitosa_deja_estado_con_detalle_y_traza_de_inicio_y_fin(corrida, monkeypatch, caplog):
    monkeypatch.setitem(tareas.TAREAS, "agregar", lambda run_id: {"tablas": {"ventas_por_dia": 31}})

    with caplog.at_level(logging.INFO, logger="datamarca"):
        codigo = tareas.ejecutar("agregar", RUN, 1)

    assert codigo == 0
    estado = tareas.leer_estados(RUN)["agregar"]
    assert estado["estado"] == "exitosa"
    assert estado["detalle"] == {"tablas": {"ventas_por_dia": 31}}
    assert "INICIO agregar" in caplog.text and "FIN agregar OK" in caplog.text


def test_el_estado_en_ejecucion_se_escribe_antes_de_correr_la_tarea(corrida, monkeypatch):
    """Si Airflow mata el proceso (timeout), el estado queda 'en_ejecucion' y no se pierde el rastro."""
    visto = {}

    def espia(run_id):
        visto.update(tareas.leer_estados(run_id)["persistir_gold"])
        return {}
    monkeypatch.setitem(tareas.TAREAS, "persistir_gold", espia)

    tareas.ejecutar("persistir_gold", RUN, 1)

    assert visto["estado"] == "en_ejecucion"


def test_clasificar_distingue_interrumpidas_y_no_ejecutadas(corrida):
    _estado("ingestar_transacciones")
    _estado("limpiar_logs", estado="en_ejecucion")        # proceso terminado por timeout
    clasificacion = tareas.clasificar(tareas.leer_estados(RUN))
    assert clasificacion["ingestar_transacciones"] == "exitosa"
    assert clasificacion["limpiar_logs"] == "interrumpida"
    assert clasificacion["unir"] == "no_ejecutada"          # upstream_failed en Airflow


# ---------------------------------------------------------------------------
# Fallo forzado (DATAMARCA_FALLAR_EN): insumo de la evidencia de T1.3
# ---------------------------------------------------------------------------
def test_el_fallo_forzado_solo_afecta_a_la_tarea_indicada(corrida, monkeypatch):
    llamadas = []
    monkeypatch.setenv("DATAMARCA_FALLAR_EN", "limpiar_logs")
    monkeypatch.setitem(tareas.TAREAS, "limpiar_logs", lambda run_id: llamadas.append("logs"))
    monkeypatch.setitem(tareas.TAREAS, "limpiar_transacciones", lambda run_id: llamadas.append("tx"))

    assert tareas.ejecutar("limpiar_logs", RUN, 1) == 1
    assert tareas.ejecutar("limpiar_transacciones", RUN, 1) == 0

    assert llamadas == ["tx"]                              # la tarea forzada no llegó a ejecutarse
    estados = tareas.leer_estados(RUN)
    assert estados["limpiar_logs"]["error"].startswith("RuntimeError: FALLO FORZADO en 'limpiar_logs'")
    assert estados["limpiar_transacciones"]["estado"] == "exitosa"


def test_el_fallo_forzado_en_el_primer_intento_permite_la_recuperacion(corrida, monkeypatch):
    monkeypatch.setenv("DATAMARCA_FALLAR_EN", "unir")
    monkeypatch.setenv("DATAMARCA_FALLAR_INTENTOS", "1")
    monkeypatch.setitem(tareas.TAREAS, "unir", lambda run_id: {"filas": 5000})

    assert tareas.ejecutar("unir", RUN, 1) == 1            # primer intento: falla
    assert tareas.ejecutar("unir", RUN, 2) == 0            # reintento de Airflow: se recupera

    estado = tareas.leer_estados(RUN)["unir"]
    assert estado["estado"] == "exitosa" and estado["intento"] == 2


def test_la_demora_forzada_solo_afecta_a_la_tarea_indicada(corrida, monkeypatch, caplog):
    esperas = []
    monkeypatch.setattr(tareas.time, "sleep", esperas.append)   # no se espera de verdad
    monkeypatch.setenv("DATAMARCA_DEMORAR_EN", "compuerta_calidad:300")
    monkeypatch.setitem(tareas.TAREAS, "compuerta_calidad", lambda run_id: {})
    monkeypatch.setitem(tareas.TAREAS, "unir", lambda run_id: {})

    assert tareas.ejecutar("compuerta_calidad", RUN, 1) == 0
    assert tareas.ejecutar("unir", RUN, 1) == 0

    assert esperas == [300.0]
    assert "DEMORA FORZADA en 'compuerta_calidad': 300 s" in caplog.text


def test_las_variables_de_demostracion_se_avisan_en_el_log_y_en_el_resumen(corrida, monkeypatch, caplog):
    monkeypatch.setenv("DATAMARCA_FALLAR_EN", "limpiar_logs")
    monkeypatch.setitem(tareas.TAREAS, "ingestar_logs", lambda run_id: {})

    tareas.ejecutar("ingestar_logs", RUN, 1)                   # una tarea que no es la del fallo
    assert "Modo demostración activo: DATAMARCA_FALLAR_EN=limpiar_logs" in caplog.text

    _corrida_con_fallo_en_limpiar_logs()
    tareas.notificar(RUN)
    texto, _ = _resumen()
    assert "- Modo demostración: DATAMARCA_FALLAR_EN=limpiar_logs" in texto


# ---------------------------------------------------------------------------
# Compuerta de calidad (T5) y fallos rápidos de la ingestión
# ---------------------------------------------------------------------------
def test_la_compuerta_detiene_la_corrida_si_el_rechazo_supera_el_umbral(corrida):
    _metricas("transacciones", pct_rechazo=0.5)
    _metricas("logs_ecommerce", pct_rechazo=0.0)

    assert tareas.ejecutar("compuerta_calidad", RUN, 1) == 1

    error = tareas.leer_estados(RUN)["compuerta_calidad"]["error"]
    assert error.startswith("CompuertaCalidadError")
    assert "transacciones" in error and "logs_ecommerce" not in error


def test_la_compuerta_deja_pasar_un_rechazo_igual_al_umbral(corrida):
    _metricas("transacciones", pct_rechazo=tareas.UMBRAL_RECHAZO_PCT)
    _metricas("logs_ecommerce", pct_rechazo=0.0)

    resultado = tareas.compuerta_calidad(RUN)

    assert resultado["pct_rechazo"]["transacciones"] == tareas.UMBRAL_RECHAZO_PCT


def test_la_compuerta_falla_si_falta_la_metrica_de_una_fuente(corrida):
    _metricas("transacciones", pct_rechazo=0.0)
    with pytest.raises(FileNotFoundError, match="logs_ecommerce"):
        tareas.compuerta_calidad(RUN)


def test_si_falta_un_resultado_intermedio_la_tarea_falla_con_un_mensaje_claro(corrida):
    with pytest.raises(FileNotFoundError, match="unida"):
        tareas.leer_stage(None, RUN, "unida")              # falla antes de tocar Spark


@pytest.mark.parametrize("contenido, error, mensaje", [
    (None, FileNotFoundError, "no existe"),                  # el archivo no está
    ("", FileNotFoundError, "vacío"),
    ("{no es json", ValueError, None),                       # JSONDecodeError es un ValueError
    ('{"evento": "click"}', ValueError, "arreglo"),
    ("[]", ValueError, "arreglo"),
])
def test_ingestar_logs_falla_rapido_ante_un_origen_invalido(corrida, monkeypatch, contenido, error, mensaje):
    from config import rutas

    origen = corrida / "logs.json"
    if contenido is not None:
        origen.write_text(contenido, encoding="utf-8")
    monkeypatch.setattr(rutas, "JSON_LOGS", str(origen))

    with pytest.raises(error, match=mensaje):
        tareas.ingestar_logs(RUN)


def test_ingestar_logs_cuenta_las_filas_de_un_origen_valido(corrida, monkeypatch):
    from config import rutas

    origen = corrida / "logs.json"
    origen.write_text('[{"user_id": 1}, {"user_id": 2}]', encoding="utf-8")
    monkeypatch.setattr(rutas, "JSON_LOGS", str(origen))

    assert tareas.ingestar_logs(RUN)["filas_origen"] == 2


def test_ingestar_transacciones_falla_si_faltan_columnas(corrida, monkeypatch):
    pytest.importorskip("pyspark")                           # config.schemas usa tipos de Spark
    from config import rutas

    origen = corrida / "tx.csv"
    origen.write_text("columna_que_no_existe\n1\n", encoding="utf-8")
    monkeypatch.setattr(rutas, "CSV_TRANSACCIONES", str(origen))

    with pytest.raises(ValueError, match="no tiene las columnas"):
        tareas.ingestar_transacciones(RUN)


# ---------------------------------------------------------------------------
# Notificación (T12): trazas de la corrida y retención de la zona de trabajo
# ---------------------------------------------------------------------------
def _corrida_con_fallo_en_limpiar_logs():
    for t in ("ingestar_transacciones", "ingestar_logs", "validar_transacciones", "validar_logs",
              "compuerta_calidad", "limpiar_transacciones"):
        _estado(t)
    _estado("limpiar_logs", estado="fallida", intento=4,
            error="RuntimeError: FALLO FORZADO en 'limpiar_logs' (intento 4) por DATAMARCA_FALLAR_EN")


def _corrida_exitosa():
    for t in tareas.TAREAS_ORDEN:
        _estado(t)
    _metricas("transacciones", 0.0)
    _metricas("logs_ecommerce", 0.0)


def test_la_notificacion_de_una_corrida_fallida_separa_fallidas_canceladas_y_completadas(corrida, caplog):
    _corrida_con_fallo_en_limpiar_logs()

    assert tareas.ejecutar("notificar", RUN, 1) == 0       # notificar no cambia el resultado

    texto, datos = _resumen()
    assert "CON FALLO" in texto
    assert "FALLÓ `limpiar_logs` (intento 4)" in texto
    assert "No se ejecutaron por la falla: unir, agregar, persistir_silver, persistir_gold" in texto
    assert ("Completadas antes/durante el fallo: ingestar_transacciones, ingestar_logs, "
            "validar_transacciones, validar_logs, compuerta_calidad, limpiar_transacciones") in texto
    assert datos["exito"] is False
    assert datos["tareas"]["limpiar_logs"] == "fallida" and datos["tareas"]["unir"] == "no_ejecutada"
    assert any(r.levelname == "ERROR" and "NOTIFICACIÓN" in r.getMessage() for r in caplog.records)


def test_una_tarea_detenida_por_timeout_aparece_como_interrumpida(corrida):
    for t in ("ingestar_transacciones", "ingestar_logs", "validar_transacciones", "validar_logs"):
        _estado(t)
    _estado("compuerta_calidad", estado="en_ejecucion", intento=2)   # Airflow la mató al vencer el tiempo

    tareas.notificar(RUN)

    texto, datos = _resumen()
    assert "FALLÓ `compuerta_calidad` (intento 2): sin cierre (timeout o proceso terminado)" in texto
    assert datos["tareas"]["compuerta_calidad"] == "interrumpida"
    assert datos["tareas"]["limpiar_logs"] == "no_ejecutada"


def test_la_notificacion_avisa_cuando_solo_una_capa_quedo_escrita(corrida):
    _corrida_exitosa()
    _estado("persistir_gold", estado="fallida", error="OSError: disco lleno")

    tareas.notificar(RUN)

    texto, _ = _resumen()
    assert "ATENCIÓN: solo una capa (silver o gold) quedó escrita" in texto


@pytest.mark.parametrize("limpiar, quedan_intermedios", [("true", False), ("false", True), (None, True)])
def test_retencion_de_una_corrida_exitosa(corrida, monkeypatch, limpiar, quedan_intermedios):
    if limpiar is not None:
        monkeypatch.setenv("DATAMARCA_LIMPIAR_STAGING", limpiar)
    _corrida_exitosa()
    _intermedios()

    assert tareas.notificar(RUN) == {"exito": True}

    carpeta = tareas.dir_corrida(RUN)
    assert all((carpeta / n).exists() == quedan_intermedios for n in INTERMEDIOS)
    assert (carpeta / "estado").is_dir() and (carpeta / "metricas").is_dir()   # la traza se conserva


def test_una_corrida_fallida_conserva_sus_intermedios_aunque_la_limpieza_este_activa(corrida, monkeypatch):
    monkeypatch.setenv("DATAMARCA_LIMPIAR_STAGING", "true")
    _corrida_con_fallo_en_limpiar_logs()
    _intermedios()

    tareas.notificar(RUN)

    assert all((tareas.dir_corrida(RUN) / n).is_dir() for n in INTERMEDIOS)


def test_un_fallo_del_webhook_no_cambia_el_resultado_de_la_corrida(corrida, monkeypatch, caplog):
    def sin_red(*args, **kwargs):
        raise OSError("sin red")
    monkeypatch.setenv("DATAMARCA_WEBHOOK_URL", "https://ejemplo.invalid/webhook")
    monkeypatch.setattr(tareas.urllib.request, "urlopen", sin_red)
    _corrida_exitosa()

    assert tareas.ejecutar("notificar", RUN, 1) == 0

    texto, _ = _resumen()                                   # el resumen se escribió antes del envío
    assert "EXITOSA" in texto
    assert "No se pudo completar la notificación: sin red" in caplog.text


def test_la_retencion_no_depende_de_que_el_webhook_funcione(corrida, monkeypatch):
    """Brecha B4 de manejo_errores_logging.md, corregida: la limpieza tiene su propio try."""
    def sin_red(*args, **kwargs):
        raise OSError("sin red")
    monkeypatch.setenv("DATAMARCA_WEBHOOK_URL", "https://ejemplo.invalid/webhook")
    monkeypatch.setenv("DATAMARCA_LIMPIAR_STAGING", "true")
    monkeypatch.setenv("DATAMARCA_RETENCION_DIAS", "7")
    monkeypatch.setattr(tareas.urllib.request, "urlopen", sin_red)
    vieja = _carpeta_corrida("manual__vieja", dias_atras=8)
    _corrida_exitosa()
    _intermedios()

    tareas.notificar(RUN)

    assert not any((tareas.dir_corrida(RUN) / n).exists() for n in INTERMEDIOS)
    assert not vieja.exists()


# ---------------------------------------------------------------------------
# Retención por antigüedad (DATAMARCA_RETENCION_DIAS)
# ---------------------------------------------------------------------------
def _carpeta_corrida(nombre, dias_atras):
    """Carpeta de una corrida anterior cuya última escritura de estado fue hace `dias_atras` días."""
    carpeta = tareas.STAGING_ROOT / nombre
    (carpeta / "estado").mkdir(parents=True)
    (carpeta / "unida").mkdir()
    archivo = carpeta / "estado" / "ingestar_logs.json"
    archivo.write_text("{}", encoding="utf-8")
    marca = time.time() - dias_atras * 86400
    os.utime(archivo, (marca, marca))
    return carpeta


def test_la_purga_borra_solo_las_corridas_mas_antiguas_que_el_plazo(corrida, monkeypatch):
    monkeypatch.setenv("DATAMARCA_RETENCION_DIAS", "7")
    vieja = _carpeta_corrida("manual__vieja", dias_atras=8)
    reciente = _carpeta_corrida("manual__reciente", dias_atras=6)
    ajena = tareas.STAGING_ROOT / "carpeta_ajena"              # sin estado/: no la creó tareas.py
    ajena.mkdir()
    os.utime(ajena, (time.time() - 30 * 86400,) * 2)
    _corrida_con_fallo_en_limpiar_logs()                         # la corrida actual

    resultado = tareas.aplicar_retencion(RUN, exito=False)

    assert resultado["purgadas"] == ["manual__vieja"]
    assert not vieja.exists()
    assert reciente.exists() and ajena.exists() and tareas.dir_corrida(RUN).exists()


def test_la_corrida_actual_nunca_se_purga(corrida, monkeypatch):
    monkeypatch.setenv("DATAMARCA_RETENCION_DIAS", "1")
    _corrida_con_fallo_en_limpiar_logs()
    for archivo in (tareas.dir_corrida(RUN) / "estado").glob("*.json"):
        os.utime(archivo, (time.time() - 30 * 86400,) * 2)

    assert tareas.aplicar_retencion(RUN, exito=False)["purgadas"] == []
    assert tareas.dir_corrida(RUN).is_dir()


@pytest.mark.parametrize("valor", [None, "", "0"])
def test_sin_plazo_de_retencion_no_se_purga_nada(corrida, monkeypatch, valor):
    if valor is not None:
        monkeypatch.setenv("DATAMARCA_RETENCION_DIAS", valor)
    vieja = _carpeta_corrida("manual__vieja", dias_atras=400)

    assert tareas.aplicar_retencion(RUN, exito=True)["purgadas"] == []
    assert vieja.exists()
