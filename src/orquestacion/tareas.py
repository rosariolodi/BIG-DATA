"""
Puntos de entrada por tarea del pipeline DATAMARCA (Unidad 4, tarea T1.2).

Cada tarea del DAG (airflow/dags/datamarca_pipeline.py) ejecuta un proceso independiente:

    python src/orquestacion/tareas.py <tarea> --run-id <id_corrida> [--intento N]

El código de negocio no se duplica: se reutilizan los módulos de la Unidad 3
(ingestion, transformations, persistence). Este archivo solo añade lo que la orquestación
necesita y que `main.py` resolvía con DataFrames en memoria:

  * zona de trabajo (staging) por corrida: <STAGING>/<run_id>/<resultado>/ en Parquet;
  * un archivo de estado por tarea (<STAGING>/<run_id>/estado/<tarea>.json) que alimenta la
    notificación y la auditoría;
  * la compuerta de calidad (T5) y la notificación (T12), que son nuevas.

Las 12 tareas y sus dependencias están en docs/orquestacion/dependencias.md.

Variables de entorno (detalle en airflow/README.md):
  DATAMARCA_STAGING_ROOT     zona de trabajo; por defecto data/_staging
  UMBRAL_RECHAZO_PCT         % de rechazo por fuente que detiene la corrida (defecto 0.1, criterio C6)
  DATAMARCA_SPARK_MASTER     master de Spark; por defecto local[2]
  SHUFFLE_PARTITIONS         particiones de shuffle; por defecto 8 (los datos son pequeños)
  DATAMARCA_LIMPIAR_STAGING  true = borra los resultados intermedios de una corrida exitosa
  DATAMARCA_RETENCION_DIAS   días que se conserva la carpeta de cualquier corrida; 0 o vacío = no se purga
  DATAMARCA_WEBHOOK_URL      opcional: URL de Teams Workflows a la que notificar
  DATAMARCA_FALLAR_EN        nombre de una tarea a la que se le fuerza un fallo (demostración, T1.3)
  DATAMARCA_FALLAR_INTENTOS  cuántos intentos fallan antes de dejar pasar la tarea (defecto: todos)
  DATAMARCA_DEMORAR_EN       <tarea>:<segundos>: demora una tarea para probar los timeouts (T1.3)

La retención de la zona de trabajo y el manejo de errores están en docs/orquestacion/manejo_errores_logging.md.
"""
import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = SRC_DIR.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from orquestacion.alertas import enviar_una_vez

STAGING_ROOT = Path(os.getenv("DATAMARCA_STAGING_ROOT", ROOT_DIR / "data" / "_staging"))
REPORTES_DIR = ROOT_DIR / "reports" / "orquestacion"
UMBRAL_RECHAZO_PCT = float(os.getenv("UMBRAL_RECHAZO_PCT", "0.1"))
FUENTES = ("transacciones", "logs_ecommerce")

log = logging.getLogger("datamarca")


class CompuertaCalidadError(RuntimeError):
    """El rechazo de una fuente supera el umbral: la corrida se detiene (T5)."""


# ---------------------------------------------------------------------------
# Utilidades de corrida: rutas, estado, Spark
# ---------------------------------------------------------------------------
def limpiar_run_id(run_id: str) -> str:
    """El run_id de Airflow trae ':' y '+', que no sirven en rutas; se reemplazan."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", run_id)


def dir_corrida(run_id: str) -> Path:
    return STAGING_ROOT / limpiar_run_id(run_id)


def ruta_stage(run_id: str, nombre: str) -> str:
    return str(dir_corrida(run_id) / nombre)


def escribir_estado(run_id: str, tarea: str, datos: dict) -> None:
    carpeta = dir_corrida(run_id) / "estado"
    carpeta.mkdir(parents=True, exist_ok=True)
    with open(carpeta / f"{tarea}.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2, default=str)


def leer_estados(run_id: str) -> dict:
    carpeta = dir_corrida(run_id) / "estado"
    if not carpeta.is_dir():
        return {}
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in carpeta.glob("*.json")}


def crear_spark(tarea: str):
    """Sesión de Spark con la misma configuración de main.py (zona horaria America/Bogota)."""
    from pyspark.sql import SparkSession
    from persistence.writer import configurar_builder

    constructor = (SparkSession.builder
                   .appName(f"Datamarca-{tarea}")
                   .master(os.getenv("DATAMARCA_SPARK_MASTER", "local[2]"))
                   .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                   .config("spark.sql.session.timeZone", "America/Bogota")
                   .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
                   .config("spark.ui.enabled", "false")
                   .config("spark.ui.showConsoleProgress", "false"))
    spark = configurar_builder(constructor).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Bogota")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def guardar_stage(df, run_id: str, nombre: str) -> int:
    """Deja el resultado de una tarea en la zona de trabajo y devuelve las filas escritas."""
    ruta = ruta_stage(run_id, nombre)
    df.write.mode("overwrite").parquet(ruta)
    filas = df.sparkSession.read.parquet(ruta).count()
    log.info("Resultado intermedio '%s': %s filas -> %s", nombre, filas, ruta)
    return filas


def leer_stage(spark, run_id: str, nombre: str):
    ruta = ruta_stage(run_id, nombre)
    if not Path(ruta).exists():
        raise FileNotFoundError(f"Falta el resultado intermedio '{nombre}' de la corrida: {ruta}")
    return spark.read.parquet(ruta)


# ---------------------------------------------------------------------------
# Etapa 1. Ingestión (decisión 1a de dependencias.md: verificar el origen y fallar rápido)
# ---------------------------------------------------------------------------
def ingestar_transacciones(run_id: str) -> dict:
    from config.rutas import CSV_TRANSACCIONES
    from config.schemas import TRANSACCIONES_SCHEMA_CORRUPT

    esperadas = [c.name for c in TRANSACCIONES_SCHEMA_CORRUPT.fields if c.name != "_corrupt_record"]
    ruta = Path(CSV_TRANSACCIONES)
    if not ruta.is_file() or ruta.stat().st_size == 0:
        raise FileNotFoundError(f"El origen de transacciones no existe o está vacío: {ruta}")
    with open(ruta, encoding="utf-8", newline="") as f:
        encabezado = next(csv.reader(f), [])
        filas = sum(1 for _ in f)
    faltantes = [c for c in esperadas if c not in encabezado]
    if faltantes:
        raise ValueError(f"El CSV de transacciones no tiene las columnas {faltantes}")
    return {"origen": str(ruta), "filas_origen": filas}


def ingestar_logs(run_id: str) -> dict:
    from config.rutas import JSON_LOGS

    ruta = Path(JSON_LOGS)
    if not ruta.is_file() or ruta.stat().st_size == 0:
        raise FileNotFoundError(f"El origen de logs no existe o está vacío: {ruta}")
    with open(ruta, encoding="utf-8") as f:
        datos = json.load(f)  # un JSON mal formado falla aquí, antes de gastar una sesión de Spark
    if not isinstance(datos, list) or not datos or not all(isinstance(r, dict) for r in datos):
        raise ValueError("El JSON de logs debe ser un arreglo no vacío de objetos")
    return {"origen": str(ruta), "filas_origen": len(datos)}


# ---------------------------------------------------------------------------
# Etapa 2. Validación
# ---------------------------------------------------------------------------
def _validar_fuente(run_id: str, fuente: str) -> dict:
    from config.rutas import DIR_CALIDAD
    from ingestion.reader import leer_transacciones_csv_crudo, leer_logs_ecommerce_json_crudo
    from ingestion.validacion import (validar_transacciones, validar_logs, imprimir_metricas,
                                      guardar_resultado_validacion)

    leer, validar = ((leer_transacciones_csv_crudo, validar_transacciones) if fuente == "transacciones"
                     else (leer_logs_ecommerce_json_crudo, validar_logs))
    spark = crear_spark(f"validar_{fuente}")
    try:
        resultado = validar(leer(spark))
        imprimir_metricas(resultado)
        # Si no se puede guardar la cuarentena la tarea falla: la compuerta y la investigación la necesitan.
        guardar_resultado_validacion(resultado, DIR_CALIDAD)
        guardar_stage(resultado.validos, run_id, f"validos_{fuente}")
        metricas = resultado.metricas
        carpeta = dir_corrida(run_id) / "metricas"
        carpeta.mkdir(parents=True, exist_ok=True)
        (carpeta / f"{fuente}.json").write_text(json.dumps(metricas, ensure_ascii=False, indent=2, default=str),
                                                encoding="utf-8")
        return {k: metricas[k] for k in ("leidos", "validos", "cuarentena", "pct_rechazo")}
    finally:
        spark.stop()


def validar_transacciones(run_id: str) -> dict:
    return _validar_fuente(run_id, "transacciones")


def validar_logs(run_id: str) -> dict:
    return _validar_fuente(run_id, "logs_ecommerce")


def compuerta_calidad(run_id: str) -> dict:
    """T5: detiene la corrida si el rechazo de una fuente supera el umbral (criterio C6)."""
    resumen, excesos = {}, []
    for fuente in FUENTES:
        ruta = dir_corrida(run_id) / "metricas" / f"{fuente}.json"
        if not ruta.is_file():
            raise FileNotFoundError(f"No hay métricas de calidad de {fuente}: {ruta}")
        pct = float(json.loads(ruta.read_text(encoding="utf-8"))["pct_rechazo"])
        resumen[fuente] = pct
        log.info("Compuerta de calidad: %s rechazo=%.4f%% (umbral %.4f%%)", fuente, pct, UMBRAL_RECHAZO_PCT)
        if pct > UMBRAL_RECHAZO_PCT:
            excesos.append(f"{fuente}: {pct}% > {UMBRAL_RECHAZO_PCT}%")
    if excesos:
        raise CompuertaCalidadError("Rechazo por encima del umbral; no se transforma ni se persiste. "
                                    + "; ".join(excesos))
    return {"pct_rechazo": resumen, "umbral_pct": UMBRAL_RECHAZO_PCT}


# ---------------------------------------------------------------------------
# Etapa 3. Transformación
# ---------------------------------------------------------------------------
def _limpiar(run_id: str, fuente: str) -> dict:
    from transformations.cleaning import ejecutar_limpieza_transacciones, ejecutar_limpieza_logs

    limpiar = ejecutar_limpieza_transacciones if fuente == "transacciones" else ejecutar_limpieza_logs
    spark = crear_spark(f"limpiar_{fuente}")
    try:
        validos = leer_stage(spark, run_id, f"validos_{fuente}")
        return {"filas": guardar_stage(limpiar(spark, validos), run_id, f"limpio_{fuente}")}
    finally:
        spark.stop()


def limpiar_transacciones(run_id: str) -> dict:
    return _limpiar(run_id, "transacciones")


def limpiar_logs(run_id: str) -> dict:
    return _limpiar(run_id, "logs_ecommerce")


def unir(run_id: str) -> dict:
    from transformations.joins import realizar_join_transacciones_logs

    spark = crear_spark("unir")
    try:
        tx = leer_stage(spark, run_id, "limpio_transacciones")
        logs = leer_stage(spark, run_id, "limpio_logs_ecommerce")
        return {"filas": guardar_stage(realizar_join_transacciones_logs(tx, logs), run_id, "unida")}
    finally:
        spark.stop()


def agregar(run_id: str) -> dict:
    from transformations.joins import generar_agregaciones_negocio
    from transformations.agregaciones import ejecutar_agregaciones

    spark = crear_spark("agregar")
    try:
        unida = leer_stage(spark, run_id, "unida")
        gold = {"kpis_negocio_categoria": generar_agregaciones_negocio(unida),
                **ejecutar_agregaciones(spark, unida)}
        filas = {nombre: guardar_stage(df, run_id, f"gold_{nombre}") for nombre, df in gold.items()}
        (dir_corrida(run_id) / "gold_tablas.json").write_text(json.dumps(list(gold)), encoding="utf-8")
        return {"tablas": filas}
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Etapa 4. Persistencia (idempotente: sobrescritura dinámica de particiones)
# ---------------------------------------------------------------------------
def _raiz_capa(raiz: str, capa: str) -> str:
    from config.rutas import EN_NUBE, _abfss
    return _abfss(capa) if EN_NUBE else raiz


def persistir_silver(run_id: str) -> dict:
    from persistence.writer import preparar_persistencia, persistir_silver as _persistir

    spark = crear_spark("persistir_silver")
    try:
        formato, raiz = preparar_persistencia(spark)
        tx = leer_stage(spark, run_id, "limpio_transacciones")
        logs = leer_stage(spark, run_id, "limpio_logs_ecommerce")
        tablas = _persistir(tx, logs, formato, _raiz_capa(raiz, "silver"))
        return {"tablas": {t.tabla: t.filas for t in tablas}, "formato": formato}
    finally:
        spark.stop()


def persistir_gold(run_id: str) -> dict:
    from persistence.writer import preparar_persistencia, persistir_gold as _persistir

    spark = crear_spark("persistir_gold")
    try:
        formato, raiz = preparar_persistencia(spark)
        nombres = json.loads((dir_corrida(run_id) / "gold_tablas.json").read_text(encoding="utf-8"))
        gold = {n: leer_stage(spark, run_id, f"gold_{n}") for n in nombres}
        tablas = _persistir(gold, formato, _raiz_capa(raiz, "gold"))
        return {"tablas": {t.tabla: t.filas for t in tablas}, "formato": formato}
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Etapa 5. Notificación (se ejecuta siempre; nunca cambia el estado del procesamiento)
# ---------------------------------------------------------------------------
TAREAS_ORDEN = ["ingestar_transacciones", "ingestar_logs", "validar_transacciones", "validar_logs",
                "compuerta_calidad", "limpiar_transacciones", "limpiar_logs", "unir", "agregar",
                "persistir_silver", "persistir_gold"]


def clasificar(estados: dict) -> dict:
    """Estado de cada tarea. Una tarea sin archivo de estado nunca llegó a ejecutarse
    (Airflow la marcó upstream_failed por una dependencia que falló)."""
    clasificacion = {}
    for tarea in TAREAS_ORDEN:
        e = estados.get(tarea)
        if e is None:
            clasificacion[tarea] = "no_ejecutada"
        elif e["estado"] == "en_ejecucion":
            clasificacion[tarea] = "interrumpida"  # sin cierre: timeout o proceso terminado
        else:
            clasificacion[tarea] = e["estado"]
    return clasificacion


def _ultima_actividad(carpeta: Path) -> float:
    """Momento de la última escritura de estado de una corrida (o de la carpeta, si no tiene)."""
    return max((p.stat().st_mtime for p in (carpeta / "estado").glob("*.json")),
               default=carpeta.stat().st_mtime)


def aplicar_retencion(run_id: str, exito: bool) -> dict:
    """Retención de la zona de trabajo (docs/orquestacion/manejo_errores_logging.md, sección 5).

    1. Corrida exitosa con DATAMARCA_LIMPIAR_STAGING=true: se borran sus intermedios y se conservan
       estado/ y metricas/. Los recibos nuevos viven fuera del staging. Las corridas fallidas se conservan para investigarlas o reanudarlas.
    2. DATAMARCA_RETENCION_DIAS=N (N > 0): se borran las carpetas de otras corridas cuya última
       actividad tenga más de N días. Solo se tocan carpetas con estado/ (las crea este script),
       para no borrar nada ajeno si DATAMARCA_STAGING_ROOT apunta a otro lugar.
    """
    actual = dir_corrida(run_id)
    limpiados, purgadas = [], []
    if exito and os.getenv("DATAMARCA_LIMPIAR_STAGING", "false").lower() == "true" and actual.is_dir():
        for sub in actual.iterdir():
            if sub.is_dir() and sub.name not in ("estado", "metricas"):
                shutil.rmtree(sub, ignore_errors=True)
                limpiados.append(sub.name)
    dias = float(os.getenv("DATAMARCA_RETENCION_DIAS") or 0)
    if dias > 0 and STAGING_ROOT.is_dir():
        limite = time.time() - dias * 86400
        for carpeta in STAGING_ROOT.iterdir():
            if (carpeta.is_dir() and carpeta.name != actual.name and (carpeta / "estado").is_dir()
                    and _ultima_actividad(carpeta) < limite):
                shutil.rmtree(carpeta, ignore_errors=True)
                purgadas.append(carpeta.name)
    if limpiados:
        log.info("Retención: intermedios borrados de la corrida %s: %s", actual.name, ", ".join(sorted(limpiados)))
    if purgadas:
        log.info("Retención: corridas con más de %s días borradas: %s", dias, ", ".join(sorted(purgadas)))
    return {"limpiados": sorted(limpiados), "purgadas": sorted(purgadas)}


def notificar(run_id: str) -> dict:
    estados = leer_estados(run_id)
    clasif = clasificar(estados)
    fallidas = [t for t, c in clasif.items() if c in ("fallida", "interrumpida")]
    canceladas = [t for t, c in clasif.items() if c == "no_ejecutada"]
    exito = not fallidas and not canceladas
    rid = limpiar_run_id(run_id)

    lineas = [f"# Corrida {rid}: {'EXITOSA' if exito else 'CON FALLO'}", ""]
    demo = {v: os.getenv(v) for v in ("DATAMARCA_FALLAR_EN", "DATAMARCA_DEMORAR_EN") if os.getenv(v)}
    if demo:
        lineas.append("- Modo demostración: " + ", ".join(f"{k}={v}" for k, v in demo.items()))
    if exito:
        m = {f: json.loads((dir_corrida(run_id) / "metricas" / f"{f}.json").read_text(encoding="utf-8"))
             for f in FUENTES if (dir_corrida(run_id) / "metricas" / f"{f}.json").is_file()}
        for f, met in m.items():
            lineas.append(f"- {f}: leídos={met['leidos']} válidos={met['validos']} rechazados={met['cuarentena']}")
        lineas.append(f"- Tiempo acumulado de tareas: {sum(e.get('segundos', 0) for e in estados.values()):.1f} s")
    else:
        for t in fallidas:
            lineas.append(f"- FALLÓ `{t}` (intento {estados[t].get('intento')}): "
                          f"{estados[t].get('error', 'sin cierre (timeout o proceso terminado)')}")
        if canceladas:
            lineas.append(f"- No se ejecutaron por la falla: {', '.join(canceladas)}")
        hechas = [t for t, c in clasif.items() if c == "exitosa"]
        if hechas:
            lineas.append(f"- Completadas antes/durante el fallo: {', '.join(hechas)}")
        if "persistir_silver" in hechas and "persistir_gold" not in hechas or \
           "persistir_gold" in hechas and "persistir_silver" not in hechas:
            lineas.append("- ATENCIÓN: solo una capa (silver o gold) quedó escrita; reintente la tarea fallida.")
    lineas += ["", "| Tarea | Estado | Segundos | Intento |", "|---|---|---|---|"]
    for t in TAREAS_ORDEN:
        e = estados.get(t, {})
        lineas.append(f"| {t} | {clasif[t]} | {e.get('segundos', '-')} | {e.get('intento', '-')} |")
    texto = "\n".join(lineas)
    (log.info if exito else log.error)("NOTIFICACIÓN\n%s", texto)

    # Cada paso en su propio try: un fallo al notificar no cambia el estado del procesamiento,
    # y un fallo de uno de los pasos no impide los demás (B4 de manejo_errores_logging.md).
    try:
        REPORTES_DIR.mkdir(parents=True, exist_ok=True)
        (REPORTES_DIR / f"resumen_{rid}.md").write_text(texto, encoding="utf-8")
        (REPORTES_DIR / f"resumen_{rid}.json").write_text(
            json.dumps({"run_id": rid, "exito": exito, "tareas": clasif}, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log.error("No se pudo guardar el resumen de la corrida: %s", e)
    try:
        enviar_una_vez(texto, run_id, "notificar", carpeta_legacy=dir_corrida(run_id))
    except Exception as e:
        log.error("No se pudo completar la notificación: tipo=%s", type(e).__name__)
    try:
        aplicar_retencion(run_id, exito)
    except Exception as e:
        log.error("No se pudo aplicar la retención de la zona de trabajo: %s", e)
    return {"exito": exito}


TAREAS = {
    "ingestar_transacciones": ingestar_transacciones, "ingestar_logs": ingestar_logs,
    "validar_transacciones": validar_transacciones, "validar_logs": validar_logs,
    "compuerta_calidad": compuerta_calidad,
    "limpiar_transacciones": limpiar_transacciones, "limpiar_logs": limpiar_logs,
    "unir": unir, "agregar": agregar,
    "persistir_silver": persistir_silver, "persistir_gold": persistir_gold,
    "notificar": notificar,
}


# ---------------------------------------------------------------------------
# Ejecución de una tarea con estado y trazas
# ---------------------------------------------------------------------------
def _fallo_forzado(tarea: str, intento: int) -> None:
    """Gancho para la evidencia de la tarea T1.3: DATAMARCA_FALLAR_EN=<tarea>."""
    if os.getenv("DATAMARCA_FALLAR_EN") == tarea and intento <= int(os.getenv("DATAMARCA_FALLAR_INTENTOS", "999")):
        raise RuntimeError(f"FALLO FORZADO en '{tarea}' (intento {intento}) por DATAMARCA_FALLAR_EN")


def _demora_forzada(tarea: str) -> None:
    """Gancho para probar los timeouts (T1.3): DATAMARCA_DEMORAR_EN=<tarea>:<segundos>.
    Si la demora supera el execution_timeout de la tarea, Airflow la detiene (SIGTERM) y el
    estado queda 'en_ejecucion', que la notificación reporta como 'interrumpida'."""
    nombre, _, segundos = os.getenv("DATAMARCA_DEMORAR_EN", "").partition(":")
    if nombre == tarea and segundos:
        log.warning("DEMORA FORZADA en '%s': %s s por DATAMARCA_DEMORAR_EN", tarea, segundos)
        time.sleep(float(segundos))


def ejecutar(tarea: str, run_id: str, intento: int) -> int:
    inicio = time.time()
    base = {"tarea": tarea, "run_id": limpiar_run_id(run_id), "intento": intento,
            "inicio": datetime.now().isoformat(timespec="seconds")}
    log.info("INICIO %s (corrida=%s, intento=%s)", tarea, base["run_id"], intento)
    demo = {v: os.getenv(v) for v in ("DATAMARCA_FALLAR_EN", "DATAMARCA_DEMORAR_EN") if os.getenv(v)}
    if demo:  # que una variable de demostración olvidada no pase por un fallo real (B9)
        log.warning("Modo demostración activo: %s", ", ".join(f"{k}={v}" for k, v in demo.items()))
    if tarea != "notificar":
        escribir_estado(run_id, tarea, {**base, "estado": "en_ejecucion"})
    try:
        _demora_forzada(tarea)
        _fallo_forzado(tarea, intento)
        detalle = TAREAS[tarea](run_id) or {}
    except Exception as e:
        seg = round(time.time() - inicio, 2)
        log.error("FALLO %s tras %.1f s: %s\n%s", tarea, seg, e, traceback.format_exc())
        escribir_estado(run_id, tarea, {**base, "estado": "fallida", "segundos": seg,
                                        "error": f"{type(e).__name__}: {e}"})
        return 1  # código de salida != 0: Airflow marca la tarea como fallida y aplica los reintentos
    seg = round(time.time() - inicio, 2)
    if tarea != "notificar":
        escribir_estado(run_id, tarea, {**base, "estado": "exitosa", "segundos": seg, "detalle": detalle})
    log.info("FIN %s OK en %.1f s %s", tarea, seg, json.dumps(detalle, ensure_ascii=False, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta una tarea del pipeline DATAMARCA")
    parser.add_argument("tarea", choices=sorted(TAREAS))
    parser.add_argument("--run-id", default=datetime.now().strftime("manual-%Y%m%dT%H%M%S"))
    parser.add_argument("--intento", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    return ejecutar(args.tarea, args.run_id, args.intento)


if __name__ == "__main__":
    sys.exit(main())
