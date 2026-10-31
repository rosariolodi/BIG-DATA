"""
Orquestador Principal del Pipeline Batch
DATAMARCA S.A.
"""

import os
import time
import sys
import subprocess
import urllib.request
from pathlib import Path

# Benchmark 5.1: ejecucion independiente y sin alterar el pipeline normal.
if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "--benchmark":
    raiz_proyecto = Path(__file__).resolve().parent.parent
    benchmark = raiz_proyecto / "src" / "performance" / "benchmark_5_1.py"
    if not benchmark.is_file():
        raise SystemExit(f"No se encontro el benchmark: {benchmark}")
    resultado = subprocess.run(
        [sys.executable, str(benchmark), *sys.argv[2:]],
        cwd=raiz_proyecto,
        check=False,
    )
    raise SystemExit(resultado.returncode)

# --- AUTOCORRECCIÓN DE WINUTILS PARA WINDOWS ---
hadoop_dir = Path(os.path.expanduser("~")) / ".hadoop"
bin_dir = hadoop_dir / "bin"
winutils_path = bin_dir / "winutils.exe"

if os.name == "nt" and not winutils_path.exists():  # solo en Windows (en Databricks/Linux no hace falta)
    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        url = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.0.0/bin/winutils.exe"
        urllib.request.urlretrieve(url, winutils_path)
    except Exception:
        pass

if os.name == "nt":
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
# ---------------------------------------------

from pyspark.sql import SparkSession
from ingestion.reader import ingestar_transacciones, ingestar_logs_ecommerce
from transformations.cleaning import ejecutar_limpieza_transacciones, ejecutar_limpieza_logs
from transformations.joins import realizar_join_transacciones_logs, generar_agregaciones_negocio
from transformations.agregaciones import ejecutar_agregaciones
from utils.tiempos import RegistroTiempos, materializar
from config.rutas import DIR_TIEMPOS, DIR_TIEMPOS_NUBE, EN_NUBE, _abfss
from persistence.writer import configurar_builder, preparar_persistencia, persistir_silver, persistir_gold

# Los logs de tiempos (Inciso 5.2) se guardan en DIR_TIEMPOS (ver config/rutas.py)


def _raices_persistencia(raiz_local: str) -> dict:
    """
    Raíz de cada capa para la persistencia particionada (Inciso 4.3).
    Local: data/processed (o DATAMARCA_STORAGE_ROOT). Azure: el contenedor de cada capa en ADLS Gen2
    (ver config/rutas.py); las tablas quedan en <contenedor>/<capa>/<tabla>, separadas de las
    tablas Delta sin particionar de corridas anteriores.
    """
    if EN_NUBE:
        return {"silver": _abfss("silver"), "gold": _abfss("gold")}
    return {"silver": raiz_local, "gold": raiz_local}


def ejecutar_pipeline():
    registro = RegistroTiempos()
    datos_corrida = {}

    # 1. Inicializar Spark Session
    constructor = SparkSession.builder \
        .appName("Datamarca-Pipeline-Joins") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.session.timeZone", "America/Bogota")
    # Variable de entorno opcional para las pruebas de rendimiento (5.3): si no se define,
    # Spark usa su valor por defecto (200 particiones de shuffle).
    if os.environ.get("SHUFFLE_PARTITIONS"):
        constructor = constructor.config("spark.sql.shuffle.partitions", os.environ["SHUFFLE_PARTITIONS"])
    # Delta y el conector de ADLS se agregan según la configuración de la persistencia (Inciso 4.3)
    spark = configurar_builder(constructor).getOrCreate()
    # En Databricks la sesión ya existe y el .config() del builder no se aplica: se fija en la sesión.
    # La partición por día (docs/particionamiento.md) exige America/Bogota.
    spark.conf.set("spark.sql.session.timeZone", "America/Bogota")

    try:
        formato, raiz = preparar_persistencia(spark)
        raices = _raices_persistencia(raiz)

        print("--- INICIO DE LIMPIEZA Y JOINS DISTRIBUIDOS ---")

        # 2. Ingestión: lectura con esquema estricto y validación de calidad (Incisos 2.1 y 2.2)
        with registro.medir("ingestion", "transacciones_csv") as m:
            ingesta_tx = ingestar_transacciones(spark)
            m.filas = materializar(ingesta_tx.validos)
        with registro.medir("ingestion", "logs_json") as m:
            ingesta_logs = ingestar_logs_ecommerce(spark)
            m.filas = materializar(ingesta_logs.validos)
        datos_corrida = {"transacciones_leidas": ingesta_tx.metricas["leidos"],
                         "logs_leidos": ingesta_logs.metricas["leidos"]}

        # 3. Transformación: limpieza, joins y agregaciones.
        # Cada subetapa termina con materializar() para que Spark ejecute el trabajo dentro de ella.
        with registro.medir("transformacion", "limpieza_transacciones") as m:
            df_clean_tx = ejecutar_limpieza_transacciones(spark, ingesta_tx.validos)
            m.filas = materializar(df_clean_tx)
        with registro.medir("transformacion", "limpieza_logs") as m:
            df_clean_logs = ejecutar_limpieza_logs(spark, ingesta_logs.validos)
            m.filas = materializar(df_clean_logs)

        # Desarrollo de Joins Distribuidos (Inciso 4.1)
        with registro.medir("transformacion", "join") as m:
            df_joined = realizar_join_transacciones_logs(df_clean_tx, df_clean_logs)
            m.filas = materializar(df_joined)

        # Agregaciones de Negocio (Incisos 4.1 y 4.2)
        with registro.medir("transformacion", "agregaciones") as m:
            df_resultado_negocio = generar_agregaciones_negocio(df_joined)
            resultados_agregaciones = ejecutar_agregaciones(spark, df_joined)
            m.filas = sum(materializar(df) for df in [df_resultado_negocio, *resultados_agregaciones.values()])

        # Mostrar las primeras filas del resultado analítico
        print("--- RESULTADOS DE LAS AGREGACIONES POR CATEGORÍA ---")
        df_resultado_negocio.show(10, truncate=False)

        # 4. Persistencia en Parquet o Delta particionado (Inciso 4.3, ver docs/persistencia.md).
        # silver: transacciones y logs limpios por `fecha`; gold: agregaciones por `mes` o `fecha_proceso`.
        # En Windows sin hadoop.dll el módulo escribe el mismo layout con pyarrow.
        print(f"--- PERSISTENCIA PARTICIONADA ({formato}) ---")
        with registro.medir("persistencia", "silver") as m:
            tablas = persistir_silver(df_clean_tx, df_clean_logs, formato, raices["silver"])
            m.filas = sum(t.filas for t in tablas)
        with registro.medir("persistencia", "gold") as m:
            gold = {"kpis_negocio_categoria": df_resultado_negocio, **resultados_agregaciones}
            tablas = persistir_gold(gold, formato, raices["gold"])
            m.filas = sum(t.filas for t in tablas)

        print(f"--- PIPELINE COMPLETADO EXITOSAMENTE (Tiempo total: {registro.total_s:.2f} s) ---")
    finally:
        # El log de tiempos se guarda aunque alguna etapa falle (queda con estado "error")
        registro.finalizar()
        registro.imprimir()
        registro.guardar(DIR_TIEMPOS, spark, datos_corrida)
        if EN_NUBE:
            # El disco del driver se pierde al apagar el cluster: la evidencia queda en ADLS Gen2
            try:
                registro.publicar(DIR_TIEMPOS_NUBE, spark, datos_corrida)
            except Exception as e:  # publicar los tiempos no debe tumbar un pipeline ya terminado
                print(f"[Tiempos] No se pudieron publicar en {DIR_TIEMPOS_NUBE}: {e}")
        spark.stop()

if __name__ == "__main__":
    ejecutar_pipeline()