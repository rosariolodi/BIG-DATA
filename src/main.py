"""
Orquestador Principal del Pipeline Batch
DATAMARCA S.A.
"""

import os
import time
import urllib.request
from pathlib import Path

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
from transformations.agregaciones import ejecutar_agregaciones, guardar_resultados
from utils.tiempos import RegistroTiempos, materializar
from config.rutas import DIR_KPIS, DIR_TIEMPOS, EN_NUBE, escribir_delta

# Los logs de tiempos (Inciso 5.2) se guardan en DIR_TIEMPOS (ver config/rutas.py)

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
    spark = constructor.getOrCreate()

    try:
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

        # 4. Persistencia de resultados procesados a Parquet usando Pandas en Windows
        print("--- GUARDANDO RESULTADOS EN PARQUET ---")

        with registro.medir("persistencia", "kpis_negocio_categoria") as m:
            if EN_NUBE:
                # Azure: Delta Lake en la capa gold de ADLS Gen2
                m.filas = escribir_delta(df_resultado_negocio, DIR_KPIS)
            else:
                # Local (Windows): Pandas + Parquet (evita el error de winutils/hadoop.dll)
                Path("data/processed/kpis_negocio_categoria").mkdir(parents=True, exist_ok=True)
                df_pandas = df_resultado_negocio.toPandas()
                df_pandas.to_parquet("data/processed/kpis_negocio_categoria/resultado.parquet", index=False)
                m.filas = len(df_pandas)

        # Agregaciones de negocio adicionales con SparkSQL y DataFrames (Inciso 4.2)
        with registro.medir("persistencia", "agregaciones_adicionales"):
            guardar_resultados(resultados_agregaciones, "data/processed")

        print(f"--- PIPELINE COMPLETADO EXITOSAMENTE (Tiempo total: {registro.total_s:.2f} s) ---")
    finally:
        # El log de tiempos se guarda aunque alguna etapa falle (queda con estado "error")
        registro.finalizar()
        registro.imprimir()
        registro.guardar(DIR_TIEMPOS, spark, datos_corrida)
        spark.stop()

if __name__ == "__main__":
    ejecutar_pipeline()
