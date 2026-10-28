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

if not winutils_path.exists():
    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        url = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.0.0/bin/winutils.exe"
        urllib.request.urlretrieve(url, winutils_path)
    except Exception:
        pass

os.environ["HADOOP_HOME"] = str(hadoop_dir)
# ---------------------------------------------

from pyspark.sql import SparkSession
from transformations.cleaning import ejecutar_limpieza_transacciones, ejecutar_limpieza_logs
from transformations.joins import realizar_join_transacciones_logs, generar_agregaciones_negocio
from transformations.agregaciones import ejecutar_agregaciones
from persistence.writer import configurar_builder, preparar_persistencia, persistir_silver, persistir_gold

def ejecutar_pipeline():
    inicio_total = time.time()

    # 1. Inicializar Spark Session (Delta y ADLS se agregan según la configuración, Inciso 4.3)
    builder = SparkSession.builder \
        .appName("Datamarca-Pipeline-Joins") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.session.timeZone", "America/Bogota")
    spark = configurar_builder(builder).getOrCreate()
    formato, raiz = preparar_persistencia(spark)

    print("--- INICIO DE LIMPIEZA Y JOINS DISTRIBUIDOS ---")

    # 2. Ejecutar Limpieza (se cachea: se usa en la persistencia silver y en el join)
    df_clean_tx = ejecutar_limpieza_transacciones(spark).cache()
    df_clean_logs = ejecutar_limpieza_logs(spark).cache()

    # Persistencia silver particionada por fecha (Inciso 4.3)
    print("--- PERSISTENCIA SILVER ---")
    t0 = time.time()
    persistir_silver(df_clean_tx, df_clean_logs, formato, raiz)
    print(f"[Tiempo] Persistencia silver completada en: {time.time() - t0:.2f} segundos")

    # 3. Desarrollo de Joins Distribuidos (Inciso 4.1)
    t0 = time.time()
    df_joined = realizar_join_transacciones_logs(df_clean_tx, df_clean_logs)
    print(f"[Tiempo] Join distribuido completado en: {time.time() - t0:.2f} segundos")
    
    # 4. Agregaciones de Negocio
    df_resultado_negocio = generar_agregaciones_negocio(df_joined)
    
    # Mostrar las primeras filas del resultado analítico
    print("--- RESULTADOS DE LAS AGREGACIONES POR CATEGORÍA ---")
    df_resultado_negocio.show(10, truncate=False)

    # 5. Agregaciones de negocio adicionales con SparkSQL y DataFrames (Inciso 4.2)
    print("--- AGREGACIONES DE NEGOCIO (SPARKSQL Y DATAFRAMES) ---")
    t0 = time.time()
    resultados_agregaciones = ejecutar_agregaciones(spark, df_joined)
    print(f"[Tiempo] Agregaciones de negocio completadas en: {time.time() - t0:.2f} segundos")

    # 6. Persistencia gold en Delta o Parquet particionado (Inciso 4.3)
    # Reemplaza la escritura con pandas a un único archivo, que no aplicaba particionamiento.
    print("--- PERSISTENCIA GOLD ---")
    t0 = time.time()
    persistir_gold({"kpis_negocio_categoria": df_resultado_negocio, **resultados_agregaciones}, formato, raiz)
    print(f"[Tiempo] Persistencia gold completada en: {time.time() - t0:.2f} segundos")

    tiempo_total = time.time() - inicio_total
    print(f"--- PIPELINE COMPLETADO EXITOSAMENTE (Tiempo total: {tiempo_total:.2f} s) ---")
    
    spark.stop()

if __name__ == "__main__":
    ejecutar_pipeline()