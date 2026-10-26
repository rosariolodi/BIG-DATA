"""
Módulo de Limpieza, Estandarización y Tratamiento de Nulos (Inciso 3.1)
DATAMARCA S.A.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, lower, upper, when, lit
from ingestion.reader import leer_transacciones_csv, leer_logs_ecommerce_json

def ejecutar_limpieza_transacciones(spark: SparkSession) -> DataFrame:
    """
    1. Llama los datos crudos de transacciones desde la capa raw.
    2. Aplica la limpieza inicial, estandarización de texto y tratamiento de nulos.
    """
    # Llamado de datos desde raw (usando la función de ingestión)
    df_raw = leer_transacciones_csv(spark)
    
    # Limpieza y estandarización optimizada (Catalyst friendly)
    df_limpio = df_raw \
        .dropna(subset=["transaction_id", "store_id", "amount_cop"]) \
        .withColumn("store_id", upper(trim(col("store_id")))) \
        .withColumn("product_category", lower(trim(col("product_category")))) \
        .withColumn("payment_method", lower(trim(col("payment_method")))) \
        .withColumn(
            "payment_method",
            when(col("payment_method").isNull() | (col("payment_method") == ""), lit("desconocido"))
            .otherwise(col("payment_method"))
        )
    
    return df_limpio

def ejecutar_limpieza_logs(spark: SparkSession) -> DataFrame:
    """
    1. Llama los datos crudos de logs de e-commerce desde la capa raw.
    2. Aplica la limpieza inicial, estandarización de texto y tratamiento de nulos.
    """
    # Llamado de datos desde raw (usando la función de ingestión)
    df_raw = leer_logs_ecommerce_json(spark)
    
    # Limpieza y estandarización optimizada
    df_limpio = df_raw \
        .dropna(subset=["event_id", "user_id", "timestamp"]) \
        .withColumn("device", lower(trim(col("device")))) \
        .withColumn("product_category", lower(trim(col("product_category")))) \
        .withColumn("event_type", lower(trim(col("event_type")))) \
        .withColumn(
            "session_duration_sec",
            when(col("session_duration_sec").isNull(), lit(0))
            .otherwise(col("session_duration_sec"))
        )
    
    return df_limpio