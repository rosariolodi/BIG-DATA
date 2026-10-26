"""
Módulo de Limpieza, Estandarización y Tratamiento de Nulos (Inciso 3.1)
DATAMARCA S.A.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, lower, upper, when, lit, expr
from ingestion.reader import leer_transacciones_csv, leer_logs_ecommerce_json

# Se descarta todo registro con fecha posterior a octubre de 2026
# (es decir, desde el 1 de noviembre de 2026 en adelante).
FECHA_LIMITE_EXCLUSIVA = "2026-11-01 00:00:00"


def _filtrar_fechas_validas(df: DataFrame, columna: str = "timestamp") -> DataFrame:
    """
    Convierte la columna a timestamp (acepta 'yyyy-MM-dd HH:mm:ss' y formato
    ISO 'yyyy-MM-ddTHH:mm:ssZ') y elimina:
      - fechas que no se pueden interpretar (try_cast las deja nulas sin romper
        el job, incluso con ANSI mode activo),
      - fechas futuras (>= FECHA_LIMITE_EXCLUSIVA).
    """
    return (
        df.withColumn(columna, expr(f"try_cast(`{columna}` AS timestamp)"))
        .dropna(subset=[columna])
        .filter(col(columna) < lit(FECHA_LIMITE_EXCLUSIVA).cast("timestamp"))
    )


def ejecutar_limpieza_transacciones(spark: SparkSession) -> DataFrame:
    """
    1. Llama los datos crudos de transacciones desde la capa raw.
    2. Aplica la limpieza inicial, estandarización de texto, tratamiento de nulos
       y descarte de fechas futuras.
    """
    # Llamado de datos desde raw (usando la función de ingestión)
    df_raw = leer_transacciones_csv(spark)

    # Limpieza y estandarización optimizada (Catalyst friendly)
    df_limpio = (
        df_raw
        .dropna(subset=["transaction_id", "user_id", "store_id", "amount_cop"])
        .withColumn("user_id", lower(trim(col("user_id"))))
        .withColumn("store_id", upper(trim(col("store_id"))))
        .withColumn("product_category", lower(trim(col("product_category"))))
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        .withColumn(
            "payment_method",
            when(col("payment_method").isNull() | (col("payment_method") == ""), lit("desconocido"))
            .otherwise(col("payment_method"))
        )
    )

    return _filtrar_fechas_validas(df_limpio)


def ejecutar_limpieza_logs(spark: SparkSession) -> DataFrame:
    """
    1. Llama los datos crudos de logs de e-commerce desde la capa raw.
    2. Aplica la limpieza inicial, estandarización de texto, tratamiento de nulos
       y descarte de fechas futuras.
    """
    # Llamado de datos desde raw (usando la función de ingestión)
    df_raw = leer_logs_ecommerce_json(spark)

    # Limpieza y estandarización optimizada
    df_limpio = (
        df_raw
        .dropna(subset=["event_id", "user_id", "timestamp"])
        .withColumn("user_id", lower(trim(col("user_id"))))
        .withColumn("device", lower(trim(col("device"))))
        .withColumn("product_category", lower(trim(col("product_category"))))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn(
            "session_duration_sec",
            when(col("session_duration_sec").isNull(), lit(0))
            .otherwise(col("session_duration_sec"))
        )
    )

    return _filtrar_fechas_validas(df_limpio)