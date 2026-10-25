"""
Módulo de Limpieza, Estandarización y Tratamiento de Nulos (Inciso 3.1)
DATAMARCA S.A.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, upper, when, lit

def limpiar_y_estandarizar_transacciones(df: DataFrame) -> DataFrame:
    """
    Realiza la limpieza inicial, estandarización de texto y tratamiento de nulos
    para el DataFrame de transacciones.
    """
    df_limpio = df \
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

def limpiar_y_estandarizar_logs(df: DataFrame) -> DataFrame:
    """
    Realiza la limpieza inicial, estandarización de texto y tratamiento de nulos
    para el DataFrame de logs de e-commerce.
    """
    df_limpio = df \
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
