"""
Módulo de Ingestión y Validación de Calidad (Inciso 2.2)
DATAMARCA S.A.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Calculamos la ruta absoluta de la raíz del proyecto de forma dinámica:
# reader.py -> ingestion/ -> src/ -> [RAÍZ DEL PROYECTO]
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

DEFAULT_CSV_PATH = str(ROOT_DIR / "data" / "raw" / "DATAMARCA_transacciones_5000_real.csv")
DEFAULT_JSON_PATH = str(ROOT_DIR / "data" / "raw" / "ecommerce_logs_5000.json")


def leer_transacciones_csv(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH):
    """
    Ingesta el archivo CSV de transacciones y aplica reglas de validación
    de completitud e integridad (inciso 2.2) usando los nombres reales de las columnas.
    """
    df_transacciones = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ruta_csv)

    # Columnas reales: transaction_id, user_id, timestamp, store_id,
    # product_category, amount_cop, payment_method
    # user_id es la clave del join con los logs, por eso es obligatorio.
    df_valido = df_transacciones.filter(
        col("transaction_id").isNotNull() &
        col("user_id").isNotNull() &
        col("store_id").isNotNull() &
        col("amount_cop").isNotNull() &
        (col("amount_cop") > 0) &
        col("timestamp").isNotNull() &
        col("product_category").isNotNull() &
        col("payment_method").isNotNull()
    )

    return df_valido


def leer_logs_ecommerce_json(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH):
    """
    Ingesta los logs JSON semiestructurados de e-commerce y aplica validaciones
    de completitud de datos esenciales usando los nombres reales de las columnas.
    """
    # multiLine porque el JSON es una lista de objetos [...]
    df_logs = spark.read \
        .option("multiLine", "true") \
        .json(ruta_json)

    # df_logs.printSchema()  # descomentar para ver columnas y tipos detectados

    df_valido = df_logs.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("event_type").isNotNull() &
        col("device").isNotNull() &
        col("product_category").isNotNull() &
        col("session_duration_sec").isNotNull() &
        (col("session_duration_sec") > 0)
    )

    return df_valido