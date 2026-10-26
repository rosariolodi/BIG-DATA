"""
Módulo de Ingestión y Validación de Calidad (Incisos 2.1 y 2.2)
DATAMARCA S.A.

2.1: conectores de lectura con esquema estricto (src/config/schemas.py), sin inferSchema.
2.2: validaciones básicas de completitud e integridad sobre lo leído.
"""

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from config.schemas import (
    TRANSACCIONES_SCHEMA_CORRUPT,
    TRANSACCIONES_READ_OPTIONS,
    LOGS_ECOMMERCE_SCHEMA_CORRUPT,
    LOGS_ECOMMERCE_READ_OPTIONS,
)

# Calculamos la ruta absoluta de la raíz del proyecto de forma dinámica:
# reader.py -> ingestion/ -> src/ -> [RAÍZ DEL PROYECTO]
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

DEFAULT_CSV_PATH = str(ROOT_DIR / "data" / "raw" / "DATAMARCA_transacciones_5000_real.csv")
DEFAULT_JSON_PATH = str(ROOT_DIR / "data" / "raw" / "ecommerce_logs_5000.json")

# ---------------------------------------------------------------------------
# 2.1 Conectores de lectura (sin filtrar)
# ---------------------------------------------------------------------------
def leer_transacciones_csv_crudo(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH) -> DataFrame:
    """
    Lee el CSV de transacciones con esquema estricto (sin inferSchema).
    Los registros mal formados quedan en la columna `_corrupt_record` (modo PERMISSIVE).
    """
    return (spark.read
            .schema(TRANSACCIONES_SCHEMA_CORRUPT)
            .options(**TRANSACCIONES_READ_OPTIONS)
            .csv(ruta_csv))


def leer_logs_ecommerce_json_crudo(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH) -> DataFrame:
    """
    Lee los logs JSON de e-commerce con esquema estricto.
    El archivo es un array JSON, por eso se usa multiLine (ver LOGS_ECOMMERCE_READ_OPTIONS).
    Los registros mal formados quedan en la columna `_corrupt_record` (modo PERMISSIVE).
    """
    return (spark.read
            .schema(LOGS_ECOMMERCE_SCHEMA_CORRUPT)
            .options(**LOGS_ECOMMERCE_READ_OPTIONS)
            .json(ruta_json))


# ---------------------------------------------------------------------------
# 2.2 Validación (se mantienen los nombres que usa transformations/cleaning.py)
# ---------------------------------------------------------------------------
def leer_transacciones_csv(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH) -> DataFrame:
    """
    Ingesta el CSV de transacciones y aplica reglas de validación
    de completitud e integridad (inciso 2.2) usando los nombres reales de las columnas.
    """
    df_transacciones = leer_transacciones_csv_crudo(spark, ruta_csv)

    # Reglas de validación de calidad ajustadas a los nombres reales del CSV:
    # Columnas reales: transaction_id, timestamp, store_id, product_category, amount_cop, payment_method
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

    # La columna de registros corruptos no se propaga a las capas siguientes
    return df_valido.drop("_corrupt_record")


def leer_logs_ecommerce_json(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH) -> DataFrame:
    """
    Ingesta los logs JSON semiestructurados de e-commerce y aplica validaciones
    de completitud de datos esenciales usando los nombres reales de las columnas.
    """
    df_logs = leer_logs_ecommerce_json_crudo(spark, ruta_json)

    # Reglas de validación de calidad ajustadas a los nombres reales del JSON:
    df_valido = df_logs.filter(
        col("_corrupt_record").isNull() &
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("event_type").isNotNull() &
        col("device").isNotNull() &
        col("product_category").isNotNull() &
        col("session_duration_sec").isNotNull() &
        (col("session_duration_sec") > 0)
    )

    return df_valido.drop("_corrupt_record")
