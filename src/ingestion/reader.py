from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def leer_transacciones_csv(spark: SparkSession, ruta_csv: str):
    """
    Lee el archivo CSV de transacciones y aplica reglas básicas de validación 
    de completitud e integridad (inciso 2.2).
    """
    df_transacciones = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ruta_csv)
    
    # Reglas de validación de calidad de datos (Completitud e Integridad):
    # - Asegurar que los campos clave no sean nulos
    # - Validar que el monto sea un valor positivo (> 0)
    df_valido = df_transacciones.filter(
        col("id_transaccion").isNotNull() & 
        col("cliente_id").isNotNull() & 
        col("monto").isNotNull() & 
        (col("monto") > 0)
    )
    
    return df_valido

def leer_logs_ecommerce_json(spark: SparkSession, ruta_json: str):
    """
    Lee los logs JSON de e-commerce y aplica validaciones básicas de completitud.
    """
    df_logs = spark.read.json(ruta_json)
    
    # Reglas de validación de calidad para los logs semiestructurados:
    df_valido = df_logs.filter(
        col("evento_id").isNotNull() & 
        col("usuario_id").isNotNull() & 
        col("timestamp").isNotNull()
    )
    
    return df_valido
