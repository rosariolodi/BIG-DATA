"""
Módulo de Joins Distribuidos y Agregaciones de Negocio (Inciso 4.1)
DATAMARCA S.A.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg, count, broadcast

def realizar_join_transacciones_logs(df_transacciones: DataFrame, df_logs: DataFrame) -> DataFrame:
    """
    Realiza un join distribuido entre el DataFrame limpio de transacciones y el de logs 
    utilizando 'product_category' como clave de negocio.
    
    Nota técnica: Si una de las tablas fuera muy pequeña (ej. catálogo de tiendas o categorías),
    podríamos usar broadcast(tabla_pequena) para optimizar el join. Aquí hacemos un join distribuido estándar.
    """
    # Renombramos el timestamp en logs para evitar conflictos de columnas duplicadas tras el join
    df_logs_renamed = df_logs.withColumnRenamed("timestamp", "log_timestamp")
    
    # Join distribuido basado en la categoría de producto
    df_joined = df_transacciones.join(
        df_logs_renamed,
        on="product_category",
        how="inner"
    )
    
    return df_joined

def generar_agregaciones_negocio(df_joined: DataFrame) -> DataFrame:
    """
    Genera métricas de negocio agregadas a partir del dataset cruzado 
    (ej. ventas totales, promedio de sesión y conteo de eventos por categoría).
    """
    df_agg = df_joined.groupBy("product_category").agg(
        sum("amount_cop").alias("total_ventas_cop"),
        avg("session_duration_sec").alias("promedio_duracion_sesion_sec"),
        count("transaction_id").alias("total_transacciones"),
        count("event_id").alias("total_eventos_web")
    )
    
    return df_agg