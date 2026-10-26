"""
Módulo de Joins Distribuidos y Agregaciones de Negocio (Inciso 4.1)
DATAMARCA S.A.

Diseño del cruce
----------------
La clave de negocio es `user_id`: une cada transacción con la actividad web
(logs) del cliente que la realizó.

Problema del join directo transacciones <-> logs por user_id:
un usuario con 8 transacciones y 6 eventos generaría 8 x 6 = 48 filas, lo que
infla las ventas y los conteos. Para evitarlo, los logs se resumen primero a
UNA fila por usuario (relación muchos-a-uno) y recién entonces se cruzan con
las transacciones. Así cada transacción conserva su fila y sus montos.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    countDistinct,
    first,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum,
    when,
)

COLUMNAS_TRANSACCIONES = {"transaction_id", "user_id", "product_category", "amount_cop"}
COLUMNAS_LOGS = {"event_id", "user_id", "timestamp", "event_type", "session_duration_sec"}


def _validar_columnas(df: DataFrame, requeridas: set, nombre: str) -> None:
    faltantes = requeridas - set(df.columns)
    if faltantes:
        raise ValueError(f"El DataFrame de {nombre} no tiene las columnas: {sorted(faltantes)}")


def _resumir_logs_por_usuario(df_logs: DataFrame) -> DataFrame:
    """
    Resume los logs a una fila por user_id (perfil de actividad web).
    Métricas: volumen de eventos, duración promedio de sesión, ventana de
    actividad, eventos por tipo (embudo) y dispositivos usados.
    """
    return (
        df_logs.dropDuplicates(["event_id"])
        .groupBy("user_id")
        .agg(
            count("event_id").alias("total_eventos_web"),
            spark_round(avg("session_duration_sec"), 2).alias("promedio_duracion_sesion_sec"),
            spark_min("timestamp").alias("primer_evento_web"),
            spark_max("timestamp").alias("ultimo_evento_web"),
            countDistinct("device").alias("dispositivos_distintos"),
            count(when(col("event_type") == "page_view", 1)).alias("eventos_page_view"),
            count(when(col("event_type") == "search", 1)).alias("eventos_search"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("eventos_add_to_cart"),
            count(when(col("event_type") == "checkout_start", 1)).alias("eventos_checkout_start"),
            count(when(col("event_type") == "purchase", 1)).alias("eventos_purchase"),
        )
    )


def realizar_join_transacciones_logs(df_transacciones: DataFrame, df_logs: DataFrame) -> DataFrame:
    """
    Cruza transacciones y logs usando 'user_id' como clave de negocio.

    1. Los logs se resumen a una fila por usuario (evita multiplicar filas).
    2. Join left: se conservan TODAS las transacciones; si el usuario no tiene
       actividad web queda con nulos y `tiene_actividad_web = False`.
    3. El resumen de logs es pequeño (una fila por usuario), por lo que se
       envía a todos los executors con broadcast() y se evita el shuffle de
       la tabla grande de transacciones.
    """
    _validar_columnas(df_transacciones, COLUMNAS_TRANSACCIONES, "transacciones")
    _validar_columnas(df_logs, COLUMNAS_LOGS | {"device"}, "logs")

    df_logs_usuario = _resumir_logs_por_usuario(df_logs)

    df_joined = df_transacciones.join(
        broadcast(df_logs_usuario),
        on="user_id",
        how="left",
    ).withColumn("tiene_actividad_web", col("total_eventos_web").isNotNull())

    return df_joined


def generar_agregaciones_negocio(df_joined: DataFrame) -> DataFrame:
    """
    Métricas de negocio por categoría de producto a partir del dataset cruzado.

    Se agrega en dos pasos para no contar dos veces el perfil web de un
    usuario que compra varias veces en la misma categoría:
      (a) una fila por (categoría, usuario) con sus ventas y su perfil web;
      (b) resumen por categoría.
    """
    df_por_usuario = df_joined.groupBy("product_category", "user_id").agg(
        sum("amount_cop").alias("ventas_usuario"),
        count("transaction_id").alias("transacciones_usuario"),
        first("total_eventos_web", ignorenulls=True).alias("eventos_web_usuario"),
        first("promedio_duracion_sesion_sec", ignorenulls=True).alias("duracion_sesion_usuario"),
        first("tiene_actividad_web", ignorenulls=True).alias("tiene_actividad_web"),
    )

    df_agg = df_por_usuario.groupBy("product_category").agg(
        sum("ventas_usuario").alias("total_ventas_cop"),
        sum("transacciones_usuario").alias("total_transacciones"),
        countDistinct("user_id").alias("usuarios_unicos"),
        spark_round(sum("ventas_usuario") / sum("transacciones_usuario"), 2).alias("ticket_promedio_cop"),
        spark_round(sum("ventas_usuario") / countDistinct("user_id"), 2).alias("venta_promedio_por_usuario_cop"),
        spark_round(avg("duracion_sesion_usuario"), 2).alias("promedio_duracion_sesion_sec"),
        sum("eventos_web_usuario").alias("total_eventos_web"),
        spark_round(avg("eventos_web_usuario"), 2).alias("promedio_eventos_web_por_usuario"),
        spark_round(
            100 * count(when(col("tiene_actividad_web"), 1)) / countDistinct("user_id"), 2
        ).alias("pct_usuarios_con_actividad_web"),
    )

    return df_agg.orderBy(col("total_ventas_cop").desc())