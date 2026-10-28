"""
Agregaciones de negocio con SparkSQL y la API de DataFrames (Inciso 4.2)
DATAMARCA S.A.

Entrada: el DataFrame cruzado de transacciones y actividad web que produce
`transformations.joins.realizar_join_transacciones_logs` (una fila por transacción).

Agregaciones:
  SparkSQL      -> kpis_por_categoria, ventas_por_dia, top_clientes
  DataFrame API -> ventas_por_tienda, ventas_por_metodo_pago

`kpis_por_categoria_sql` replica exactamente `joins.generar_agregaciones_negocio` (versión
DataFrame); los resultados de ambas deben coincidir.

Todo usa funciones nativas de Spark (sin UDFs de Python) para que Catalyst pueda optimizar el plan.
La fecha de las ventas se toma en la zona horaria de la sesión de Spark, que debe ser
`America/Bogota` (ver `spark.sql.session.timeZone` en main.py y docs/particionamiento.md).
"""
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

VISTA_VENTAS = "ventas_enriquecidas"
ZONA_HORARIA = "America/Bogota"

# Ciudad según el prefijo de store_id (supuesto tomado del EDA: BOG, MED, CAL, BAR)
CIUDADES = {"BOG": "Bogotá", "MED": "Medellín", "CAL": "Cali", "BAR": "Barranquilla"}


def _avisar_zona_horaria(spark: SparkSession) -> None:
    zona = spark.conf.get("spark.sql.session.timeZone")
    if zona != ZONA_HORARIA:
        print(f"[Agregaciones] AVISO: la zona horaria de la sesión es {zona}; para agrupar por día "
              f"se espera {ZONA_HORARIA} (configurar spark.sql.session.timeZone).")


def registrar_vista(df_joined: DataFrame) -> None:
    """Expone el DataFrame cruzado como vista temporal para las consultas SparkSQL."""
    df_joined.createOrReplaceTempView(VISTA_VENTAS)


# ---------------------------------------------------------------------------
# SparkSQL
# ---------------------------------------------------------------------------
def kpis_por_categoria_sql(spark: SparkSession) -> DataFrame:
    """
    Métricas por categoría de producto. Se agrega en dos pasos para no contar dos veces el
    perfil web de un usuario que compra varias veces en la misma categoría:
    (a) una fila por (categoría, usuario); (b) resumen por categoría.
    """
    return spark.sql(f"""
        WITH por_usuario AS (
            SELECT product_category,
                   user_id,
                   SUM(amount_cop)                                 AS ventas_usuario,
                   COUNT(transaction_id)                           AS transacciones_usuario,
                   FIRST(total_eventos_web, TRUE)                  AS eventos_web_usuario,
                   FIRST(promedio_duracion_sesion_sec, TRUE)       AS duracion_sesion_usuario,
                   FIRST(tiene_actividad_web, TRUE)                AS tiene_actividad_web
            FROM {VISTA_VENTAS}
            GROUP BY product_category, user_id
        )
        SELECT product_category,
               SUM(ventas_usuario)                                                     AS total_ventas_cop,
               SUM(transacciones_usuario)                                              AS total_transacciones,
               COUNT(DISTINCT user_id)                                                 AS usuarios_unicos,
               ROUND(SUM(ventas_usuario) / SUM(transacciones_usuario), 2)              AS ticket_promedio_cop,
               ROUND(SUM(ventas_usuario) / COUNT(DISTINCT user_id), 2)                 AS venta_promedio_por_usuario_cop,
               ROUND(AVG(duracion_sesion_usuario), 2)                                  AS promedio_duracion_sesion_sec,
               SUM(eventos_web_usuario)                                                AS total_eventos_web,
               ROUND(AVG(eventos_web_usuario), 2)                                      AS promedio_eventos_web_por_usuario,
               ROUND(100 * COUNT(CASE WHEN tiene_actividad_web THEN 1 END)
                         / COUNT(DISTINCT user_id), 2)                                 AS pct_usuarios_con_actividad_web
        FROM por_usuario
        GROUP BY product_category
        ORDER BY total_ventas_cop DESC
    """)


def ventas_por_dia_sql(spark: SparkSession) -> DataFrame:
    """Ventas y transacciones por día, con ventas acumuladas (función de ventana)."""
    _avisar_zona_horaria(spark)
    return spark.sql(f"""
        WITH diario AS (
            SELECT TO_DATE(`timestamp`)                    AS fecha,
                   COUNT(transaction_id)                   AS transacciones,
                   SUM(amount_cop)                         AS ventas_cop,
                   ROUND(AVG(amount_cop), 2)               AS ticket_promedio_cop,
                   COUNT(DISTINCT user_id)                 AS usuarios_unicos
            FROM {VISTA_VENTAS}
            GROUP BY TO_DATE(`timestamp`)
        )
        SELECT fecha, transacciones, ventas_cop, ticket_promedio_cop, usuarios_unicos,
               SUM(ventas_cop) OVER (ORDER BY fecha)       AS ventas_acumuladas_cop
        FROM diario
        ORDER BY fecha
    """)


def top_clientes_sql(spark: SparkSession, n: int = 10) -> DataFrame:
    """Los n clientes con más ventas, con su actividad web."""
    return spark.sql(f"""
        WITH por_cliente AS (
            SELECT user_id,
                   COUNT(transaction_id)                   AS transacciones,
                   SUM(amount_cop)                         AS ventas_cop,
                   ROUND(AVG(amount_cop), 2)               AS ticket_promedio_cop,
                   COUNT(DISTINCT store_id)                AS tiendas_distintas,
                   MAX(total_eventos_web)                  AS eventos_web,
                   MAX(promedio_duracion_sesion_sec)       AS promedio_duracion_sesion_sec
            FROM {VISTA_VENTAS}
            GROUP BY user_id
        ),
        con_ranking AS (
            SELECT *, RANK() OVER (ORDER BY ventas_cop DESC, user_id) AS ranking
            FROM por_cliente
        )
        SELECT ranking, user_id, transacciones, ventas_cop, ticket_promedio_cop,
               tiendas_distintas, eventos_web, promedio_duracion_sesion_sec
        FROM con_ranking
        WHERE ranking <= {int(n)}
        ORDER BY ranking
    """)


# ---------------------------------------------------------------------------
# API de DataFrames
# ---------------------------------------------------------------------------
def ventas_por_tienda(df_joined: DataFrame) -> DataFrame:
    """Ventas, ticket promedio y participación por tienda, con su ciudad."""
    ciudad = F.lit("Sin ciudad")
    for prefijo, nombre in CIUDADES.items():
        ciudad = F.when(F.substring("store_id", 1, 3) == prefijo, nombre).otherwise(ciudad)

    por_tienda = (df_joined
                  .groupBy("store_id")
                  .agg(F.count("transaction_id").alias("transacciones"),
                       F.sum("amount_cop").alias("ventas_cop"),
                       F.round(F.avg("amount_cop"), 2).alias("ticket_promedio_cop"),
                       F.countDistinct("user_id").alias("usuarios_unicos"))
                  .withColumn("ciudad", ciudad))

    total = Window.partitionBy()  # el resultado ya está agregado: pocas filas
    return (por_tienda
            .withColumn("pct_ventas", F.round(100 * F.col("ventas_cop") / F.sum("ventas_cop").over(total), 2))
            .select("store_id", "ciudad", "transacciones", "ventas_cop", "ticket_promedio_cop",
                    "usuarios_unicos", "pct_ventas")
            .orderBy(F.col("ventas_cop").desc()))


def ventas_por_metodo_pago(df_joined: DataFrame) -> DataFrame:
    """Transacciones, ventas y participación por método de pago."""
    por_metodo = (df_joined
                  .groupBy("payment_method")
                  .agg(F.count("transaction_id").alias("transacciones"),
                       F.sum("amount_cop").alias("ventas_cop"),
                       F.round(F.avg("amount_cop"), 2).alias("ticket_promedio_cop")))

    total = Window.partitionBy()
    return (por_metodo
            .withColumn("pct_transacciones", F.round(100 * F.col("transacciones") / F.sum("transacciones").over(total), 2))
            .withColumn("pct_ventas", F.round(100 * F.col("ventas_cop") / F.sum("ventas_cop").over(total), 2))
            .orderBy(F.col("ventas_cop").desc()))


# ---------------------------------------------------------------------------
# Orquestación y salida
# ---------------------------------------------------------------------------
def ejecutar_agregaciones(spark: SparkSession, df_joined: DataFrame) -> Dict[str, DataFrame]:
    """Calcula todas las agregaciones de negocio y las devuelve por nombre."""
    df_joined = df_joined.cache()  # se reutiliza en varias consultas
    registrar_vista(df_joined)
    return {
        "kpis_por_categoria": kpis_por_categoria_sql(spark),
        "ventas_por_dia": ventas_por_dia_sql(spark),
        "top_clientes": top_clientes_sql(spark),
        "ventas_por_tienda": ventas_por_tienda(df_joined),
        "ventas_por_metodo_pago": ventas_por_metodo_pago(df_joined),
    }


def guardar_resultados(resultados: Dict[str, DataFrame], directorio: str = "data/processed") -> None:
    """
    Guarda cada agregación en <directorio>/<nombre>/resultado.parquet.
    Se usa pandas, igual que src/main.py, para evitar el problema de winutils/hadoop.dll en Windows.
    """
    for nombre, df in resultados.items():
        carpeta = Path(directorio) / nombre
        carpeta.mkdir(parents=True, exist_ok=True)
        df.toPandas().to_parquet(carpeta / "resultado.parquet", index=False)
        print(f"[Agregaciones] {nombre}: {carpeta / 'resultado.parquet'}")
