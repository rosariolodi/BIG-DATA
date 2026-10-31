
"""
Pruebas independientes de optimización Catalyst.
Proyecto DATAMARCA S.A. - Apartado 5.3.

No modifica ni reemplaza el pipeline principal.
"""

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ingestion.reader import (
    ingestar_transacciones,
    ingestar_logs_ecommerce,
)

from transformations.cleaning import (
    ejecutar_limpieza_transacciones,
    ejecutar_limpieza_logs,
)

from transformations.joins import realizar_join_transacciones_logs

from optimization.catalyst_analysis import (
    analizar_plan_ejecucion,
    analizar_column_pruning,
    analizar_predicate_pushdown,
    comparar_funcion_nativa_y_udf,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
DIR_EVIDENCIAS = ROOT_DIR / "reports" / "optimizacion"


def guardar_plan(df, nombre):
    """Guarda el plan lógico y físico de una operación."""
    DIR_EVIDENCIAS.mkdir(parents=True, exist_ok=True)

    plan = df._jdf.queryExecution().toString()
    destino = DIR_EVIDENCIAS / f"{nombre}.txt"
    destino.write_text(plan, encoding="utf-8")

    print(f"Evidencia guardada: {destino}")


def ejecutar_analisis():
    """Ejecuta pruebas de optimización sin alterar el pipeline principal."""

    spark = (
        SparkSession.builder
        .appName("DATAMARCA-Catalyst-Analysis")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )

    try:
        print("\n=== 1. INGESTIÓN CON VALIDACIÓN DE CALIDAD ===")

        resultado_tx = ingestar_transacciones(spark)
        resultado_logs = ingestar_logs_ecommerce(spark)

        print("\n=== 2. LIMPIEZA ===")

        df_tx = ejecutar_limpieza_transacciones(
            spark, resultado_tx.validos
        )

        df_logs = ejecutar_limpieza_logs(
            spark, resultado_logs.validos
        )

        print("\n=== 3. COLUMN PRUNING ===")

        columnas_tx = [
            "transaction_id",
            "user_id",
            "product_category",
            "amount_cop",
        ]

        df_tx_reducido = analizar_column_pruning(
            df_tx, columnas_tx
        )

        guardar_plan(df_tx_reducido, "01_column_pruning")

        print("\n=== 4. PREDICATE PUSHDOWN ===")

        columna_filtro = "product_category"

        valor = (
            df_tx_reducido
            .filter(col(columna_filtro).isNotNull())
            .select(columna_filtro)
            .first()
        )

        if valor is not None:
            df_filtrado = analizar_predicate_pushdown(
                df_tx_reducido,
                columna_filtro,
                valor[0],
            )

            guardar_plan(df_filtrado, "02_predicate_pushdown")
        else:
            print("No hay valores para realizar la prueba del filtro.")

        print("\n=== 5. FUNCIONES NATIVAS FRENTE A UDF ===")

        resultados_udf = comparar_funcion_nativa_y_udf(
            df_tx_reducido, "user_id"
        )

        guardar_plan(
            resultados_udf["nativo"],
            "03_funcion_nativa",
        )

        guardar_plan(
            resultados_udf["udf_python"],
            "04_udf_python",
        )

        print("\n=== 6. PLAN DE EJECUCIÓN DEL JOIN ===")

        df_joined = realizar_join_transacciones_logs(
            df_tx, df_logs
        )

        analizar_plan_ejecucion(df_joined, "Join de transacciones y logs")
        guardar_plan(df_joined, "05_join")

        print("\n=== 7. FINALIZADO ===")
        print(f"Evidencias disponibles en: {DIR_EVIDENCIAS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    ejecutar_analisis()
