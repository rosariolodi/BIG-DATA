
"""
Optimizaciones de Apache Spark mediante Catalyst.
Proyecto DATAMARCA S.A. - Apartado 5.3.

Este módulo permite analizar:
- Planes de ejecución de Spark SQL y Catalyst.
- Column pruning.
- Predicate pushdown.
- Uso de funciones nativas frente a UDFs de Python.

No modifica el pipeline principal.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, trim, udf
from pyspark.sql.types import StringType


def analizar_plan_ejecucion(df: DataFrame, nombre: str) -> str:
    """
    Obtiene el plan de ejecución de un DataFrame.

    Permite inspeccionar las optimizaciones realizadas por Catalyst.
    """
    print(f"\n{'=' * 70}")
    print(f"ANÁLISIS DEL PLAN DE EJECUCIÓN: {nombre}")
    print(f"{'=' * 70}")

    df.explain(mode="formatted")

    return df._jdf.queryExecution().toString()


def analizar_column_pruning(df: DataFrame, columnas: list[str]) -> DataFrame:
    """
    Crea una proyección con las columnas necesarias para un análisis.

    La proyección permite evaluar la reducción de columnas procesadas.
    No modifica el DataFrame original.
    """
    columnas_inexistentes = set(columnas) - set(df.columns)

    if columnas_inexistentes:
        raise ValueError(
            f"Columnas inexistentes en el DataFrame: "
            f"{sorted(columnas_inexistentes)}"
        )

    df_optimizado = df.select(*columnas)

    analizar_plan_ejecucion(
        df_optimizado,
        "Column pruning: selección de columnas"
    )

    return df_optimizado


def analizar_predicate_pushdown(
    df: DataFrame,
    columna: str,
    valor
) -> DataFrame:
    """
    Construye un filtro usando expresiones nativas de Spark.

    El plan permite comprobar si Spark aplica el filtro en la lectura
    o en una etapa posterior. La aplicación efectiva depende del origen
    de datos y del formato utilizado.
    """
    if columna not in df.columns:
        raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

    df_filtrado = df.filter(col(columna) == valor)

    analizar_plan_ejecucion(
        df_filtrado,
        f"Predicate pushdown: {columna} = {valor}"
    )

    return df_filtrado


def comparar_funcion_nativa_y_udf(df: DataFrame, columna: str) -> dict:
    """
    Compara una transformación nativa de Spark con una UDF de Python.

    La comparación utiliza lower(trim(columna)) en ambas alternativas.
    La UDF se incluye únicamente como referencia experimental.
    """
    if columna not in df.columns:
        raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

    @udf(returnType=StringType())
    def limpiar_texto_python(valor):
        if valor is None:
            return None

        return valor.strip().lower()

    df_nativo = df.withColumn(
        "resultado_limpieza",
        lower(trim(col(columna)))
    )

    df_udf = df.withColumn(
        "resultado_limpieza",
        limpiar_texto_python(col(columna))
    )

    print("\n--- PLAN CON FUNCIONES NATIVAS ---")
    df_nativo.explain(mode="formatted")

    print("\n--- PLAN CON UDF DE PYTHON ---")
    df_udf.explain(mode="formatted")

    return {
        "nativo": df_nativo,
        "udf_python": df_udf,
    }


def guardar_plan(
    plan: str,
    nombre_archivo: str,
    directorio: str | Path = "reports/optimizacion"
) -> Path:
    """
    Guarda el plan lógico y físico en un archivo de texto.
    """
    destino = Path(directorio)
    destino.mkdir(parents=True, exist_ok=True)

    archivo = destino / nombre_archivo
    archivo.write_text(plan, encoding="utf-8")

    print(f"Plan guardado en: {archivo.resolve()}")

    return archivo


def ejecutar_pruebas_optimizacion(
    spark: SparkSession,
    df: DataFrame
) -> dict:
    """
    Ejecuta pruebas demostrativas sobre un DataFrame proporcionado.

    Debe recibir un DataFrame que ya exista en el pipeline.
    No lee archivos ni modifica el pipeline principal.
    """
    if not df.columns:
        raise ValueError("El DataFrame no contiene columnas.")

    print("\nINICIO DEL ANÁLISIS DE OPTIMIZACIÓN CATALYST")

    columnas = df.columns[: min(3, len(df.columns))]
    df_columnas = analizar_column_pruning(df, columnas)

    columna_filtro = "user_id" if "user_id" in df.columns else df.columns[0]

    valor_filtro = (
        df.select(col(columna_filtro))
        .where(col(columna_filtro).isNotNull())
        .first()
    )

    df_filtrado = None

    if valor_filtro is not None:
        df_filtrado = analizar_predicate_pushdown(
            df,
            columna_filtro,
            valor_filtro[0]
        )
    else:
        print(
            f"No se evaluó el filtro: '{columna_filtro}' "
            "no contiene valores no nulos."
        )

    resultado = {
        "column_pruning": df_columnas,
        "predicate_pushdown": df_filtrado,
    }

    if "user_id" in df.columns:
        resultado["comparacion_udf"] = comparar_funcion_nativa_y_udf(
            df,
            "user_id"
        )

    return resultado
