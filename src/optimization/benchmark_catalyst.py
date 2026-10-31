"""
Benchmark de funciones nativas de Spark frente a UDF de Python.
Proyecto DATAMARCA S.A. - Apartado 5.3.

Este script es independiente del pipeline principal.
No modifica los archivos originales del proyecto.
"""

import csv
import statistics
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    udf,
    count,
    sum as spark_sum,
    length,
    coalesce,
    lit,
)
from pyspark.sql.types import StringType

from ingestion.reader import ingestar_transacciones
from transformations.cleaning import ejecutar_limpieza_transacciones


ROOT_DIR = Path(__file__).resolve().parents[2]
DIR_EVIDENCIAS = ROOT_DIR / "reports" / "optimizacion"
REPETICIONES = 5


@udf(returnType=StringType())
def limpiar_texto_python(valor):
    """Replica la operación trim + lower mediante Python."""
    if valor is None:
        return None

    return valor.strip().lower()


def medir_ejecucion(df, columna, alternativa):
    """
    Mide una ejecución completa de la transformación.

    La agregación obliga a Spark a evaluar los valores transformados,
    evitando medir únicamente la construcción del plan.
    """
    if alternativa == "nativa":
        transformado = lower(trim(col(columna)))

    elif alternativa == "udf_python":
        transformado = limpiar_texto_python(col(columna))

    else:
        raise ValueError(f"Alternativa desconocida: {alternativa}")

    resultado = df.select(
        transformado.alias("texto_limpio")
    ).agg(
        count("texto_limpio").alias("valores_no_nulos"),
        spark_sum(
            length(coalesce(col("texto_limpio"), lit("")))
        ).alias("suma_longitudes"),
    )

    inicio = time.perf_counter()
    metricas = resultado.collect()[0]
    duracion = time.perf_counter() - inicio

    return duracion, metricas


def guardar_resultados(resultados):
    """Guarda los tiempos y el resumen del benchmark."""
    DIR_EVIDENCIAS.mkdir(parents=True, exist_ok=True)

    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")

    archivo_csv = DIR_EVIDENCIAS / f"benchmark_catalyst_{fecha}.csv"
    archivo_txt = DIR_EVIDENCIAS / f"resumen_benchmark_{fecha}.txt"

    with archivo_csv.open("w", newline="", encoding="utf-8") as archivo:
        campos = [
            "repeticion",
            "alternativa",
            "tiempo_segundos",
            "valores_no_nulos",
            "suma_longitudes",
        ]

        escritor = csv.DictWriter(archivo, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(resultados)

    resumen = {}

    for alternativa in ("nativa", "udf_python"):
        tiempos = [
            fila["tiempo_segundos"]
            for fila in resultados
            if fila["alternativa"] == alternativa
        ]

        resumen[alternativa] = {
            "promedio": statistics.mean(tiempos),
            "mediana": statistics.median(tiempos),
            "minimo": min(tiempos),
            "maximo": max(tiempos),
        }

    promedio_nativo = resumen["nativa"]["promedio"]
    promedio_udf = resumen["udf_python"]["promedio"]

    if promedio_nativo > 0:
        diferencia_pct = (
            (promedio_udf - promedio_nativo) / promedio_nativo
        ) * 100
    else:
        diferencia_pct = 0.0

    with archivo_txt.open("w", encoding="utf-8") as archivo:
        archivo.write("BENCHMARK: FUNCIONES NATIVAS VS UDF DE PYTHON\n")
        archivo.write(f"Fecha: {datetime.now().isoformat()}\n")
        archivo.write(f"Repeticiones por alternativa: {REPETICIONES}\n\n")

        for alternativa, datos in resumen.items():
            archivo.write(f"{alternativa.upper()}\n")
            archivo.write(f"Promedio: {datos['promedio']:.6f} s\n")
            archivo.write(f"Mediana: {datos['mediana']:.6f} s\n")
            archivo.write(f"Mínimo: {datos['minimo']:.6f} s\n")
            archivo.write(f"Máximo: {datos['maximo']:.6f} s\n\n")

        archivo.write(
            "Diferencia porcentual (UDF frente a nativa): "
            f"{diferencia_pct:.2f}%\n"
        )

        archivo.write(
            "Interpretación: un valor positivo indica que la UDF tardó "
            "más en promedio en esta prueba; un valor negativo indica "
            "que tardó menos. El resultado es específico de esta ejecución.\n"
        )

    print("\n=== RESULTADOS DEL BENCHMARK ===")

    for alternativa, datos in resumen.items():
        print(f"\n{alternativa.upper()}")
        print(f"Promedio: {datos['promedio']:.6f} segundos")
        print(f"Mediana:  {datos['mediana']:.6f} segundos")

    print(f"\nDiferencia porcentual: {diferencia_pct:.2f}%")
    print(f"CSV guardado en: {archivo_csv}")
    print(f"Resumen guardado en: {archivo_txt}")


def ejecutar_benchmark():
    spark = (
        SparkSession.builder
        .appName("DATAMARCA-Benchmark-Catalyst")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )

    try:
        print("=== INGESTIÓN Y LIMPIEZA ===")

        resultado_tx = ingestar_transacciones(spark)

        df = ejecutar_limpieza_transacciones(
            spark,
            resultado_tx.validos,
        )

        if "user_id" not in df.columns:
            raise ValueError(
                "El DataFrame limpio no contiene la columna user_id."
            )

        # Materializamos la entrada para que el coste de lectura y limpieza
        # no se atribuya de forma diferente a cada alternativa.
        df.cache()
        cantidad = df.count()

        if cantidad == 0:
            raise ValueError("No hay registros para realizar el benchmark.")

        print(f"Registros de entrada: {cantidad}")

        print("\n=== CALENTAMIENTO ===")

        medir_ejecucion(df, "user_id", "nativa")
        medir_ejecucion(df, "user_id", "udf_python")

        resultados = []

        print("\n=== MEDICIONES ===")

        for repeticion in range(1, REPETICIONES + 1):
            # Alternamos el orden para reducir el efecto de medir
            # siempre la misma alternativa primero.
            alternativas = (
                ["nativa", "udf_python"]
                if repeticion % 2 == 1
                else ["udf_python", "nativa"]
            )

            for alternativa in alternativas:
                segundos, metricas = medir_ejecucion(
                    df,
                    "user_id",
                    alternativa,
                )

                fila = {
                    "repeticion": repeticion,
                    "alternativa": alternativa,
                    "tiempo_segundos": round(segundos, 6),
                    "valores_no_nulos": metricas["valores_no_nulos"],
                    "suma_longitudes": metricas["suma_longitudes"],
                }

                resultados.append(fila)

                print(
                    f"Repetición {repeticion} | "
                    f"{alternativa} | {segundos:.6f} s"
                )

        # Comprobación básica de que ambas alternativas producen
        # las mismas métricas agregadas en esta prueba.
        por_alternativa = {}

        for alternativa in ("nativa", "udf_python"):
            filas = [
                fila for fila in resultados
                if fila["alternativa"] == alternativa
            ]

            por_alternativa[alternativa] = {
                (
                    fila["valores_no_nulos"],
                    fila["suma_longitudes"],
                )
                for fila in filas
            }

        if (
            por_alternativa["nativa"]
            != por_alternativa["udf_python"]
        ):
            print(
                "\nADVERTENCIA: las métricas agregadas no coinciden. "
                "No interpretes los tiempos hasta investigar la diferencia."
            )
        else:
            print(
                "\nLas métricas agregadas coinciden entre las dos "
                "alternativas en las repeticiones realizadas."
            )

        guardar_resultados(resultados)

        df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    ejecutar_benchmark()