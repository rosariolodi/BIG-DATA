"""
Evidencia de predicate pushdown, column pruning y partition pruning sobre Parquet particionado
(Inciso 5.3, DATAMARCA S.A.).

Lee la tabla silver de transacciones que escribe src/persistence/writer.py (Parquet particionado por
`fecha`) y, para cada consulta, muestra qué le "empuja" Catalyst a la lectura: columnas leídas
(ReadSchema), filtros enviados al lector de Parquet (PushedFilters), carpetas descartadas
(PartitionFilters) y cuántos archivos y bytes se leyeron realmente.

El análisis anterior (reports/optimizacion/02_predicate_pushdown.txt) partía de CSV, que no permite
estas optimizaciones; por eso aquí se parte del Parquet particionado.

Cómo leer los resultados:
  * Column pruning: se ve en ReadSchema (columnas que el lector decodifica). Los "bytes" son el tamaño de
    los archivos abiertos, no de las columnas decodificadas, por eso no bajan con el column pruning.
  * Partition pruning: se ve en PartitionFilters y en menos archivos abiertos.
  * Predicate pushdown: se ve en PushedFilters. Solo reduce lectura si las estadísticas (mínimo y máximo)
    de los grupos de filas permiten descartarlos; con un grupo de filas por archivo y rangos de monto
    solapados, como en estos datos, no descarta nada.

Uso, desde la raíz del repositorio:
    python src/optimization/pushdown_parquet.py [ruta_a_silver/transacciones]
"""
import csv
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

ROOT_DIR = Path(__file__).resolve().parents[2]
RUTA_SILVER = ROOT_DIR / "data" / "processed" / "silver" / "transacciones"
DIR_EVIDENCIAS = ROOT_DIR / "reports" / "optimizacion"

UMBRAL_MONTO = 100000
FECHA_UNICA = "2026-09-20"
FECHA_DESDE, FECHA_HASTA = "2026-09-20", "2026-09-22"


def leer_dataset(spark: SparkSession, ruta: Path) -> DataFrame:
    """
    Lee un dataset Parquet particionado. En Windows sin hadoop.dll Spark no puede listar carpetas
    (UnsatisfiedLinkError en NativeIO); en ese caso se le pasan los archivos explícitamente y
    `basePath` para que igual descubra las particiones (columna `fecha`).
    """
    try:
        return spark.read.parquet(str(ruta))
    except Exception:
        archivos = sorted(str(p) for p in Path(ruta).rglob("*.parquet"))
        if not archivos:
            raise
        return spark.read.option("basePath", str(ruta)).parquet(*archivos)


def _campo(linea: str, nombre: str) -> str:
    """Extrae un campo del nodo FileScan del plan físico (ReadSchema, PushedFilters, ...)."""
    if nombre == "ReadSchema":
        m = re.search(r"ReadSchema: (struct<.*>)\s*$", linea)
    else:
        m = re.search(nombre + r": (\[.*?\])(?:, [A-Z]|$)", linea)
    return m.group(1) if m else ""


def _columnas_leidas(read_schema: str) -> int:
    """Cuenta las columnas del nivel superior de un ReadSchema (ignora las anidadas)."""
    interior, nivel, n = read_schema[len("struct<"):-1], 0, 1 if read_schema != "struct<>" else 0
    for ch in interior:
        nivel += ch in "<("
        nivel -= ch in ">)"
        n += ch == "," and nivel == 0
    return n


def medir_lectura(df: DataFrame, ejecutar: bool = True) -> dict:
    """
    Devuelve lo que muestra el plan físico del DataFrame sobre su lectura de Parquet y, si `ejecutar`,
    las métricas reales del escaneo (archivos, bytes y filas leídos). Requiere AQE desactivado para que
    las métricas queden en el nodo de lectura.
    """
    qe = df._jdf.queryExecution()
    plan = qe.executedPlan()
    texto = plan.toString()
    linea = next((x for x in texto.splitlines() if "FileScan" in x), "")
    info = {"plan": texto, "linea_scan": linea.strip(),
            "ReadSchema": _campo(linea, "ReadSchema"), "PartitionFilters": _campo(linea, "PartitionFilters"),
            "PushedFilters": _campo(linea, "PushedFilters"), "DataFilters": _campo(linea, "DataFilters"),
            "archivos": None, "bytes": None, "filas": None, "particiones": None}
    info["columnas"] = _columnas_leidas(info["ReadSchema"]) if info["ReadSchema"] else None
    if ejecutar:
        qe.toRdd().count()
        metricas = plan.collectLeaves().head().metrics()
        for clave, destino in (("numFiles", "archivos"), ("filesSize", "bytes"),
                               ("numOutputRows", "filas"), ("numPartitions", "particiones")):
            if metricas.contains(clave):
                info[destino] = int(metricas.apply(clave).value())
    return info


def construir_casos(base: DataFrame) -> list:
    """(id, título, DataFrame, ¿se ejecuta?). El caso con UDF solo se explica: no hace falta ejecutarlo."""
    monto_alto_udf = udf(lambda x: x is not None and x > UMBRAL_MONTO, BooleanType())
    return [
        ("A", "Referencia: todas las columnas, sin filtro", base, True),
        ("B", "Column pruning: solo transaction_id y amount_cop", base.select("transaction_id", "amount_cop"), True),
        ("C", f"Predicate pushdown: amount_cop > {UMBRAL_MONTO}",
         base.filter(F.col("amount_cop") > UMBRAL_MONTO).select("transaction_id", "amount_cop"), True),
        ("D", f"Partition pruning: fecha = {FECHA_UNICA}",
         base.filter(F.col("fecha") == FECHA_UNICA).select("transaction_id", "amount_cop"), True),
        ("E", f"Combinado: fecha entre {FECHA_DESDE} y {FECHA_HASTA} y amount_cop > {UMBRAL_MONTO}",
         base.filter((F.col("fecha").between(FECHA_DESDE, FECHA_HASTA)) & (F.col("amount_cop") > UMBRAL_MONTO))
             .select("transaction_id", "amount_cop"), True),
        ("F", "Contraste: el mismo filtro de C con una UDF de Python",
         base.filter(monto_alto_udf(F.col("amount_cop"))).select("transaction_id", "amount_cop"), False),
    ]


def ejecutar(ruta: Path = RUTA_SILVER, destino: Path = DIR_EVIDENCIAS) -> list:
    spark = (SparkSession.builder.appName("DATAMARCA-Pushdown-Parquet").master("local[*]")
             .config("spark.sql.session.timeZone", "America/Bogota")
             .config("spark.sql.adaptive.enabled", "false")      # métricas de lectura accesibles en el plan
             .config("spark.sql.maxMetadataStringLength", "10000")   # Spark trunca el plan a 100 caracteres
             .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        base = leer_dataset(spark, Path(ruta))
        resultados = []
        for id_, titulo, df, ejecutar_caso in construir_casos(base):
            info = medir_lectura(df, ejecutar=ejecutar_caso)
            info.update({"id": id_, "titulo": titulo})
            resultados.append(info)
        guardar(resultados, Path(destino))
        return resultados
    finally:
        spark.stop()


def guardar(resultados: list, destino: Path) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    total = next(r for r in resultados if r["id"] == "A")
    with open(destino / "06_pushdown_parquet_planes.txt", "w", encoding="utf-8") as f:
        for r in resultados:
            f.write(f"=== {r['id']}. {r['titulo']}\n{r['linea_scan']}\n\n")
    with open(destino / "06_pushdown_parquet_resumen.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["caso", "descripcion", "columnas_leidas", "filtros_empujados", "filtros_de_particion",
                    "archivos_leidos", "archivos_totales", "bytes_de_archivos_abiertos", "bytes_totales",
                    "pct_bytes_de_archivos_abiertos", "filas_leidas"])
        for r in resultados:
            pct = None if r["bytes"] is None else round(100 * r["bytes"] / total["bytes"], 1)
            w.writerow([r["id"], r["titulo"], r["columnas"], r["PushedFilters"], r["PartitionFilters"],
                        r["archivos"], total["archivos"], r["bytes"], total["bytes"], pct, r["filas"]])
    print(f"{'caso':<5}{'cols':>5} {'archivos':>9} {'% bytes':>8} {'filas':>6}  {'filtros empujados al lector':<56}")
    for r in resultados:
        arch = "-" if r["archivos"] is None else f"{r['archivos']}/{total['archivos']}"
        pct = "-" if r["bytes"] is None else f"{100 * r['bytes'] / total['bytes']:.1f}"
        print(f"{r['id']:<5}{str(r['columnas']):>5} {arch:>9} {pct:>8} {str(r['filas'] or '-'):>6}  "
              f"{r['PushedFilters'][:56] or '(ninguno)':<56}")
    print(f"\nEvidencia guardada en {destino}")


if __name__ == "__main__":
    ejecutar(Path(sys.argv[1]) if len(sys.argv) > 1 else RUTA_SILVER)
