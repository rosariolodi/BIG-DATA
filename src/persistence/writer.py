"""
Persistencia en Delta Lake o Parquet particionado (Inciso 4.3)
DATAMARCA S.A.

Aplica la estrategia de docs/particionamiento.md (tarea 1.4) a las salidas del pipeline:

  Capa    Tabla                              Partición
  silver  transacciones, logs_ecommerce      fecha          (día en America/Bogota)
  gold    ventas_por_dia                     mes            (AAAA-MM)
  gold    agregaciones de todo el periodo    fecha_proceso  (día de la ejecución)

Las agregaciones gold que resumen todo el periodo no tienen fecha propia: cada ejecución se
guarda como una foto en la partición de su fecha_proceso, así se conserva el histórico.

La escritura usa sobrescritura dinámica por partición: una ejecución solo reemplaza las
particiones que trae el DataFrame y no borra días ya procesados.

Variables de entorno (detalle en docs/persistencia.md):
  DATAMARCA_STORAGE_ROOT   raíz del almacenamiento; por defecto data/processed.
                           En Azure: abfss://<contenedor>@<cuenta>.dfs.core.windows.net
  DATAMARCA_FORMATO        auto (defecto) | delta | parquet
  AZURE_STORAGE_ACCOUNT,
  AZURE_STORAGE_KEY        credenciales de ADLS Gen2 fuera de Databricks
"""
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# persistence/ -> src/ -> [RAÍZ DEL PROYECTO], igual que ingestion/reader.py
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_STORAGE_ROOT = str(ROOT_DIR / "data" / "processed")
ZONA_HORARIA = "America/Bogota"

DELTA = "delta"
PARQUET = "parquet"
FORMATOS = ("auto", DELTA, PARQUET)

# Conector de ADLS Gen2 para Spark fuera de Databricks. Su versión debe coincidir con la de
# Hadoop que trae pyspark (3.3.4 en pyspark 3.4 y 3.5).
PAQUETE_HADOOP_AZURE = os.getenv("DATAMARCA_HADOOP_AZURE_PKG", "org.apache.hadoop:hadoop-azure:3.3.4")

# Columnas de filtro frecuente que se ordenan con ZORDER en Delta (docs/particionamiento.md, sección 2)
ZORDER_TRANSACCIONES = ["store_id", "product_category"]


@dataclass
class ResultadoPersistencia:
    tabla: str
    ruta: str
    formato: str
    particion: str
    filas: int
    particiones: int
    desde: str
    hasta: str
    escritor: str  # spark o pyarrow
    segundos: float


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
def raiz_almacenamiento() -> str:
    return os.getenv("DATAMARCA_STORAGE_ROOT", DEFAULT_STORAGE_ROOT).rstrip("/\\")


def _formato_pedido() -> str:
    formato = os.getenv("DATAMARCA_FORMATO", "auto").strip().lower()
    if formato not in FORMATOS:
        raise ValueError(f"DATAMARCA_FORMATO={formato!r} no es válido; use uno de {', '.join(FORMATOS)}.")
    return formato


def _en_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _es_local(ruta: str) -> bool:
    return "://" not in ruta or ruta.startswith("file://")


def configurar_builder(builder: SparkSession.Builder) -> SparkSession.Builder:
    """
    Agrega a la sesión, antes de crearla, lo que pide la persistencia. No aplica en Databricks,
    donde Delta y ADLS ya vienen configurados.
      * Delta Lake, solo si DATAMARCA_FORMATO=delta (descarga los jars de Maven la primera vez).
      * El conector hadoop-azure si el destino es abfss://.
    """
    if _en_databricks():
        return builder
    paquetes = [PAQUETE_HADOOP_AZURE] if raiz_almacenamiento().startswith("abfss://") else []
    if _formato_pedido() == DELTA:
        from delta import configure_spark_with_delta_pip  # paquete delta-spark
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        return configure_spark_with_delta_pip(builder, extra_packages=paquetes)
    if paquetes:
        builder = builder.config("spark.jars.packages", ",".join(paquetes))
    return builder


def preparar_persistencia(spark: SparkSession) -> Tuple[str, str]:
    """
    Valida la configuración y devuelve (formato, raiz). Falla antes de procesar si los datos
    fueran a quedar mal particionados o en un formato distinto al pedido.
    """
    zona = spark.conf.get("spark.sql.session.timeZone")
    if zona != ZONA_HORARIA:
        raise ValueError(f"spark.sql.session.timeZone es {zona}; la partición por día exige {ZONA_HORARIA} "
                         "(ver docs/particionamiento.md, sección 3).")

    pedido = _formato_pedido()
    delta_disponible = (_en_databricks()
                        or "DeltaSparkSessionExtension" in spark.conf.get("spark.sql.extensions", ""))
    if pedido == DELTA and not delta_disponible:
        raise RuntimeError("DATAMARCA_FORMATO=delta, pero la sesión de Spark no tiene Delta Lake: "
                           "créela con configurar_builder() e instale delta-spark.")
    formato = DELTA if delta_disponible and pedido != PARQUET else PARQUET

    raiz = raiz_almacenamiento()
    cuenta, clave = os.getenv("AZURE_STORAGE_ACCOUNT"), os.getenv("AZURE_STORAGE_KEY")
    if raiz.startswith("abfss://") and cuenta and clave:
        spark.conf.set(f"fs.azure.account.key.{cuenta}.dfs.core.windows.net", clave)

    print(f"[Persistencia] formato={formato} destino={raiz}")
    return formato, raiz


def ruta_tabla(raiz: str, capa: str, nombre: str) -> str:
    if _es_local(raiz):
        return str(Path(raiz) / capa / nombre)
    return f"{raiz}/{capa}/{nombre}"


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------
def _escribir_spark(df: DataFrame, ruta: str, particion: List[str], formato: str) -> None:
    (df.write
       .format(formato)
       .mode("overwrite")
       .option("partitionOverwriteMode", "dynamic")
       .partitionBy(*particion)
       .save(ruta))


def _escribir_pyarrow(df: DataFrame, ruta: str, particion: List[str]) -> None:
    """
    Respaldo para Windows sin hadoop.dll, donde Spark no puede escribir en disco local.
    Genera el mismo layout (columna=valor/) y también reemplaza solo las particiones que trae.

    toPandas entrega los timestamps sin zona (hora de la sesión) y en nanosegundos, un tipo
    Parquet que Spark 3.x no puede leer. Se pasan a UTC y a microsegundos, como los escribe
    Spark, para que la tabla se lea igual sin importar qué escritor la generó.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    zona = df.sparkSession.conf.get("spark.sql.session.timeZone")
    pdf = df.toPandas()
    for campo in df.schema.fields:
        if isinstance(campo.dataType, TimestampType):
            pdf[campo.name] = pdf[campo.name].dt.tz_localize(zona).dt.tz_convert("UTC")

    tabla = pa.Table.from_pandas(pdf, preserve_index=False)
    esquema = pa.schema([f.with_type(pa.timestamp("us", tz="UTC")) if pa.types.is_timestamp(f.type) else f
                         for f in tabla.schema])
    pq.write_to_dataset(tabla.cast(esquema), root_path=ruta, partition_cols=particion,
                        existing_data_behavior="delete_matching")


def _optimizar_delta(spark: SparkSession, ruta: str, columna: str, desde: str, columnas_zorder: List[str]) -> None:
    """Compacta archivos pequeños y aplica ZORDER solo en las particiones recién escritas."""
    try:
        from delta.tables import DeltaTable
        (DeltaTable.forPath(spark, ruta)
         .optimize()
         .where(f"{columna} >= '{desde}'")
         .executeZOrderBy(*columnas_zorder))
    except Exception as e:  # la optimización no debe tumbar una escritura que ya terminó
        print(f"[Persistencia] AVISO: no se pudo optimizar {ruta}: {e}")


def guardar_tabla(df: DataFrame, raiz: str, capa: str, nombre: str,
                  particion: List[str], formato: str) -> ResultadoPersistencia:
    """
    Escribe df en <raiz>/<capa>/<nombre> particionado por `particion`. Antes de escribir junta
    cada partición en una sola tarea (repartition) para dejar un archivo por partición y no
    muchos archivos pequeños.
    """
    t0 = time.time()
    ruta = ruta_tabla(raiz, capa, nombre)
    df = df.repartition(*particion).persist()
    try:
        resumen = df.agg(F.count(F.lit(1)).alias("filas"),
                         F.countDistinct(*particion).alias("particiones"),
                         F.min(particion[0]).cast("string").alias("desde"),
                         F.max(particion[0]).cast("string").alias("hasta")).first()
        escritor = "spark"
        try:
            _escribir_spark(df, ruta, particion, formato)
        except Exception as e:
            if formato != PARQUET or not _es_local(ruta):
                raise
            print(f"[Persistencia] Spark no pudo escribir en {ruta} ({type(e).__name__}); "
                  "se escribe con pyarrow (típico de Windows sin hadoop.dll).")
            _escribir_pyarrow(df, ruta, particion)
            escritor = "pyarrow"
    finally:
        df.unpersist()

    resultado = ResultadoPersistencia(
        tabla=f"{capa}/{nombre}", ruta=ruta, formato=formato, particion=",".join(particion),
        filas=int(resumen["filas"]), particiones=int(resumen["particiones"]),
        desde=resumen["desde"], hasta=resumen["hasta"], escritor=escritor,
        segundos=round(time.time() - t0, 2))
    print(f"[Persistencia] {resultado.tabla}: {resultado.filas} filas, {resultado.particiones} particiones "
          f"({resultado.particion} {resultado.desde}..{resultado.hasta}), {resultado.formato} con "
          f"{resultado.escritor} en {resultado.segundos:.2f} s -> {ruta}")
    return resultado


# ---------------------------------------------------------------------------
# Capas
# ---------------------------------------------------------------------------
def persistir_silver(df_transacciones: DataFrame, df_logs: DataFrame,
                     formato: str, raiz: str) -> List[ResultadoPersistencia]:
    """
    Transacciones y logs ya validados y limpios, particionados por día (`fecha`).
    La sesión está en America/Bogota (lo exige preparar_persistencia), por eso to_date da el
    día local en las dos fuentes aunque los logs lleguen en UTC.
    """
    resultados = []
    for nombre, df in (("transacciones", df_transacciones), ("logs_ecommerce", df_logs)):
        df_con_fecha = df.withColumn("fecha", F.to_date("timestamp"))
        resultados.append(guardar_tabla(df_con_fecha, raiz, "silver", nombre, ["fecha"], formato))

    if formato == DELTA:
        tx = resultados[0]
        _optimizar_delta(df_transacciones.sparkSession, tx.ruta, "fecha", tx.desde, ZORDER_TRANSACCIONES)
    return resultados


def persistir_gold(agregaciones: Dict[str, DataFrame], formato: str, raiz: str) -> List[ResultadoPersistencia]:
    """
    Agregaciones de negocio. Las que tienen `fecha` se parten por mes; las que resumen todo
    el periodo se guardan como foto de la ejecución en `fecha_proceso`.
    """
    resultados = []
    for nombre, df in agregaciones.items():
        if "fecha" in df.columns:
            df, particion = df.withColumn("mes", F.date_format("fecha", "yyyy-MM")), "mes"
        else:
            df, particion = df.withColumn("fecha_proceso", F.current_date()), "fecha_proceso"
        resultados.append(guardar_tabla(df, raiz, "gold", nombre, [particion], formato))
    return resultados
