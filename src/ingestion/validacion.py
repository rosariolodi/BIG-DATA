"""
Validación de calidad de datos en la ingestión (Inciso 2.2)
DATAMARCA S.A.

Implementa el catálogo de reglas definido en docs/reglas_calidad.md:
  * Reglas CRÍTICAS: el registro se rechaza y va a CUARENTENA con los IDs de todas
    las reglas que incumplió.
  * Reglas de ADVERTENCIA: el registro continúa y queda marcado en `dq_warnings`.

Todo se expresa con funciones nativas de Spark (sin UDFs de Python) para que el
optimizador Catalyst pueda planificar la evaluación completa en una sola pasada.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from config.rutas import EN_NUBE
from config.schemas import (
    DOMINIO_LOGS,
    DOMINIO_TRANSACCIONES,
    PATRON_EVENT_ID,
    PATRON_STORE_ID,
    PATRON_TRANSACTION_ID,
    PATRON_USER_ID,
)

CRITICA = "critica"
ADVERTENCIA = "advertencia"

# Rango de duración de sesión observado en los logs (regla L-D04)
DURACION_MIN_SEC = 10
DURACION_MAX_SEC = 1800

# Factor del IQR para marcar montos extremos (regla T-D04)
FACTOR_IQR_OUTLIER = 3.0


@dataclass
class Regla:
    id: str
    descripcion: str
    severidad: str
    falla: Column  # True cuando el registro INCUMPLE la regla (nunca nulo)


@dataclass
class ResultadoValidacion:
    fuente: str
    validos: DataFrame
    cuarentena: DataFrame
    metricas: Dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers de expresiones
# ---------------------------------------------------------------------------
def _flag(nombre_regla: str) -> str:
    return "_f_" + nombre_regla.replace("-", "_")


def _vacio(columna: str, es_texto: bool) -> Column:
    c = F.col(columna)
    return (c.isNull() | (F.trim(c) == "")) if es_texto else c.isNull()


def _incumple_patron(columna: str, patron: str) -> Column:
    # Un valor nulo no se reporta aquí: lo cubre la regla de completitud.
    return F.coalesce(~F.col(columna).rlike(patron), F.lit(False))


def _fuera_de_dominio(columna: str, dominio: List[str]) -> Column:
    return F.coalesce(~F.col(columna).isin(dominio), F.lit(False))


def _completitud(columnas_texto: List[str], columnas_otras: List[str]) -> Column:
    condiciones = [_vacio(c, True) for c in columnas_texto] + [_vacio(c, False) for c in columnas_otras]
    expr = condiciones[0]
    for c in condiciones[1:]:
        expr = expr | c
    return expr


# ---------------------------------------------------------------------------
# Catálogos de reglas (IDs idénticos a docs/reglas_calidad.md)
# ---------------------------------------------------------------------------
def _reglas_transacciones(df: DataFrame) -> List[Regla]:
    limite_outlier = _limite_superior_iqr(df, "amount_cop")
    outlier = (F.coalesce(F.col("amount_cop") > F.lit(limite_outlier), F.lit(False))
               if limite_outlier is not None else F.lit(False))
    return [
        Regla("T-C01", "Campos obligatorios no nulos ni vacíos", CRITICA,
              _completitud(["transaction_id", "user_id", "store_id", "product_category", "payment_method"],
                           ["timestamp", "amount_cop"])),
        Regla("T-I01", "transaction_id cumple ^TXN\\d{5}$", CRITICA,
              _incumple_patron("transaction_id", PATRON_TRANSACTION_ID)),
        Regla("T-I02", "transaction_id es único", CRITICA, F.col("_dq_dup_transaction_id")),
        Regla("T-I03", "timestamp interpretable", CRITICA, F.col("timestamp").isNull()),
        Regla("T-I04", "store_id cumple ^[A-Z]{3}-\\d{2}$", CRITICA,
              _incumple_patron("store_id", PATRON_STORE_ID)),
        Regla("T-I05", "amount_cop es mayor que 0", CRITICA,
              F.coalesce(F.col("amount_cop") <= 0, F.lit(False))),
        Regla("T-I06", "Registro no corrupto", CRITICA, F.col("_corrupt_record").isNotNull()),
        Regla("T-I07", "user_id cumple ^usr_\\d{4}$", CRITICA,
              _incumple_patron("user_id", PATRON_USER_ID)),
        Regla("T-D01", "store_id en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("store_id", DOMINIO_TRANSACCIONES["store_id"])),
        Regla("T-D02", "product_category en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("product_category", DOMINIO_TRANSACCIONES["product_category"])),
        Regla("T-D03", "payment_method en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("payment_method", DOMINIO_TRANSACCIONES["payment_method"])),
        Regla("T-D04", "amount_cop no es un outlier extremo (Q3 + 3*IQR)", ADVERTENCIA, outlier),
    ]


def _reglas_logs() -> List[Regla]:
    return [
        Regla("L-C01", "Campos obligatorios no nulos ni vacíos", CRITICA,
              _completitud(["event_id", "user_id", "event_type", "device", "product_category"],
                           ["timestamp", "session_duration_sec"])),
        Regla("L-I01", "event_id cumple ^evt_\\d{5}$", CRITICA,
              _incumple_patron("event_id", PATRON_EVENT_ID)),
        Regla("L-I02", "event_id es único", CRITICA, F.col("_dq_dup_event_id")),
        Regla("L-I03", "timestamp interpretable", CRITICA, F.col("timestamp").isNull()),
        Regla("L-I04", "user_id cumple ^usr_\\d{4}$", CRITICA,
              _incumple_patron("user_id", PATRON_USER_ID)),
        Regla("L-I05", "session_duration_sec es mayor que 0", CRITICA,
              F.coalesce(F.col("session_duration_sec") <= 0, F.lit(False))),
        Regla("L-I06", "Registro no corrupto", CRITICA, F.col("_corrupt_record").isNotNull()),
        Regla("L-D01", "event_type en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("event_type", DOMINIO_LOGS["event_type"])),
        Regla("L-D02", "device en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("device", DOMINIO_LOGS["device"])),
        Regla("L-D03", "product_category en el dominio conocido", ADVERTENCIA,
              _fuera_de_dominio("product_category", DOMINIO_LOGS["product_category"])),
        Regla("L-D04", f"session_duration_sec entre {DURACION_MIN_SEC} y {DURACION_MAX_SEC}", ADVERTENCIA,
              F.coalesce(~F.col("session_duration_sec").between(DURACION_MIN_SEC, DURACION_MAX_SEC),
                         F.lit(False))),
    ]


def _limite_superior_iqr(df: DataFrame, columna: str):
    """Q3 + FACTOR_IQR_OUTLIER * IQR sobre los valores positivos; None si no hay datos."""
    cuantiles = df.filter(F.col(columna) > 0).approxQuantile(columna, [0.25, 0.75], 0.01)
    if len(cuantiles) < 2:
        return None
    q1, q3 = cuantiles
    return q3 + FACTOR_IQR_OUTLIER * (q3 - q1)


# ---------------------------------------------------------------------------
# Motor de validación
# ---------------------------------------------------------------------------
def _marcar_duplicados(df: DataFrame, llave: str) -> DataFrame:
    """Marca como duplicada toda aparición de la llave después de la primera."""
    orden = Window.partitionBy(llave).orderBy("_dq_fila")
    return df.withColumn(
        "_dq_dup_" + llave,
        F.col(llave).isNotNull() & (F.row_number().over(orden) > 1),
    )


def _validar(df_crudo: DataFrame, fuente: str, llave: str, construir_reglas) -> ResultadoValidacion:
    columnas = df_crudo.columns  # incluye _corrupt_record
    columnas_datos = [c for c in columnas if c != "_corrupt_record"]

    df = df_crudo.withColumn("_dq_fila", F.monotonically_increasing_id())
    df = _marcar_duplicados(df, llave)

    reglas = construir_reglas(df)
    for r in reglas:
        df = df.withColumn(_flag(r.id), r.falla)

    def ids(severidad):
        return [r.id for r in reglas if r.severidad == severidad]

    def lista_ids(severidad):
        marcas = [F.when(F.col(_flag(i)), F.lit(i)) for i in ids(severidad)]
        return F.filter(F.array(*marcas), lambda x: x.isNotNull())

    df = (df.withColumn("reglas_fallidas", lista_ids(CRITICA))
            .withColumn("dq_warnings", lista_ids(ADVERTENCIA))
            .cache())

    validos = (df.filter(F.size("reglas_fallidas") == 0)
                 .select(*columnas_datos, "dq_warnings"))
    cuarentena = (df.filter(F.size("reglas_fallidas") > 0)
                    .select(F.lit(fuente).alias("fuente"),
                            F.to_json(F.struct(*columnas)).alias("registro_original"),
                            "reglas_fallidas",
                            F.current_timestamp().alias("fecha_ingestion")))

    fila = df.agg(
        F.count(F.lit(1)).alias("_leidos"),
        F.sum(F.when(F.size("reglas_fallidas") > 0, 1).otherwise(0)).alias("_cuarentena"),
        *[F.sum(F.col(_flag(r.id)).cast("int")).alias(_flag(r.id)) for r in reglas],
    ).first()

    leidos = int(fila["_leidos"] or 0)
    en_cuarentena = int(fila["_cuarentena"] or 0)
    metricas = {
        "fuente": fuente,
        "leidos": leidos,
        "validos": leidos - en_cuarentena,
        "cuarentena": en_cuarentena,
        "pct_rechazo": round(100.0 * en_cuarentena / leidos, 4) if leidos else 0.0,
        "por_regla": [
            {
                "id": r.id,
                "severidad": r.severidad,
                "descripcion": r.descripcion,
                "fallos": int(fila[_flag(r.id)] or 0),
                "pct": round(100.0 * int(fila[_flag(r.id)] or 0) / leidos, 4) if leidos else 0.0,
            }
            for r in reglas
        ],
    }
    return ResultadoValidacion(fuente, validos, cuarentena, metricas)


def validar_transacciones(df_crudo: DataFrame) -> ResultadoValidacion:
    """Aplica el catálogo T-* al DataFrame crudo de transacciones (con _corrupt_record)."""
    return _validar(df_crudo, "transacciones", "transaction_id", _reglas_transacciones)


def validar_logs(df_crudo: DataFrame) -> ResultadoValidacion:
    """
    Aplica el catálogo L-* al DataFrame crudo de logs (con _corrupt_record).
    `timestamp` y `session_duration_sec` pueden llegar como texto: se convierten aquí con
    try_cast, de modo que un valor inválido queda nulo y solo rechaza ese registro.
    """
    df_tipado = (df_crudo
                 .withColumn("timestamp", F.expr("try_cast(`timestamp` AS timestamp)"))
                 .withColumn("session_duration_sec", F.expr("try_cast(`session_duration_sec` AS int)")))
    return _validar(df_tipado, "logs_ecommerce", "event_id", lambda _df: _reglas_logs())


# ---------------------------------------------------------------------------
# Salida: consola y archivos
# ---------------------------------------------------------------------------
def imprimir_metricas(resultado: ResultadoValidacion) -> None:
    m = resultado.metricas
    print(f"[Calidad:{m['fuente']}] leídos={m['leidos']} válidos={m['validos']} "
          f"cuarentena={m['cuarentena']} ({m['pct_rechazo']}% rechazo)")
    for r in m["por_regla"]:
        if r["fallos"]:
            print(f"    {r['id']} [{r['severidad']}] {r['fallos']} registros ({r['pct']}%) - {r['descripcion']}")


def guardar_resultado_validacion(resultado: ResultadoValidacion, directorio: str) -> None:
    """
    Guarda la cuarentena (Parquet) y las métricas (JSON) de una fuente.
    Se usa pandas para escribir, igual que src/main.py, para evitar el problema
    de winutils/hadoop.dll de Spark en Windows.
    """
    if EN_NUBE:
        # En Azure se escribe con Spark directamente en ADLS Gen2 (no hay disco local persistente)
        resultado.cuarentena.write.mode("overwrite").parquet(f"{directorio}/cuarentena_{resultado.fuente}")
        spark = resultado.cuarentena.sparkSession
        (spark.createDataFrame([(json.dumps(resultado.metricas, ensure_ascii=False, default=str),)], ["json"])
              .coalesce(1).write.mode("overwrite").text(f"{directorio}/metricas_{resultado.fuente}"))
        return
    carpeta = Path(directorio)
    carpeta.mkdir(parents=True, exist_ok=True)
    resultado.cuarentena.toPandas().to_parquet(carpeta / f"cuarentena_{resultado.fuente}.parquet", index=False)
    with open(carpeta / f"metricas_{resultado.fuente}.json", "w", encoding="utf-8") as f:
        json.dump(resultado.metricas, f, ensure_ascii=False, indent=2)
