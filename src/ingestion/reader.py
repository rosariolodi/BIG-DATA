"""
Módulo de Ingestión y Validación de Calidad (Incisos 2.1 y 2.2)
DATAMARCA S.A.

2.1: conectores de lectura con esquema estricto (src/config/schemas.py), sin inferSchema.
2.2: validación de calidad con cuarentena y métricas (ver ingestion/validacion.py).
"""

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from config.schemas import (
    TRANSACCIONES_SCHEMA_CORRUPT,
    TRANSACCIONES_READ_OPTIONS,
    LOGS_ECOMMERCE_SCHEMA_CORRUPT,
    LOGS_ECOMMERCE_READ_OPTIONS,
)
from ingestion.validacion import (
    ResultadoValidacion,
    guardar_resultado_validacion,
    imprimir_metricas,
    validar_logs,
    validar_transacciones,
)

# Calculamos la ruta absoluta de la raíz del proyecto de forma dinámica:
# reader.py -> ingestion/ -> src/ -> [RAÍZ DEL PROYECTO]
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

DEFAULT_CSV_PATH = str(ROOT_DIR / "data" / "raw" / "DATAMARCA_transacciones_5000_real.csv")
DEFAULT_JSON_PATH = str(ROOT_DIR / "data" / "raw" / "ecommerce_logs_5000.json")
# Carpeta donde se guardan la cuarentena y las métricas de calidad de cada ejecución
DEFAULT_CALIDAD_DIR = str(ROOT_DIR / "data" / "quarantine")

# En un JSON multiLine (array), un valor con el tipo equivocado marca TODOS los registros
# del archivo como corruptos (el "registro corrupto" es el documento completo). Por eso los
# campos no textuales se leen como String y se convierten en la validación con try_cast:
# un valor inválido solo rechaza ese registro (reglas L-I03 y L-I05).
_LOGS_CAMPOS_COMO_TEXTO = {"timestamp", "session_duration_sec"}
LOGS_SCHEMA_LECTURA = StructType([
    StructField(f.name, StringType(), True) if f.name in _LOGS_CAMPOS_COMO_TEXTO else f
    for f in LOGS_ECOMMERCE_SCHEMA_CORRUPT.fields
])


# ---------------------------------------------------------------------------
# 2.1 Conectores de lectura (sin filtrar)
# ---------------------------------------------------------------------------
def leer_transacciones_csv_crudo(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH) -> DataFrame:
    """
    Lee el CSV de transacciones con esquema estricto (sin inferSchema).
    Los registros mal formados quedan en la columna `_corrupt_record` (modo PERMISSIVE).
    """
    return (spark.read
            .schema(TRANSACCIONES_SCHEMA_CORRUPT)
            .options(**TRANSACCIONES_READ_OPTIONS)
            .csv(ruta_csv))


def leer_logs_ecommerce_json_crudo(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH) -> DataFrame:
    """
    Lee los logs JSON de e-commerce con esquema explícito (sin inferencia).
    El archivo es un array JSON, por eso se usa multiLine (ver LOGS_ECOMMERCE_READ_OPTIONS).
    `timestamp` y `session_duration_sec` llegan como texto (ver LOGS_SCHEMA_LECTURA) y
    `validar_logs` los convierte a su tipo final.
    """
    return (spark.read
            .schema(LOGS_SCHEMA_LECTURA)
            .options(**LOGS_ECOMMERCE_READ_OPTIONS)
            .json(ruta_json))


# ---------------------------------------------------------------------------
# 2.2 Ingestión con validación de calidad
# ---------------------------------------------------------------------------
def _publicar(resultado: ResultadoValidacion, directorio_calidad) -> ResultadoValidacion:
    imprimir_metricas(resultado)
    if directorio_calidad:
        try:
            guardar_resultado_validacion(resultado, directorio_calidad)
        except Exception as e:  # la ingestión no debe caerse por no poder escribir la cuarentena
            print(f"[Calidad:{resultado.fuente}] No se pudo guardar la cuarentena en {directorio_calidad}: {e}")
    return resultado


def ingestar_transacciones(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH,
                           directorio_calidad=DEFAULT_CALIDAD_DIR) -> ResultadoValidacion:
    """Lee el CSV, aplica las reglas T-* y devuelve válidos, cuarentena y métricas."""
    return _publicar(validar_transacciones(leer_transacciones_csv_crudo(spark, ruta_csv)), directorio_calidad)


def ingestar_logs_ecommerce(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH,
                            directorio_calidad=DEFAULT_CALIDAD_DIR) -> ResultadoValidacion:
    """Lee el JSON, aplica las reglas L-* y devuelve válidos, cuarentena y métricas."""
    return _publicar(validar_logs(leer_logs_ecommerce_json_crudo(spark, ruta_json)), directorio_calidad)


# Se mantienen los nombres que usa transformations/cleaning.py: devuelven solo los registros
# válidos (con la columna `dq_warnings`); la cuarentena y las métricas se guardan aparte.
def leer_transacciones_csv(spark: SparkSession, ruta_csv: str = DEFAULT_CSV_PATH) -> DataFrame:
    return ingestar_transacciones(spark, ruta_csv).validos


def leer_logs_ecommerce_json(spark: SparkSession, ruta_json: str = DEFAULT_JSON_PATH) -> DataFrame:
    return ingestar_logs_ecommerce(spark, ruta_json).validos
