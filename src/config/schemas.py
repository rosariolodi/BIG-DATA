"""
Tarea 1.2 - Definición y tipado estricto de esquemas (StructType)
Proyecto Spark Pipeline - DATAMARCA S.A.
Responsable: Julián

Fuentes:
  1. Transacciones POS   (CSV)  -> TRANSACCIONES_SCHEMA
  2. Logs de e-commerce  (JSON) -> LOGS_ECOMMERCE_SCHEMA

Principios:
  * Nunca se usa inferSchema: el esquema se declara explícitamente (reproducible y
    sin el pase extra sobre los datos que cuesta inferSchema).
  * Cada columna tiene un tipo concreto (no todo String) y se documenta su origen.
  * Los dominios de valores (tiendas, categorías, etc.) NO se imponen en el esquema,
    se exponen como constantes para que las reglas de calidad (tarea 1.3 / 2.2) los usen.

AVISO IMPORTANTE sobre nullable:
  Cuando Spark LEE archivos (CSV/JSON/Parquet) con un esquema de usuario, convierte
  todos los campos a nullable=True, aunque aquí se declaren nullable=False. Por eso
  `nullable=False` es la intención/contrato documentado, y el cumplimiento real se
  verifica con las reglas de completitud de la tarea 1.3 (ver `CAMPOS_OBLIGATORIOS_*`).
"""
from pyspark.sql.types import (
    LongType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# 1) Transacciones POS (CSV)
# ---------------------------------------------------------------------------
# Muestra:  TXN10000,2026-09-15 08:02:06,MED-01,Snacks,74979,App
TRANSACCIONES_SCHEMA = StructType([
    # PK. Formato ^TXN\d{5}$
    StructField("transaction_id",   StringType(),    nullable=False),
    # Hora local del POS, sin zona horaria: 'yyyy-MM-dd HH:mm:ss'
    StructField("timestamp",        TimestampType(), nullable=False),
    # Formato ^[A-Z]{3}-\d{2}$ ; prefijo = ciudad (BOG, MED, CAL, BAR)
    StructField("store_id",         StringType(),    nullable=False),
    StructField("product_category", StringType(),    nullable=False),
    # Pesos colombianos enteros (sin centavos). Rango observado 2.005 - 413.280.
    # LongType: cabe holgado en agregaciones y evita overflow al sumar.
    StructField("amount_cop",       LongType(),      nullable=False),
    StructField("payment_method",   StringType(),    nullable=False),
])

# Opciones de lectura asociadas (se pasan a spark.read.options(**...))
TRANSACCIONES_READ_OPTIONS = {
    "header": "true",
    "sep": ",",
    "encoding": "UTF-8",                      # hay tildes: Lácteos, Panadería
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    "timeZone": "America/Bogota",             # el POS registra hora local (ver nota abajo)
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record",
}

# ---------------------------------------------------------------------------
# 2) Logs de e-commerce (JSON)
# ---------------------------------------------------------------------------
# Muestra:  {"event_id": "evt_00001", "timestamp": "2026-09-06T14:23:05Z", ...}
LOGS_ECOMMERCE_SCHEMA = StructType([
    # PK. Formato ^evt_\d{5}$
    StructField("event_id",             StringType(),    nullable=False),
    # ISO-8601 en UTC (sufijo 'Z')
    StructField("timestamp",            TimestampType(), nullable=False),
    # Formato ^usr_\d{4}$
    StructField("user_id",              StringType(),    nullable=False),
    StructField("event_type",           StringType(),    nullable=False),
    StructField("device",               StringType(),    nullable=False),
    # En INGLÉS; no coincide con las categorías del POS (ver EDA, sección 15)
    StructField("product_category",     StringType(),    nullable=False),
    # Segundos, rango observado 10 - 1800 -> IntegerType es suficiente
    StructField("session_duration_sec", IntegerType(),   nullable=False),
])

# El archivo es un ARRAY JSON ([{...}, {...}]) pretty-printed, no JSON Lines
# -> multiLine=true es obligatorio, de lo contrario Spark devuelve solo _corrupt_record.
LOGS_ECOMMERCE_READ_OPTIONS = {
    "multiLine": "true",
    "encoding": "UTF-8",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ssX",   # 'X' interpreta la 'Z' como UTC
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record",
}

# ---------------------------------------------------------------------------
# 3) Variantes con columna de registros corruptos (para modo PERMISSIVE)
# ---------------------------------------------------------------------------
# Con un esquema de usuario, Spark solo guarda el registro corrupto si el esquema
# incluye explícitamente la columna `_corrupt_record` (String).
CORRUPT_FIELD = StructField("_corrupt_record", StringType(), nullable=True)

TRANSACCIONES_SCHEMA_CORRUPT = StructType(TRANSACCIONES_SCHEMA.fields + [CORRUPT_FIELD])
LOGS_ECOMMERCE_SCHEMA_CORRUPT = StructType(LOGS_ECOMMERCE_SCHEMA.fields + [CORRUPT_FIELD])

# ---------------------------------------------------------------------------
# 4) Constantes para las reglas de calidad (tarea 1.3 / 2.2)
# ---------------------------------------------------------------------------
CAMPOS_OBLIGATORIOS_TRANSACCIONES = [f.name for f in TRANSACCIONES_SCHEMA.fields if not f.nullable]
CAMPOS_OBLIGATORIOS_LOGS = [f.name for f in LOGS_ECOMMERCE_SCHEMA.fields if not f.nullable]

# Dominios observados en los 5.000 registros de cada fuente (EDA + perfilado)
DOMINIO_TRANSACCIONES = {
    "store_id": ["BAR-01", "BOG-01", "BOG-02", "CAL-01", "MED-01", "MED-02"],
    "product_category": ["Aseo", "Bebidas", "Carnes", "Frutas", "Granos",
                         "Licores", "Lácteos", "Panadería", "Snacks", "Verduras"],
    "payment_method": ["App", "Bono", "Efectivo", "Tarjeta"],
}
DOMINIO_LOGS = {
    "event_type": ["page_view", "search", "add_to_cart", "remove_from_cart",
                   "checkout_start", "purchase"],
    "device": ["desktop", "mobile", "tablet"],
    "product_category": ["beauty", "clothing", "electronics", "home", "sports"],
}

# Patrones de formato de llaves
PATRON_TRANSACTION_ID = r"^TXN\d{5}$"
PATRON_STORE_ID = r"^[A-Z]{3}-\d{2}$"
PATRON_EVENT_ID = r"^evt_\d{5}$"
PATRON_USER_ID = r"^usr_\d{4}$"


# ---------------------------------------------------------------------------
# 5) Helpers de lectura (los conectores completos son la tarea 2.1 de Joel)
# ---------------------------------------------------------------------------
def leer_transacciones(spark, path):
    """Lee el CSV de transacciones con esquema estricto (sin inferSchema)."""
    return (spark.read
            .schema(TRANSACCIONES_SCHEMA_CORRUPT)
            .options(**TRANSACCIONES_READ_OPTIONS)
            .csv(path))


def leer_logs_ecommerce(spark, path):
    """Lee el JSON de logs con esquema estricto (sin inferencia)."""
    return (spark.read
            .schema(LOGS_ECOMMERCE_SCHEMA_CORRUPT)
            .options(**LOGS_ECOMMERCE_READ_OPTIONS)
            .json(path))
