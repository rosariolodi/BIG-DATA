"""
Rutas de entrada/salida del pipeline: local (por defecto) o Azure (ADLS Gen2).

Cómo se elige el modo:
- Sin variables de entorno -> modo LOCAL: lee de data/raw y escribe en data/ (igual que antes).
- Con DATAMARCA_STORAGE=<cuenta de almacenamiento> -> modo AZURE: lee de bronze y escribe en
  silver (cuarentena) y gold (resultados) en formato Delta Lake, según docs/particionamiento.md.

En Databricks la variable se define en el cluster y el acceso al storage se configura con la
clave guardada en un secret scope (ver azure/README.md).
"""

import os
import tempfile
from pathlib import Path

# config/ -> src/ -> [RAÍZ DEL PROYECTO]
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

CUENTA = os.environ.get("DATAMARCA_STORAGE")
EN_NUBE = bool(CUENTA)


def _abfss(contenedor: str, ruta: str = "") -> str:
    base = f"abfss://{contenedor}@{CUENTA}.dfs.core.windows.net"
    return f"{base}/{ruta}" if ruta else base


if EN_NUBE:
    CSV_TRANSACCIONES = _abfss("bronze", "DATAMARCA_transacciones_5000_real.csv")
    JSON_LOGS = _abfss("bronze", "ecommerce_logs_5000.json")
    DIR_CALIDAD = _abfss("silver", "quarantine")
    DIR_GOLD = _abfss("gold")
    DIR_KPIS = _abfss("gold", "kpis_negocio_categoria")
    # El disco del driver es temporal; por eso los tiempos también se publican en ADLS (DIR_TIEMPOS_NUBE)
    DIR_TIEMPOS = Path(tempfile.gettempdir()) / "tiempos"
    DIR_TIEMPOS_NUBE = _abfss("gold", "reports/tiempos")
else:
    CSV_TRANSACCIONES = str(ROOT_DIR / "data" / "raw" / "DATAMARCA_transacciones_5000_real.csv")
    JSON_LOGS = str(ROOT_DIR / "data" / "raw" / "ecommerce_logs_5000.json")
    DIR_CALIDAD = str(ROOT_DIR / "data" / "quarantine")
    DIR_GOLD = str(ROOT_DIR / "data" / "processed")
    DIR_KPIS = str(ROOT_DIR / "data" / "processed" / "kpis_negocio_categoria")
    DIR_TIEMPOS = ROOT_DIR / "reports" / "tiempos"
    DIR_TIEMPOS_NUBE = None


def escribir_delta(df, ruta: str, modo: str = "overwrite") -> int:
    """Escribe un DataFrame en Delta Lake y devuelve el número de filas escritas."""
    df.write.format("delta").mode(modo).option("overwriteSchema", "true").save(ruta)
    return df.sparkSession.read.format("delta").load(ruta).count()