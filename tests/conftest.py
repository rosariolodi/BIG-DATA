import os
import sys
from pathlib import Path

import pytest

# Permite `from ingestion.validacion import ...` igual que cuando se ejecuta src/main.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Los workers de Spark deben usar el mismo Python que ejecuta las pruebas
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    sesion = (SparkSession.builder
              .master("local[2]")
              .appName("tests-datamarca")
              .config("spark.ui.enabled", "false")
              # Evita "Invalid Spark URL" en equipos cuyo nombre tiene guion bajo
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
              # Las ventas por día se agrupan en hora de Bogotá
              .config("spark.sql.session.timeZone", "America/Bogota")
              .config("spark.sql.shuffle.partitions", "2")
              .getOrCreate())
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()
