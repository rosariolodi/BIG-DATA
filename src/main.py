from pyspark.sql import SparkSession
from src.transformations.cleaning import ejecutar_limpieza_transacciones, ejecutar_limpieza_logs

# Inicializar Spark
spark = SparkSession.builder.appName("Datamarca-Limpieza").getOrCreate()

# Llamar y limpiar transacciones
df_transacciones_clean = ejecutar_limpieza_transacciones(spark)
df_transacciones_clean.show(5)

# Llamar y limpiar logs
df_logs_clean = ejecutar_limpieza_logs(spark)
df_logs_clean.show(5)