# Persistencia en Delta Lake o Parquet particionado (Tarea 4.3)

Proyecto Spark Pipeline - DATAMARCA S.A.

Este documento describe cómo se guardan los resultados del pipeline. La estrategia (qué columna particiona cada capa y por qué) está en `docs/particionamiento.md` (tarea 1.4); aquí se documenta su implementación en `src/persistence/writer.py`, que se invoca desde `src/main.py`.

## 1. Qué se guarda

| Capa | Tabla | Origen | Partición |
|---|---|---|---|
| Silver | `silver/transacciones` | Transacciones válidas y limpias (tareas 2.2 y 3.1), con `dq_warnings` | `fecha` |
| Silver | `silver/logs_ecommerce` | Logs válidos y limpios (tareas 2.2 y 3.1), con `dq_warnings` | `fecha` |
| Gold | `gold/ventas_por_dia` | Agregación SparkSQL (tarea 4.2) | `mes` (`AAAA-MM`) |
| Gold | `gold/kpis_negocio_categoria` | Agregación DataFrame (tarea 4.1) | `fecha_proceso` |
| Gold | `gold/kpis_por_categoria`, `gold/top_clientes`, `gold/ventas_por_tienda`, `gold/ventas_por_metodo_pago` | Agregaciones de la tarea 4.2 | `fecha_proceso` |

- **`fecha`** es el día en hora de Bogotá, calculado con `to_date(timestamp)` y la sesión en `America/Bogota` (ver la corrección de la sección 3 de `docs/particionamiento.md`).
- **`mes`** se usa en gold cuando la tabla tiene columna `fecha`.
- **`fecha_proceso`** es el día de la ejecución. Las agregaciones que resumen todo el periodo no tienen fecha propia, así que cada corrida queda como una foto en su partición y se conserva el histórico. Para consultar la última: `WHERE fecha_proceso = (SELECT MAX(fecha_proceso) FROM ...)`.

Ejemplo de estructura generada (local):

```
data/processed/
├── silver/
│   ├── transacciones/fecha=2026-09-15/part-....parquet
│   └── logs_ecommerce/fecha=2026-09-15/part-....parquet
└── gold/
    ├── ventas_por_dia/mes=2026-09/part-....parquet
    └── kpis_por_categoria/fecha_proceso=2026-10-28/part-....parquet
```

## 2. Decisiones de implementación

| Decisión | Motivo |
|---|---|
| Sobrescritura dinámica por partición (`partitionOverwriteMode=dynamic`, también como opción de escritura) | Reprocesar un día solo reemplaza ese día; no se borran los ya procesados |
| `repartition(<columna de partición>)` antes de escribir | Un archivo por partición; evita el problema de archivos pequeños (~150 filas por día en el conjunto de prueba) |
| Formato `auto`: Delta si la sesión lo tiene, si no Parquet | En Databricks se obtiene Delta (ACID, *time travel*) sin configurar nada; en local funciona sin descargar jars |
| En Delta, `OPTIMIZE ... ZORDER BY (store_id, product_category)` sobre `silver/transacciones` | Lo pide la sección 2 de `docs/particionamiento.md`. Solo se optimizan las particiones recién escritas, no la tabla completa |
| Respaldo con pyarrow en Windows | Sin `hadoop.dll`, Spark no puede escribir en disco local. Si eso pasa, se escribe el mismo layout `columna=valor/` con pyarrow, reemplazando solo las particiones que trae la corrida. Los timestamps se guardan en UTC y en microsegundos, como los escribe Spark (Spark 3.x no lee los nanosegundos que pyarrow usa por defecto). No aplica a Delta ni a rutas cloud: ahí el error se propaga |
| Validación antes de procesar | Si la zona horaria de la sesión no es `America/Bogota`, o se pide Delta y la sesión no lo tiene, el pipeline falla al inicio en lugar de guardar datos mal particionados |
| Rutas absolutas desde la raíz del proyecto | Igual que `ingestion/reader.py`; ejecutar desde `src/` ya no crea `src/data/` |

Cada tabla imprime una línea con filas, número de particiones, rango, formato, escritor y tiempo, con esta forma:

```
[Persistencia] silver/transacciones: <filas> filas, <n> particiones (fecha <desde>..<hasta>), parquet con spark en <s> s -> .../data/processed/silver/transacciones
```

Las funciones devuelven esos mismos datos (`ResultadoPersistencia`) para las pruebas de rendimiento (tareas 5.1 y 5.2).

## 3. Variables de entorno

| Variable | Valor por defecto | Uso |
|---|---|---|
| `DATAMARCA_STORAGE_ROOT` | `data/processed` (ruta absoluta del proyecto) | Raíz del almacenamiento. En Azure: `abfss://<contenedor>@<cuenta>.dfs.core.windows.net` |
| `DATAMARCA_FORMATO` | `auto` | `auto`, `delta` o `parquet` |
| `AZURE_STORAGE_ACCOUNT` | (sin valor) | Cuenta de ADLS Gen2, solo fuera de Databricks |
| `AZURE_STORAGE_KEY` | (sin valor) | Clave de la cuenta. **Nunca se versiona**; va en `.env` o en un secreto |
| `DATAMARCA_HADOOP_AZURE_PKG` | `org.apache.hadoop:hadoop-azure:3.3.4` | Conector ADLS fuera de Databricks; debe coincidir con la versión de Hadoop de pyspark |

## 4. Cómo ejecutar

Instalar dependencias (desde la raíz del repositorio):

```bash
pip install -r src/requitements.txt
```

**Local, Parquet particionado (por defecto):**

```bash
cd src && python main.py
```

**Local, Delta Lake** (la primera vez descarga los jars de Delta desde Maven; requiere Java y no funciona en Windows sin `hadoop.dll`):

```bash
cd src && DATAMARCA_FORMATO=delta python main.py
```

**Azure Databricks:** importar el repositorio (Repos) y ejecutar `src/main.py` como job con `DATAMARCA_STORAGE_ROOT=abfss://<contenedor>@<cuenta>.dfs.core.windows.net`. Delta y el acceso a ADLS ya vienen en el runtime; el acceso se configura en el clúster (Unity Catalog o *service principal*), no con la clave de cuenta.

**Spark local escribiendo en ADLS Gen2:** definir `DATAMARCA_STORAGE_ROOT`, `AZURE_STORAGE_ACCOUNT` y `AZURE_STORAGE_KEY`; el conector `hadoop-azure` se descarga al crear la sesión.

**Verificar lo escrito:**

```python
spark.read.parquet("data/processed/silver/transacciones").where("fecha = '2026-09-15'").count()
# En Delta: spark.read.format("delta").load(...)
```

## 5. Supuestos y pendientes

- **No se ha probado contra Azure.** La infraestructura de `azure/infrastructure/` aún está vacía; la escritura en `abfss://` queda lista por configuración, pero falta crear la cuenta ADLS Gen2 y ejecutarla.
- **Bronze no se persiste** en esta tarea: la 4.3 pide los resultados finales. Los datos crudos siguen en `data/raw/`.
- **Archivo anterior:** `data/processed/kpis_negocio_categoria/resultado.parquet` era la salida con pandas sin particionar. `src/main.py` ya no lo genera; se conserva como referencia hasta que el equipo decida retirarlo.
- **`guardar_resultados` en `transformations/agregaciones.py`** (tarea 4.2) ya no se llama desde `main.py`; la persistencia de esas tablas la hace este módulo.
- **Volumen real:** con ~50 millones de filas diarias, un archivo por partición diaria queda en 1 a 2 GB. Si las pruebas de la tarea 5.1 muestran escrituras lentas, se puede usar `spark.sql.files.maxRecordsPerFile` para dividirlo.
