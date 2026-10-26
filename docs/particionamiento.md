# Estrategia de particionamiento para almacenamiento distribuido en la nube (Tarea 1.4)

Proyecto Spark Pipeline - DATAMARCA S.A.

Este documento **define** cómo se particionan los datos en el almacenamiento cloud. La escritura en Delta Lake o Parquet corresponde a la tarea 4.3. Las columnas y formatos salen de `src/config/schemas.py` y de `docs/reglas_calidad.md`.

## 1. Contexto y supuestos

| Aspecto | Valor |
|---|---|
| Nube y almacenamiento | Azure, ADLS Gen2 (según el informe técnico de la Unidad 2) |
| Formato | Delta Lake sobre Parquet (ACID, control de esquema, *time travel*) |
| Arquitectura | Medallion: Bronze, Silver y Gold |
| Volumen real del negocio | ~50 millones de transacciones al día |
| Volumen del conjunto de prueba | 5.000 transacciones (31 días, 139 a 195 por día completo) y los logs de e-commerce |
| Consultas típicas | Reportes diarios de inventario y ventas, ventanas de tiempo y cruces entre fuentes por fecha |

**Supuesto de tamaño (a validar con la prueba de rendimiento):** con 6 columnas, 50 millones de filas diarias ocupan del orden de 1 a 2 GB en Parquet comprimido. Una partición diaria queda, por tanto, en un tamaño razonable en producción.

## 2. Decisión principal: partición por fecha

Se particiona por **`fecha`** (una columna, un valor por día, por ejemplo `fecha=2026-09-15`).

| Criterio | Por qué la fecha |
|---|---|
| Patrón de consulta | Casi todas las consultas filtran por rango de fechas; Spark descarta particiones completas (*partition pruning*) |
| Distribución | Crece de forma pareja día a día, sin particiones muy desiguales |
| Cruce entre fuentes | Las fuentes no comparten llave; el único eje común es el tiempo |
| Operación | Permite reprocesar un día y borrar por retención sin tocar el resto |

### Columnas que NO se usan como partición

| Columna | Motivo |
|---|---|
| `transaction_id`, `event_id`, `user_id` | Cardinalidad altísima: miles de particiones y archivos diminutos |
| `store_id` (6 valores) y `ciudad` (4) | Reparto desigual (BOG-02 tiene ~14 % más transacciones que MED-01) y subparticiones pequeñas dentro de cada día |
| `product_category` | 10 categorías en el POS y otras en los logs; cambia de idioma entre fuentes y puede crecer |
| `payment_method`, `event_type`, `device` | Pocos valores y baja utilidad para filtrar a nivel de carpeta |

Estas columnas se aprovechan dentro de los archivos mediante estadísticas de Parquet y, en Delta, con `OPTIMIZE ... ZORDER BY (store_id, product_category)`.

## 3. Cálculo de `fecha`: una sola zona horaria

Las fuentes registran el tiempo de forma distinta:

| Fuente | Zona horaria del `timestamp` |
|---|---|
| Transacciones POS | Hora local de Bogotá, sin zona |
| Logs de e-commerce | UTC (sufijo `Z`) |

Para que un mismo día signifique lo mismo en las dos fuentes, `fecha` se calcula **siempre en `America/Bogota`**:
- Se configura `spark.sql.session.timeZone = America/Bogota`.
- En los logs se convierte con `from_utc_timestamp(timestamp, 'America/Bogota')` antes de aplicar `to_date`.
- En el POS se usa `to_date(timestamp)` (ya está en hora local).

Sin este paso, un evento de las 22:00 en Bogotá (03:00 UTC del día siguiente) caería en una partición distinta a la de las ventas de ese mismo día.

## 4. Estructura por capa

| Capa | Ruta (dentro del contenedor ADLS Gen2) | Partición |
|---|---|---|
| Bronze | `bronze/transacciones/fecha=AAAA-MM-DD/` | `fecha` |
| Bronze | `bronze/logs_ecommerce/fecha=AAAA-MM-DD/` | `fecha` |
| Silver | `silver/transacciones/fecha=AAAA-MM-DD/` | `fecha` |
| Silver | `silver/logs_ecommerce/fecha=AAAA-MM-DD/` | `fecha` |
| Cuarentena | `quarantine/<fuente>/fecha_ingestion=AAAA-MM-DD/` | fecha de ingestión |
| Gold | `gold/<tabla_agregada>/` (por ejemplo ventas diarias por tienda) | `mes` (`AAAA-MM`) |

Ruta completa de ejemplo: `abfss://<contenedor>@<cuenta>.dfs.core.windows.net/silver/transacciones/fecha=2026-09-15/`.

- **Bronze y Silver** conservan una carpeta por día porque se reprocesan por día.
- **Cuarentena** se parte por la fecha de ingestión, no por la del evento, porque un registro erróneo puede traer una fecha inválida y no tendría partición.
- **Gold** son tablas ya agregadas y pequeñas; particionar por día generaría archivos casi vacíos, por eso se agrupa por mes.

## 5. Riesgos y mitigaciones

| Riesgo | Efecto | Mitigación |
|---|---|---|
| Problema de archivos pequeños | Con pocos registros por día (como en el conjunto de prueba, ~150 filas/día) cada partición es un archivo diminuto | Compactar con `OPTIMIZE` en Delta; usar `coalesce`/`repartition` antes de escribir; en la prueba de rendimiento medir el efecto |
| Demasiadas particiones | Años de historia con partición diaria suponen miles de carpetas | Retención definida (12 meses según el roadmap del informe) y `VACUUM` |
| Partición con valores nulos o inválidos | Registros sin fecha válida | Ya se rechazan a cuarentena por las reglas T-C01/T-I03 y L-C01/L-I03 |
| Reescritura accidental de todo el dataset | Perder días ya procesados | `spark.sql.sources.partitionOverwriteMode = dynamic` (ya configurado en `src/main.py`), que solo sobrescribe las particiones presentes en el DataFrame |

## 6. Cómo se implementa (para la tarea 4.3)

Esquema general de escritura, a ajustar cuando se defina el almacenamiento:

```python
(df
 .withColumn("fecha", to_date(col("timestamp")))   # en logs: to_date(from_utc_timestamp(col("timestamp"), "America/Bogota"))
 .write
 .format("delta")                                   # o "parquet" si no hay Delta disponible
 .mode("overwrite")
 .partitionBy("fecha")
 .save("abfss://<contenedor>@<cuenta>.dfs.core.windows.net/silver/transacciones"))
```

Nota: `src/main.py` hoy guarda el resultado con pandas en un único archivo Parquet, para evitar errores de winutils en Windows. Ese camino no aplica particionamiento. La tarea 4.3 debe reemplazarlo por la escritura con Spark (`partitionBy`) o, si el entorno local no lo permite, ejecutar la persistencia particionada en el entorno cloud o en Colab.

## 7. Qué se evaluará en las pruebas de rendimiento (tareas 5.x)

- Tiempo de lectura con filtro por `fecha` frente a lectura completa (efecto del *partition pruning*).
- Número y tamaño de los archivos generados por partición.
- Comportamiento al aumentar el volumen (por ejemplo, multiplicar el conjunto de prueba x10, x100).

## 8. Pendientes que dependen de otras tareas

- **Logs definitivos:** el archivo de logs cambió recientemente; las rutas y la columna `fecha` no dependen de sus categorías, pero conviene confirmar el rango de fechas final.
- **Tablas Gold:** las agregaciones de negocio (tarea 4.2) definirán cuáles se materializan y su partición mensual.
