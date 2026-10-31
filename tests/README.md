# Pruebas automáticas

Pruebas con `pytest` sobre Spark local. No leen los archivos de `data/raw/`: generan sus propios datos pequeños en una carpeta temporal.

| Archivo | Qué prueba |
|---|---|
| `test_validacion.py` | Reglas de calidad de `docs/reglas_calidad.md` (tarea 2.2): cada regla crítica rechaza a cuarentena con el ID correcto, las advertencias no rechazan, duplicados, métricas y la regresión del JSON con un valor mal tipado |
| `test_idempotencia.py` | Reprocesar el mismo lote dos veces da el mismo resultado: agregaciones idénticas, totales sin duplicar, archivos de salida y cuarentena reemplazados y no acumulados (prueba P4 del informe de la Unidad 2) |
| `test_tiempos.py` | Registro de tiempos por etapa (tarea 5.2): duración, filas, estados de error, suma por etapa, JSON e historial CSV |
| `test_agregaciones.py` | Agregaciones de la tarea 4.2 con un conjunto calculado a mano: por tienda, método de pago, día (en hora de Bogotá), top de clientes, y que SparkSQL coincida con la API de DataFrames |

## Cómo ejecutarlas

Requisitos: Java 17 o 21 y Python 3.12 (PySpark 3.5 no soporta Python 3.14).

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r tests/requirements-test.txt
pytest tests -v
```

Resultado esperado: todas las pruebas en `passed` y ninguna en `failed`. Tarda unos 2 minutos por el arranque de Spark.

## Notas

- `conftest.py` fija `spark.driver.host=127.0.0.1` para evitar el error `Invalid Spark URL` en equipos cuyo nombre contiene guion bajo, y la zona horaria `America/Bogota`.
- Los datos de `test_agregaciones.py` se arman con SQL (`VALUES`) y no con `createDataFrame`, para no depender de workers de Python.
