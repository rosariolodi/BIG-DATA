# Pruebas automáticas

Pruebas con `pytest` sobre Spark local. No leen los archivos de `data/raw/`: generan sus propios datos pequeños en una carpeta temporal.

| Archivo | Qué prueba |
|---|---|
| `test_validacion.py` | Reglas de calidad de `docs/reglas_calidad.md` (tarea 2.2): cada regla crítica rechaza a cuarentena con el ID correcto, las advertencias no rechazan, duplicados, métricas y la regresión del JSON con un valor mal tipado |
| `test_idempotencia.py` | Reprocesar el mismo lote dos veces da el mismo resultado: agregaciones idénticas, totales sin duplicar, archivos de salida y cuarentena reemplazados y no acumulados (prueba P4 del informe de la Unidad 2) |
| `test_tiempos.py` | Registro de tiempos por etapa (tarea 5.2): duración, filas, estados de error, suma por etapa, JSON e historial CSV |
| `test_agregaciones.py` | Agregaciones de la tarea 4.2 con un conjunto calculado a mano: por tienda, método de pago, día (en hora de Bogotá), top de clientes, y que SparkSQL coincida con la API de DataFrames |
| `test_alertas.py`, `test_recuperacion_alertas.py`, `test_estado_alertas_portable.py` | Envío simulado, deduplicación entre hilos/procesos, recuperación y compatibilidad del registro de alertas con Windows y POSIX; no usan el webhook real |
| `test_orquestacion.py` | Manejo de errores y trazas de `src/orquestacion/tareas.py` (tarea T1.3), sin Spark ni Airflow salvo un caso: código de salida y estado de una tarea fallida, fallo forzado y recuperación, demora para probar timeouts, compuerta de calidad, fallos rápidos de la ingestión, resumen de `notificar` (fallidas, interrumpidas, no ejecutadas y completadas) y retención de `data/_staging/` (limpieza de exitosas y purga por antigüedad). Detalle en `docs/orquestacion/manejo_errores_logging.md` |

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

## Compatibilidad de las alertas con Windows

El registro de alertas usa `msvcrt.locking` en Windows y `fcntl.flock` en Linux/macOS,
con el mismo plazo y exclusión entre procesos. No se omiten las pruebas de alertas en
Windows. Se sincroniza el archivo JSON antes de reemplazarlo de forma atómica; POSIX
además sincroniza el directorio. Windows no usa `os.open/fsync` sobre directorios y no
se promete esa garantía adicional frente a una caída del host.

`python -m pytest tests -v` ejecuta la suite completa. Para comprobar únicamente las
alertas sin iniciar Spark: `python -m pytest tests/test_alertas.py tests/test_recuperacion_alertas.py tests/test_estado_alertas_portable.py -v`.
El workflow `.github/workflows/tests.yml` comprueba el comando completo con Python 3.12
y Java 17 en Windows y Ubuntu; sus resultados publicados acreditan cada plataforma.
Esto no convierte Airflow en una aplicación nativa de Windows: el DAG se ejecuta en Docker/Linux.

### Verificación del 9 de noviembre de 2026

La [ejecución 42604479494](https://github.com/rosariolodi/BIG-DATA/actions/runs/42604479494)
probó el commit `5f21886` con `python -m pytest tests -v`, Python 3.12 y Java 17:

| Plataforma | Resultado |
|---|---|
| Windows (`windows-latest`) | 141 passed; ninguna prueba omitida |
| Ubuntu (`ubuntu-latest`) | 141 passed; ninguna prueba omitida |

La suite completa también pasó localmente en macOS (141 passed). Las pruebas nuevas
cubren la importación sin `fcntl`, el mismo byte de bloqueo en Windows, el plazo ante
contención, la liberación al salir con error y la escritura de recibos sin fsync de
directorios. Las pruebas existentes de hilos y procesos se ejecutaron con el mecanismo
real de cada sistema operativo. No se utilizó el webhook de Teams durante estas pruebas.
