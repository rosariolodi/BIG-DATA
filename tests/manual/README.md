# Pruebas manuales

Complementan a las pruebas automáticas de `tests/`. Sirven para ver el pipeline funcionando con los datos reales y dejar capturas como evidencia. Todos los comandos se ejecutan desde la **raíz** del repositorio, con Python 3.12 (ver `tests/README.md`).

En equipos cuyo nombre contiene guion bajo, antes de cada prueba que use `main.py`:

```powershell
$env:SPARK_LOCAL_IP="127.0.0.1"; $env:SPARK_LOCAL_HOSTNAME="localhost"
```

| # | Prueba | Cómo | Resultado esperado | Resultado obtenido |
|---|---|---|---|---|
| M1 | Pipeline completo | `.venv\Scripts\python.exe src\main.py` | Termina con `PIPELINE COMPLETADO EXITOSAMENTE`; `leídos=5000 válidos=5000 cuarentena=0` en ambas fuentes; tabla `[Tiempos]` con ingestión, transformación y persistencia en estado `ok` | |
| M2 | Resultados correctos | Ver M2 abajo | Las ventas suman **235.445.066**; `ventas_por_dia` tiene **31** filas; BOG-02 tiene **905** transacciones | |
| M3 | Calidad con datos dañados | `.venv\Scripts\python.exe tests\manual\prueba_manual_calidad.py` | Los 13 casos en `[OK ]` y el mensaje `todos los casos se comportaron como se esperaba` | |
| M4 | Idempotencia | Ver M4 abajo | Dos corridas seguidas dejan los mismos archivos y las mismas sumas | |
| M5 | Tiempos y particiones de shuffle | Ver M5 abajo | Con `SHUFFLE_PARTITIONS=8` el total baja de forma marcada frente al valor por defecto | |
| M6 | Pruebas automáticas | `.venv\Scripts\python.exe -m pytest tests -q` | Todas en `passed`, ninguna en `failed` | |

## M2. Resultados correctos
Después de M1:

```powershell
.venv\Scripts\python.exe -c "import pandas as pd; t=pd.read_parquet('data/processed/ventas_por_tienda/resultado.parquet'); print(t); print('TOTAL ventas:', t.ventas_cop.sum()); d=pd.read_parquet('data/processed/ventas_por_dia/resultado.parquet'); print('Dias:', len(d)); print(d.head(3))"
```

Referencia (datos actuales de `data/raw/`): primer día 2026-09-15 con 94 transacciones y $4.364.887; BOG-02 905 transacciones y $44.284.830.

## M4. Idempotencia
```powershell
(Get-ChildItem data\processed -Recurse -File).Count
.venv\Scripts\python.exe -c "import pandas as pd; print(pd.read_parquet('data/processed/ventas_por_tienda/resultado.parquet').ventas_cop.sum())"
```
Anota ambos valores, ejecuta `src\main.py` **otra vez** y repite los dos comandos. Deben coincidir: mismo número de archivos y mismo total (no el doble). En `data\quarantine\` los archivos se reemplazan, no se acumulan.

## M5. Tiempos y particiones de shuffle
Corre `src\main.py` tres veces y anota el `total` de cada `[Tiempos] Corrida ...`:

```powershell
Remove-Item Env:SHUFFLE_PARTITIONS -ErrorAction SilentlyContinue
.venv\Scripts\python.exe src\main.py
$env:SHUFFLE_PARTITIONS="8"
.venv\Scripts\python.exe src\main.py
Remove-Item Env:SHUFFLE_PARTITIONS
.venv\Scripts\python.exe src\main.py
```

Compara las filas con `shuffle_partitions` 200 y 8 en `reports\tiempos\historial_tiempos.csv`. Los tiempos dependen del equipo y de lo que tenga abierto; sirven para comparar entre sí, no como cifra absoluta. La primera subetapa incluye el calentamiento de Spark.

## Qué guardar como evidencia
Captura de la tabla `[Tiempos]` de M1, la salida de M3 y los dos valores de M4. Las corridas que quieras versionar se copian a `reports/tiempos/evidencia/`.
