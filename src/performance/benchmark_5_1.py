#!/usr/bin/env python3
"""DATAMARCA - punto 5.1: benchmark local aislado, sin operaciones Git.

Uso desde la raiz del repositorio:
  python3 /ruta/benchmark_5_1.py --preflight
  python3 /ruta/benchmark_5_1.py --sizes 5000 10000 --repeats 1
  python3 /ruta/benchmark_5_1.py --sizes 5000 10000 25000 50000 90000 --repeats 2

Datos de entrada del repositorio se leen solamente. Archivos temporales se escriben
fuera del repositorio y se eliminan al terminar. El CSV de resultados se escribe
fuera del repositorio (por defecto ~/benchmark_datamarca_5_1.csv).
"""
import argparse
import csv
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path.cwd().resolve()
SRC = ROOT / 'src'
CSV_ORIGINAL = ROOT / 'data/raw/DATAMARCA_transacciones_5000_real.csv'
JSON_ORIGINAL = ROOT / 'data/raw/ecommerce_logs_5000.json'


def preflight():
    print('Repositorio:', ROOT)
    print('Python:', sys.version.split()[0], '| SO:', platform.platform())
    print('CSV:', CSV_ORIGINAL.exists(), '| JSON:', JSON_ORIGINAL.exists())
    print('Salida por defecto:', Path.home() / 'benchmark_datamarca_5_1.csv')
    if not CSV_ORIGINAL.is_file() or not JSON_ORIGINAL.is_file():
        raise SystemExit('ERROR: Ejecuta el script desde la raiz del proyecto BIG-DATA.')
    try:
        import pyspark
        print('PySpark:', pyspark.__version__)
    except ImportError:
        print('PySpark: NO INSTALADO en este Python')
    try:
        import pyarrow
        print('PyArrow:', pyarrow.__version__)
    except ImportError:
        print('PyArrow: NO INSTALADO (puede ser necesario para la persistencia)')
    print('Preflight terminado: no se ejecutaron pruebas ni se escribieron archivos.')


def load_templates():
    with CSV_ORIGINAL.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        tx = list(reader)
        cols = reader.fieldnames
    with JSON_ORIGINAL.open(encoding='utf-8-sig') as f:
        logs = json.load(f)
    if not tx or not isinstance(logs, list) or not logs:
        raise ValueError('Archivos de entrada vacios o JSON no es un array.')
    for col in ('transaction_id', 'user_id', 'timestamp', 'store_id', 'product_category', 'amount_cop', 'payment_method'):
        if col not in cols:
            raise ValueError('Columna CSV ausente: ' + col)
    if not isinstance(logs[0], dict) or 'event_id' not in logs[0]:
        raise ValueError('Formato JSON inesperado.')
    return tx, cols, logs


def generate_sources(size, tx_templates, cols, log_templates, work):
    """IDs nuevos y unicos por fuente; user_id se conserva para mantener cruces."""
    folder = work / ('input_' + str(size))
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / 'transacciones.csv'
    json_path = folder / 'logs.json'
    with csv_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for i in range(size):
            row = dict(tx_templates[i % len(tx_templates)])
            row['transaction_id'] = f'TXN{i:05d}'
            writer.writerow(row)
    # multiLine=true: el lector existente acepta un array JSON, no JSON Lines.
    with json_path.open('w', encoding='utf-8') as f:
        f.write('[')
        for i in range(size):
            if i:
                f.write(',')
            event = dict(log_templates[i % len(log_templates)])
            event['event_id'] = f'evt_{i:05d}'
            json.dump(event, f, ensure_ascii=False, separators=(',', ':'))
        f.write(']')
    return csv_path, json_path


def clean_tx(df):
    from pyspark.sql.functions import col, lower, trim, upper, when, lit
    from transformations.cleaning import _filtrar_fechas_validas
    df = (df.dropna(subset=['transaction_id', 'user_id', 'store_id', 'amount_cop'])
          .withColumn('user_id', lower(trim(col('user_id'))))
          .withColumn('store_id', upper(trim(col('store_id'))))
          .withColumn('product_category', lower(trim(col('product_category'))))
          .withColumn('payment_method', lower(trim(col('payment_method'))))
          .withColumn('payment_method', when(col('payment_method').isNull() | (col('payment_method') == ''),
                                             lit('desconocido')).otherwise(col('payment_method'))))
    return _filtrar_fechas_validas(df)


def clean_logs(df):
    from pyspark.sql.functions import col, lower, trim, when, lit
    from transformations.cleaning import _filtrar_fechas_validas
    df = (df.dropna(subset=['event_id', 'user_id', 'timestamp'])
          .withColumn('user_id', lower(trim(col('user_id'))))
          .withColumn('device', lower(trim(col('device'))))
          .withColumn('product_category', lower(trim(col('product_category'))))
          .withColumn('event_type', lower(trim(col('event_type'))))
          .withColumn('session_duration_sec', when(col('session_duration_sec').isNull(), lit(0))
                      .otherwise(col('session_duration_sec'))))
    return _filtrar_fechas_validas(df)


def run_once(spark, size, repetition, tx_path, log_path, work):
    from pyspark.sql import functions as F
    from ingestion.reader import ingestar_transacciones, ingestar_logs_ecommerce
    from transformations.joins import realizar_join_transacciones_logs, generar_agregaciones_negocio
    from transformations.agregaciones import ejecutar_agregaciones
    from persistence.writer import persistir_silver, persistir_gold

    start = time.perf_counter()
    stage = {}
    tx = logs = joined = None
    try:
        t = time.perf_counter()
        result_tx = ingestar_transacciones(spark, str(tx_path), directorio_calidad=None)
        result_logs = ingestar_logs_ecommerce(spark, str(log_path), directorio_calidad=None)
        stage['ingestion_s'] = time.perf_counter() - t

        t = time.perf_counter()
        tx = clean_tx(result_tx.validos).cache()
        logs = clean_logs(result_logs.validos).cache()
        # Acciones: fuerzan la ejecucion real de la limpieza.
        tx_stats = tx.agg(F.count('*').alias('n'), F.sum('amount_cop').alias('amount')).first()
        log_n = logs.count()
        stage['cleaning_s'] = time.perf_counter() - t
        tx_n = int(tx_stats['n'])
        tx_amount = int(tx_stats['amount'] or 0)

        root = str(work / ('output_%s_rep_%s' % (size, repetition)))
        t = time.perf_counter()
        persistir_silver(tx, logs, 'parquet', root)
        stage['silver_s'] = time.perf_counter() - t

        t = time.perf_counter()
        joined = realizar_join_transacciones_logs(tx, logs).cache()
        # Cache materializada para evaluar el join, incluidas columnas de logs.
        joined_stats = joined.agg(F.count('*').alias('n'),
                                  F.sum('amount_cop').alias('amount'),
                                  F.sum('total_eventos_web').alias('events')).first()
        stage['join_s'] = time.perf_counter() - t
        joined_n = int(joined_stats['n'])
        joined_amount = int(joined_stats['amount'] or 0)
        if joined_n != tx_n or joined_amount != tx_amount:
            raise AssertionError('El join cambio el numero de transacciones o sus montos.')

        t = time.perf_counter()
        business = generar_agregaciones_negocio(joined)
        aggregations = ejecutar_agregaciones(spark, joined)
        # Forzar calculo de todas las salidas; persist_gold las materializa.
        persistir_gold({'kpis_negocio_categoria': business, **aggregations}, 'parquet', root)
        stage['gold_s'] = time.perf_counter() - t

        total = time.perf_counter() - start
        record = {
            'size_per_source': size, 'repeat': repetition,
            'input_tx_rows': result_tx.metricas['leidos'],
            'valid_tx_rows': result_tx.metricas['validos'],
            'input_log_rows': result_logs.metricas['leidos'],
            'valid_log_rows': result_logs.metricas['validos'],
            'clean_tx_rows': tx_n, 'clean_log_rows': int(log_n),
            'joined_rows': joined_n, 'amount_tx': tx_amount, 'amount_join': joined_amount,
            'integrity_ok': True, 'total_s': round(total, 3),
            'tx_rows_per_second': round(tx_n / total, 2) if total else 0,
            **{key: round(value, 3) for key, value in stage.items()},
            'status': 'ok', 'error': ''
        }
        return record
    finally:
        # No dejar caches de pruebas anteriores influyendo en las siguientes.
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        for frame in (joined, tx, logs):
            if frame is not None:
                try:
                    frame.unpersist(blocking=True)
                except Exception:
                    pass


def write_csv(rows, output):
    if not rows:
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    p = argparse.ArgumentParser(description='Benchmark 5.1 de DATAMARCA, aislado de Git')
    p.add_argument('--preflight', action='store_true', help='solo consulta el entorno')
    p.add_argument('--sizes', nargs='+', type=int, default=[5000, 10000, 25000, 50000, 90000])
    p.add_argument('--repeats', type=int, default=2)
    p.add_argument('--output', type=Path, default=Path.home() / 'benchmark_datamarca_5_1.csv')
    p.add_argument('--local-cores', type=int, default=2)
    args = p.parse_args()
    if args.preflight:
        preflight()
        return
    if not SRC.is_dir() or not CSV_ORIGINAL.is_file() or not JSON_ORIGINAL.is_file():
        raise SystemExit('Ejecuta desde la raiz de BIG-DATA.')
    if any(s < 1 or s > 100000 for s in args.sizes):
        raise SystemExit('Los tamanos deben estar entre 1 y 100000 (IDs de 5 digitos).')
    if args.repeats < 1:
        raise SystemExit('Se necesita al menos una repeticion.')
    if args.output.resolve() == ROOT or ROOT in args.output.resolve().parents:
        raise SystemExit('Por seguridad, el CSV de salida debe estar FUERA del repositorio.')
    sys.path.insert(0, str(SRC))
    from pyspark.sql import SparkSession
    # Sesion local Parquet: no se necesitan conectores Azure ni Delta.

    tx_templates, cols, log_templates = load_templates()
    builder = (SparkSession.builder.master(f'local[{args.local_cores}]')
               .appName('DATAMARCA-Benchmark-5-1')
               .config('spark.sql.session.timeZone', 'America/Bogota')
               .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
               .config('spark.sql.shuffle.partitions', os.environ.get('SHUFFLE_PARTITIONS','8'))
               .config('spark.ui.enabled', 'false'))
    # No utilizar credenciales ni rutas Azure en el benchmark.
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print('Spark:', spark.version, '| master:', spark.sparkContext.master)
    rows = []
    try:
        with tempfile.TemporaryDirectory(prefix='datamarca_5_1_') as folder:
            work = Path(folder)
            for size in args.sizes:
                csv_path, json_path = generate_sources(size, tx_templates, cols, log_templates, work)
                for repetition in range(1, args.repeats + 1):
                    print(f'\n===== PRUEBA: {size} filas por fuente; repeticion {repetition} =====', flush=True)
                    try:
                        record = run_once(spark, size, repetition, csv_path, json_path, work)
                    except Exception as e:
                        print('FALLO:', repr(e), file=sys.stderr)
                        # Guardar las pruebas previas para diagnostico.
                        if rows:
                            write_csv(rows, args.output)
                        raise
                    rows.append(record)
                    write_csv(rows, args.output)
                    print('RESULTADO:', record['total_s'], 'segundos; integridad:', record['integrity_ok'])
        print('\nRESULTADOS GUARDADOS EN:', args.output)
        print('Resumen (mediana de tiempo por volumen):')
        for size in args.sizes:
            vals = [r['total_s'] for r in rows if r['size_per_source'] == size]
            print(f'  {size:>6} por fuente: {statistics.median(vals):.3f} s (n={len(vals)})')
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
