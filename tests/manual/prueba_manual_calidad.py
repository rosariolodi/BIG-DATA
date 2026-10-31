"""
Prueba manual de la validación de calidad: lee una COPIA de los datos reales a la que se le agregan
registros dañados a propósito, y muestra qué va a cuarentena y qué queda como advertencia.
No modifica data/raw ni escribe en el repositorio (todo va a una carpeta temporal).

Ejecutar desde la raíz del repositorio:
    $env:SPARK_LOCAL_IP="127.0.0.1"; $env:SPARK_LOCAL_HOSTNAME="localhost"   # solo si el equipo tiene "_" en el nombre
    .venv\\Scripts\\python.exe tests\\manual\\prueba_manual_calidad.py
"""
import csv
import json
import os
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(RAIZ / "src"))
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

from ingestion.reader import ingestar_logs_ecommerce, ingestar_transacciones

CATEGORIAS_SANAS = {"Aseo", "Bebidas", "Carnes", "Frutas", "Granos", "Licores", "Snacks", "Verduras"}

# (descripción, cambios sobre una fila buena, IDs de regla esperados). "ADVERTENCIA" = pasa marcada.
CASOS_CSV = [
    ("store_id vacío", {"store_id": ""}, ["T-C01"]),
    ("transaction_id mal formado", {"transaction_id": "TXN1"}, ["T-I01"]),
    ("monto negativo", {"amount_cop": "-50"}, ["T-I05"]),
    ("monto no numérico", {"amount_cop": "abc"}, ["T-C01", "T-I06"]),
    ("fecha inválida", {"timestamp": "no-fecha"}, ["T-C01", "T-I03", "T-I06"]),
    ("user_id mal formado", {"user_id": "usr_12"}, ["T-I07"]),
    ("tienda, categoría y pago nuevos", {"store_id": "ZZZ-99", "product_category": "Juguetes", "payment_method": "Crypto"},
     "ADVERTENCIA"),
    ("monto extremo", {"amount_cop": "9999999"}, "ADVERTENCIA"),
]
CASOS_JSON = [
    ("user_id nulo", {"user_id": None}, ["L-C01"]),
    ("event_id mal formado", {"event_id": "evt_1"}, ["L-I01"]),
    ("duración 0", {"session_duration_sec": 0}, ["L-I05"]),
    ("fecha inválida (no debe tumbar los demás)", {"timestamp": "ayer"}, ["L-C01", "L-I03"]),
    ("dispositivo nuevo", {"device": "tv"}, "ADVERTENCIA"),
]


def main():
    spark = (SparkSession.builder.master("local[2]").appName("prueba-manual-calidad")
             .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    tmp = Path(tempfile.mkdtemp(prefix="prueba_manual_"))

    # --- transacciones: 100 filas reales + una fila dañada por caso ---
    with open(RAIZ / "data" / "raw" / "DATAMARCA_transacciones_5000_real.csv", encoding="utf-8-sig", newline="") as f:
        filas = list(csv.DictReader(f))
    campos = list(filas[0])
    sal = [dict(x) for x in filas[:100]]
    # fila base con una categoría bien escrita, para que cada caso pruebe solo su propio defecto
    base = next(x for x in filas[200:] if x["product_category"] in CATEGORIAS_SANAS)
    for i, (_, cambios, _) in enumerate(CASOS_CSV):
        malo = dict(base, transaction_id=f"TXN9{i:04d}", amount_cop="20000")
        malo.update(cambios)
        sal.append(malo)
    ruta_csv = tmp / "tx_prueba.csv"
    with open(ruta_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(sal)

    print("=" * 70, "\nTRANSACCIONES (100 reales + %d dañadas a propósito)" % len(CASOS_CSV))
    res = ingestar_transacciones(spark, str(ruta_csv), directorio_calidad=str(tmp / "calidad"))
    en_cuarentena = {json.loads(r["registro_original"]).get("transaction_id"): sorted(r["reglas_fallidas"])
                     for r in res.cuarentena.collect()}
    con_aviso = {r["transaction_id"]: sorted(r["dq_warnings"])
                 for r in res.validos.filter("size(dq_warnings) > 0").collect() if r["transaction_id"].startswith("TXN9")}
    revisar(CASOS_CSV, "TXN9{:04d}", "transaction_id", en_cuarentena, con_aviso)

    # --- logs: 50 eventos reales + uno dañado por caso ---
    with open(RAIZ / "data" / "raw" / "ecommerce_logs_5000.json", encoding="utf-8-sig") as f:
        eventos = json.load(f)
    sal = list(eventos[:50])
    for i, (_, cambios, _) in enumerate(CASOS_JSON):
        malo = dict(eventos[100], event_id=f"evt_9{i:04d}")
        malo.update(cambios)
        sal.append(malo)
    ruta_json = tmp / "logs_prueba.json"
    ruta_json.write_text(json.dumps(sal, ensure_ascii=False), encoding="utf-8")

    print("=" * 70, "\nLOGS (50 reales + %d dañados a propósito)" % len(CASOS_JSON))
    res = ingestar_logs_ecommerce(spark, str(ruta_json), directorio_calidad=str(tmp / "calidad"))
    en_cuarentena = {json.loads(r["registro_original"]).get("event_id"): sorted(r["reglas_fallidas"])
                     for r in res.cuarentena.collect()}
    con_aviso = {r["event_id"]: sorted(r["dq_warnings"])
                 for r in res.validos.filter("size(dq_warnings) > 0").collect() if r["event_id"].startswith("evt_9")}
    revisar(CASOS_JSON, "evt_9{:04d}", "event_id", en_cuarentena, con_aviso)

    print("\nArchivos de calidad generados en:", tmp / "calidad")
    spark.stop()


def revisar(casos, patron_id, campo_id, en_cuarentena, con_aviso):
    fallos = 0
    for i, (descripcion, cambios, esperado) in enumerate(casos):
        id_ = cambios.get(campo_id, patron_id.format(i))   # si el caso cambia el propio ID, se busca ese
        if esperado == "ADVERTENCIA":
            obtenido = "ADVERTENCIA " + str(con_aviso.get(id_)) if id_ in con_aviso and id_ not in en_cuarentena else str(en_cuarentena.get(id_))
            ok = id_ in con_aviso and id_ not in en_cuarentena
        else:
            obtenido = str(en_cuarentena.get(id_))
            ok = en_cuarentena.get(id_) == esperado
        fallos += not ok
        print(f"  [{'OK ' if ok else 'MAL'}] {descripcion:<45} -> {obtenido}")
    print("  RESULTADO:", "todos los casos se comportaron como se esperaba" if not fallos else f"{fallos} caso(s) NO esperados")


if __name__ == "__main__":
    main()
