"""
Idempotencia del pipeline: procesar dos veces el mismo lote da el mismo resultado, sin duplicados
ni totales duplicados (equivale a la prueba P4 del plan de pruebas del informe de la Unidad 2).

Cubre ingestión, validación, limpieza, join, agregaciones y la escritura local de resultados y
cuarentena. La escritura particionada en Delta/Parquet (tarea 4.3) se prueba aparte.
"""
import json

import pytest

from ingestion.reader import ingestar_logs_ecommerce, ingestar_transacciones
from transformations.agregaciones import ejecutar_agregaciones, guardar_resultados
from transformations.cleaning import ejecutar_limpieza_logs, ejecutar_limpieza_transacciones
from transformations.joins import realizar_join_transacciones_logs

CABECERA = "transaction_id,user_id,timestamp,store_id,product_category,amount_cop,payment_method"
TIENDAS = ["BOG-01", "BOG-02", "MED-01", "MED-02", "CAL-01", "BAR-01"]
CATEGORIAS = ["Aseo", "Bebidas", "Carnes", "Frutas"]
PAGOS = ["App", "Bono", "Efectivo", "Tarjeta"]


def _transaccion(i):
    return {"id": f"TXN{10000 + i}", "user": f"usr_{1000 + i % 12}", "ts": f"2026-09-{15 + i % 6} 10:{i % 60:02d}:00",
            "tienda": TIENDAS[i % 6], "categoria": CATEGORIAS[i % 4], "monto": 1000 * (i % 40 + 1), "pago": PAGOS[i % 4]}


def _linea(t, **cambios):
    t = {**t, **cambios}
    return ",".join(str(t[k]) for k in ["id", "user", "ts", "tienda", "categoria", "monto", "pago"])


@pytest.fixture(scope="module")
def lote(tmp_path_factory):
    """Lote pequeño con registros buenos, uno inválido (a cuarentena) y un duplicado (a cuarentena)."""
    base = tmp_path_factory.mktemp("lote")
    buenas = [_transaccion(i) for i in range(60)]
    lineas = [_linea(t) for t in buenas]
    lineas.append(_linea(_transaccion(900), tienda=""))            # incompleto -> cuarentena
    lineas.append(_linea(buenas[5], monto=777))                     # transaction_id repetido -> cuarentena
    (base / "tx.csv").write_text("\n".join([CABECERA] + lineas) + "\n", encoding="utf-8")

    eventos = [{"event_id": f"evt_{10000 + i}", "timestamp": f"2026-09-{15 + i % 6}T15:00:00Z",
                "user_id": f"usr_{1000 + i % 12}", "event_type": "purchase", "device": ["mobile", "desktop"][i % 2],
                "product_category": CATEGORIAS[i % 4], "session_duration_sec": 100 + 10 * i} for i in range(36)]
    eventos.append({**eventos[0], "event_id": "evt_90000", "session_duration_sec": 0})   # duración inválida
    (base / "logs.json").write_text(json.dumps(eventos), encoding="utf-8")
    return {"dir": base, "ventas_validas": sum(t["monto"] for t in buenas), "transacciones_validas": len(buenas)}


def _correr(spark, lote, calidad, salida):
    """Una corrida completa del pipeline sobre el lote; devuelve resultados y métricas comparables."""
    ingesta_tx = ingestar_transacciones(spark, str(lote["dir"] / "tx.csv"), directorio_calidad=str(calidad))
    ingesta_logs = ingestar_logs_ecommerce(spark, str(lote["dir"] / "logs.json"), directorio_calidad=str(calidad))
    df_tx = ejecutar_limpieza_transacciones(spark, ingesta_tx.validos)
    df_logs = ejecutar_limpieza_logs(spark, ingesta_logs.validos)
    resultados = ejecutar_agregaciones(spark, realizar_join_transacciones_logs(df_tx, df_logs))
    guardar_resultados(resultados, str(salida))
    filas = {nombre: sorted(tuple(r) for r in df.collect()) for nombre, df in resultados.items()}
    metricas = {"tx": ingesta_tx.metricas, "logs": ingesta_logs.metricas}
    return filas, metricas


@pytest.fixture(scope="module")
def dos_corridas(spark, lote, tmp_path_factory):
    calidad = tmp_path_factory.mktemp("calidad")
    salida = tmp_path_factory.mktemp("salida")
    primera = _correr(spark, lote, calidad, salida)
    snapshot = {p.relative_to(salida).as_posix(): p.stat().st_size for p in salida.rglob("*") if p.is_file()}
    segunda = _correr(spark, lote, calidad, salida)
    return {"primera": primera, "segunda": segunda, "calidad": calidad, "salida": salida, "snapshot": snapshot}


def test_las_agregaciones_son_identicas_en_las_dos_corridas(dos_corridas):
    assert dos_corridas["primera"][0] == dos_corridas["segunda"][0]


def test_las_metricas_de_calidad_no_cambian_al_reprocesar(dos_corridas):
    for fuente in ("tx", "logs"):
        a, b = dos_corridas["primera"][1][fuente], dos_corridas["segunda"][1][fuente]
        assert (a["leidos"], a["validos"], a["cuarentena"]) == (b["leidos"], b["validos"], b["cuarentena"])
        assert [r["fallos"] for r in a["por_regla"]] == [r["fallos"] for r in b["por_regla"]]


def test_los_totales_no_se_duplican_tras_reprocesar(dos_corridas, lote):
    filas = dos_corridas["segunda"][0]
    ventas = sum(f[1] for f in filas["kpis_por_categoria"])           # total_ventas_cop
    transacciones = sum(f[2] for f in filas["kpis_por_categoria"])    # total_transacciones
    assert ventas == lote["ventas_validas"]
    assert transacciones == lote["transacciones_validas"]


def test_los_archivos_de_salida_se_reemplazan_y_no_se_acumulan(dos_corridas):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    salida = dos_corridas["salida"]
    despues = {p.relative_to(salida).as_posix() for p in salida.rglob("*") if p.is_file()}
    assert despues == set(dos_corridas["snapshot"])                    # mismos archivos, ninguno nuevo
    esperado = {nombre: len(filas) for nombre, filas in dos_corridas["segunda"][0].items()}
    for nombre, n in esperado.items():
        assert len(pd.read_parquet(salida / nombre / "resultado.parquet")) == n


def test_la_cuarentena_se_sobrescribe_en_cada_corrida(dos_corridas):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    calidad = dos_corridas["calidad"]
    cuarentena_tx = pd.read_parquet(calidad / "cuarentena_transacciones.parquet")
    assert len(cuarentena_tx) == 2                                     # el incompleto y el duplicado, no 4
    assert sorted(map(tuple, cuarentena_tx["reglas_fallidas"].map(sorted))) == [("T-C01",), ("T-I02",)]
    assert len(pd.read_parquet(calidad / "cuarentena_logs_ecommerce.parquet")) == 1
    assert json.loads((calidad / "metricas_transacciones.json").read_text(encoding="utf-8"))["cuarentena"] == 2
