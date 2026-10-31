"""
Pruebas del análisis de pushdown sobre Parquet particionado (tarea 5.3): Catalyst envía a la lectura
las columnas, los filtros y las particiones que corresponden, y una UDF de Python lo impide.
"""
import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from optimization.pushdown_parquet import construir_casos, leer_dataset, medir_lectura

FECHAS = ["2026-09-18", "2026-09-19", "2026-09-20", "2026-09-21", "2026-09-22", "2026-09-23"]


@pytest.fixture(scope="module")
def casos(spark, tmp_path_factory):
    """Dataset Parquet particionado por fecha (una partición por día) y los casos de la 5.3 medidos."""
    raiz = tmp_path_factory.mktemp("silver") / "transacciones"
    n = 20
    filas = [(f"TXN{i:05d}", f"usr_{1000 + i % 7}", 1000 * (i % 40 + 1) + (150000 if i % 9 == 0 else 0), fecha)
             for i, fecha in enumerate(f for f in FECHAS for _ in range(n))]
    tabla = pa.table({
        "transaction_id": [f[0] for f in filas], "user_id": [f[1] for f in filas],
        "store_id": ["BOG-01"] * len(filas), "product_category": ["aseo"] * len(filas),
        "payment_method": ["app"] * len(filas),
        "amount_cop": pa.array([f[2] for f in filas], pa.int64()), "fecha": [f[3] for f in filas],
    })
    pq.write_to_dataset(tabla, root_path=str(raiz), partition_cols=["fecha"])

    # sesión aparte: AQE desactivado para poder leer las métricas del escaneo, plan sin truncar
    sesion = spark.newSession()
    sesion.conf.set("spark.sql.adaptive.enabled", "false")
    sesion.conf.set("spark.sql.maxMetadataStringLength", "10000")
    base = leer_dataset(sesion, raiz)
    return {id_: medir_lectura(df, ejecutar=ejecutar) for id_, _, df, ejecutar in construir_casos(base)}


def test_la_referencia_lee_todas_las_columnas_y_todos_los_archivos(casos):
    a = casos["A"]
    assert a["columnas"] == 6                       # sin contar la columna de partición `fecha`
    assert a["archivos"] == len(FECHAS)
    assert a["filas"] == 20 * len(FECHAS)


def test_column_pruning_lee_solo_las_columnas_pedidas(casos):
    b = casos["B"]
    assert b["columnas"] == 2
    assert "user_id" not in b["ReadSchema"] and "store_id" not in b["ReadSchema"]


def test_predicate_pushdown_envia_el_filtro_al_lector_de_parquet(casos):
    assert "GreaterThan(amount_cop,100000)" in casos["C"]["PushedFilters"]


def test_partition_pruning_descarta_carpetas(casos):
    d = casos["D"]
    assert "fecha" in d["PartitionFilters"] and "2026-09-20" in d["PartitionFilters"]
    assert d["archivos"] == 1 and d["particiones"] == 1
    assert d["bytes"] < casos["A"]["bytes"]
    assert d["filas"] == 20


def test_el_filtro_combinado_aplica_particiones_y_pushdown(casos):
    e = casos["E"]
    assert e["archivos"] == 3                       # 20, 21 y 22 de septiembre
    assert "GreaterThan(amount_cop,100000)" in e["PushedFilters"]


def test_una_udf_de_python_impide_el_pushdown(casos):
    f = casos["F"]
    assert "GreaterThan" not in f["PushedFilters"]
    assert "BatchEvalPython" in f["plan"]           # el filtro se evalúa fuera del lector, en Python
