"""Pruebas de las agregaciones de negocio (tarea 4.2) con un conjunto pequeño calculado a mano."""
import pytest
from pyspark.sql import functions as F

from transformations.agregaciones import (
    ejecutar_agregaciones,
    guardar_resultados,
    kpis_por_categoria_sql,
    registrar_vista,
    top_clientes_sql,
    ventas_por_dia_sql,
    ventas_por_metodo_pago,
    ventas_por_tienda,
)
from transformations.joins import generar_agregaciones_negocio

COLUMNAS = ["transaction_id", "user_id", "timestamp", "store_id", "product_category", "amount_cop",
            "payment_method", "total_eventos_web", "promedio_duracion_sesion_sec", "tiene_actividad_web"]

# Total de ventas: 210.000 en 6 transacciones. usr_3 no tiene actividad web.
FILAS = [
    ("T1", "usr_1", "2026-09-15 08:00:00", "BOG-01", "aseo",   10000, "tarjeta",  2, 100.0, True),
    ("T2", "usr_1", "2026-09-15 23:30:00", "BOG-01", "snacks", 20000, "tarjeta",  2, 100.0, True),
    ("T3", "usr_2", "2026-09-16 10:00:00", "MED-01", "aseo",   30000, "efectivo", 4, 200.0, True),
    ("T4", "usr_3", "2026-09-16 12:00:00", "BAR-01", "aseo",   40000, "app",      None, None, False),
    ("T5", "usr_2", "2026-09-16 13:00:00", "MED-01", "snacks", 50000, "app",      4, 200.0, True),
    ("T6", "usr_4", "2026-09-17 09:00:00", "CAL-01", "snacks", 60000, "bono",     6, 300.0, True),
]


def _literal_sql(valor, es_fecha=False):
    if valor is None:
        return "NULL"
    if es_fecha:
        return f"TIMESTAMP '{valor}'"      # se interpreta en la zona horaria de la sesión (Bogotá)
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, int):
        return f"{valor}L"
    if isinstance(valor, float):
        return f"{valor}D"
    return f"'{valor}'"


@pytest.fixture(scope="module")
def df_joined(spark):
    # Se construye con SQL (VALUES) y no con createDataFrame para no depender de workers de Python
    filas_sql = ", ".join(
        "(" + ", ".join(_literal_sql(v, es_fecha=(c == "timestamp")) for c, v in zip(COLUMNAS, fila)) + ")"
        for fila in FILAS)
    return spark.sql(f"SELECT * FROM VALUES {filas_sql} AS t({', '.join(COLUMNAS)})").cache()


def filas_por(df, clave):
    return {r[clave]: r.asDict() for r in df.collect()}


def test_ventas_por_tienda(df_joined):
    res = ventas_por_tienda(df_joined).collect()
    assert [r["store_id"] for r in res] == ["MED-01", "CAL-01", "BAR-01", "BOG-01"]  # por ventas desc
    por_tienda = {r["store_id"]: r for r in res}
    assert por_tienda["MED-01"]["ventas_cop"] == 80000 and por_tienda["MED-01"]["ciudad"] == "Medellín"
    assert por_tienda["BOG-01"]["ventas_cop"] == 30000 and por_tienda["BOG-01"]["ciudad"] == "Bogotá"
    assert por_tienda["BAR-01"]["ciudad"] == "Barranquilla" and por_tienda["CAL-01"]["ciudad"] == "Cali"
    assert por_tienda["MED-01"]["pct_ventas"] == pytest.approx(38.10, abs=0.01)
    assert sum(r["pct_ventas"] for r in res) == pytest.approx(100, abs=0.1)
    assert sum(r["ventas_cop"] for r in res) == 210000


def test_ventas_por_metodo_pago(df_joined):
    res = filas_por(ventas_por_metodo_pago(df_joined), "payment_method")
    assert {k: v["ventas_cop"] for k, v in res.items()} == {"app": 90000, "bono": 60000, "tarjeta": 30000, "efectivo": 30000}
    assert res["app"]["transacciones"] == 2
    assert res["app"]["pct_ventas"] == pytest.approx(42.86, abs=0.01)
    assert sum(v["pct_transacciones"] for v in res.values()) == pytest.approx(100, abs=0.1)


def test_ventas_por_dia_respeta_la_hora_de_bogota(spark, df_joined):
    registrar_vista(df_joined)
    res = ventas_por_dia_sql(spark).collect()
    resumen = [(str(r["fecha"]), r["transacciones"], r["ventas_cop"], r["ventas_acumuladas_cop"]) for r in res]
    # T2 (23:30 hora de Bogotá) debe quedar en el día 15, no desplazarse al 16
    assert resumen == [("2026-09-15", 2, 30000, 30000),
                       ("2026-09-16", 3, 120000, 150000),
                       ("2026-09-17", 1, 60000, 210000)]


def test_top_clientes(spark, df_joined):
    registrar_vista(df_joined)
    res = top_clientes_sql(spark, n=10).collect()
    assert [(r["ranking"], r["user_id"], r["ventas_cop"]) for r in res] == [
        (1, "usr_2", 80000), (2, "usr_4", 60000), (3, "usr_3", 40000), (4, "usr_1", 30000)]
    assert [r["user_id"] for r in top_clientes_sql(spark, n=2).collect()] == ["usr_2", "usr_4"]


def test_kpis_por_categoria_valores_calculados_a_mano(spark, df_joined):
    registrar_vista(df_joined)
    res = filas_por(kpis_por_categoria_sql(spark), "product_category")
    aseo, snacks = res["aseo"], res["snacks"]
    assert (aseo["total_ventas_cop"], aseo["total_transacciones"], aseo["usuarios_unicos"]) == (80000, 3, 3)
    assert aseo["total_eventos_web"] == 6                      # usr_3 no tiene actividad web: no suma
    assert aseo["pct_usuarios_con_actividad_web"] == pytest.approx(66.67, abs=0.01)
    assert (snacks["total_ventas_cop"], snacks["total_transacciones"], snacks["usuarios_unicos"]) == (130000, 3, 3)
    assert snacks["pct_usuarios_con_actividad_web"] == 100.0
    assert sum(r["total_ventas_cop"] for r in res.values()) == 210000   # el join no infla las ventas


def test_sparksql_coincide_con_la_api_de_dataframes(spark, df_joined):
    registrar_vista(df_joined)
    sql = filas_por(kpis_por_categoria_sql(spark), "product_category")
    api = filas_por(generar_agregaciones_negocio(df_joined), "product_category")
    assert sql.keys() == api.keys()
    for categoria, esperado in api.items():
        for columna, valor in esperado.items():
            obtenido = sql[categoria][columna]
            if isinstance(valor, float) or isinstance(obtenido, float):
                assert obtenido == pytest.approx(valor, abs=0.011), (categoria, columna)
            else:
                assert obtenido == valor, (categoria, columna)


def test_guardar_resultados_escribe_un_parquet_por_agregacion(spark, df_joined, tmp_path):
    pytest.importorskip("pyarrow")
    pytest.importorskip("pandas")
    resultados = ejecutar_agregaciones(spark, df_joined)
    guardar_resultados(resultados, str(tmp_path))
    esperados = {"kpis_por_categoria", "ventas_por_dia", "top_clientes", "ventas_por_tienda", "ventas_por_metodo_pago"}
    assert {p.name for p in tmp_path.iterdir()} == esperados
    assert all((tmp_path / n / "resultado.parquet").exists() for n in esperados)
