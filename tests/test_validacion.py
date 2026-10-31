"""Pruebas de la validación de calidad (tarea 2.2): reglas de docs/reglas_calidad.md."""
import json

import pytest

from ingestion.reader import leer_logs_ecommerce_json_crudo, leer_transacciones_csv_crudo
from ingestion.validacion import validar_logs, validar_transacciones

CABECERA_CSV = "transaction_id,user_id,timestamp,store_id,product_category,amount_cop,payment_method"


# ---------------------------------------------------------------------------
# Datos de prueba
# ---------------------------------------------------------------------------
def fila_tx(i, **cambios):
    base = {
        "transaction_id": f"TXN{10000 + i}",
        "user_id": "usr_1000",
        "timestamp": "2026-09-15 10:00:00",
        "store_id": "BOG-01",
        "product_category": "Aseo",
        "amount_cop": str(1000 * (i % 40 + 1)),  # entre 1.000 y 40.000: sin montos extremos
        "payment_method": "App",
    }
    base.update({k: str(v) for k, v in cambios.items()})
    return ",".join(base.values())


def evento(i, **cambios):
    base = {
        "event_id": f"evt_{10000 + i}",
        "timestamp": "2026-09-15T10:00:00Z",
        "user_id": "usr_1000",
        "event_type": "purchase",
        "device": "mobile",
        "product_category": "Aseo",
        "session_duration_sec": 300,
    }
    base.update(cambios)
    return base


def validar_csv(spark, tmp_path, filas):
    ruta = tmp_path / "tx.csv"
    ruta.write_text("\n".join([CABECERA_CSV] + filas) + "\n", encoding="utf-8")
    return validar_transacciones(leer_transacciones_csv_crudo(spark, str(ruta)))


def validar_json(spark, tmp_path, eventos):
    ruta = tmp_path / "logs.json"
    ruta.write_text(json.dumps(eventos, ensure_ascii=False, indent=2), encoding="utf-8")
    return validar_logs(leer_logs_ecommerce_json_crudo(spark, str(ruta)))


def cuarentena_por(res, clave):
    return {json.loads(f["registro_original"]).get(clave): sorted(f["reglas_fallidas"])
            for f in res.cuarentena.collect()}


def advertencias_por(res, clave):
    return {f[clave]: sorted(f["dq_warnings"]) for f in res.validos.filter("size(dq_warnings) > 0").collect()}


BASE_TX = [fila_tx(i) for i in range(40)]  # montos de 1.000 a 40.000, sin outliers
BASE_LOGS = [evento(i) for i in range(20)]


# ---------------------------------------------------------------------------
# Transacciones
# ---------------------------------------------------------------------------
def test_transacciones_validas_no_se_rechazan(spark, tmp_path):
    res = validar_csv(spark, tmp_path, BASE_TX)
    assert res.metricas["leidos"] == 40
    assert res.metricas["cuarentena"] == 0
    assert res.validos.count() == 40
    assert "dq_warnings" in res.validos.columns
    assert "_corrupt_record" not in res.validos.columns


@pytest.mark.parametrize("cambio, reglas_esperadas", [
    ({"store_id": ""}, ["T-C01"]),
    ({"transaction_id": "TXN1"}, ["T-I01"]),
    ({"amount_cop": "-50"}, ["T-I05"]),
    ({"amount_cop": "abc"}, ["T-C01", "T-I06"]),
    ({"timestamp": "no-fecha"}, ["T-C01", "T-I03", "T-I06"]),
    ({"store_id": "BOGOTA"}, ["T-I04"]),
    ({"user_id": "usr_12"}, ["T-I07"]),
])
def test_cada_regla_critica_rechaza_a_cuarentena(spark, tmp_path, cambio, reglas_esperadas):
    res = validar_csv(spark, tmp_path, BASE_TX + [fila_tx(900, **cambio)])
    q = cuarentena_por(res, "transaction_id")
    assert res.metricas["cuarentena"] == 1
    assert res.validos.count() == 40
    assert list(q.values()) == [reglas_esperadas]


def test_transaccion_duplicada_conserva_la_primera(spark, tmp_path):
    duplicada = fila_tx(5, amount_cop=777)  # mismo transaction_id que BASE_TX[5], otro monto
    res = validar_csv(spark, tmp_path, BASE_TX + [duplicada])
    registros = [json.loads(f["registro_original"]) for f in res.cuarentena.collect()]
    assert [(r["transaction_id"], r["amount_cop"]) for r in registros] == [("TXN10005", 777)]
    assert cuarentena_por(res, "transaction_id") == {"TXN10005": ["T-I02"]}
    assert res.validos.filter("transaction_id = 'TXN10005' AND amount_cop = 6000").count() == 1


def test_un_registro_con_varios_fallos_lista_todos_los_ids(spark, tmp_path):
    res = validar_csv(spark, tmp_path, BASE_TX + [fila_tx(900, store_id="", amount_cop="-50")])
    assert list(cuarentena_por(res, "transaction_id").values()) == [["T-C01", "T-I05"]]


def test_dominios_nuevos_son_advertencia_y_no_rechazo(spark, tmp_path):
    nuevo = fila_tx(900, store_id="ZZZ-99", product_category="Juguetes", payment_method="Crypto")
    res = validar_csv(spark, tmp_path, BASE_TX + [nuevo])
    assert res.metricas["cuarentena"] == 0
    assert advertencias_por(res, "transaction_id") == {"TXN10900": ["T-D01", "T-D02", "T-D03"]}


def test_monto_extremo_es_advertencia(spark, tmp_path):
    res = validar_csv(spark, tmp_path, BASE_TX + [fila_tx(900, amount_cop=9999999)])
    assert res.metricas["cuarentena"] == 0
    assert advertencias_por(res, "transaction_id") == {"TXN10900": ["T-D04"]}


def test_metricas_de_rechazo(spark, tmp_path):
    res = validar_csv(spark, tmp_path, BASE_TX + [fila_tx(900, amount_cop="-50")])
    m = res.metricas
    assert (m["leidos"], m["validos"], m["cuarentena"]) == (41, 40, 1)
    assert m["pct_rechazo"] == pytest.approx(100 / 41, abs=1e-3)
    por_regla = {r["id"]: r for r in m["por_regla"]}
    assert por_regla["T-I05"]["fallos"] == 1
    assert por_regla["T-I01"]["fallos"] == 0
    assert por_regla["T-I05"]["severidad"] == "critica"
    assert por_regla["T-D01"]["severidad"] == "advertencia"


# ---------------------------------------------------------------------------
# Logs de e-commerce
# ---------------------------------------------------------------------------
def test_logs_validos_no_se_rechazan(spark, tmp_path):
    res = validar_json(spark, tmp_path, BASE_LOGS)
    assert res.metricas["leidos"] == 20
    assert res.metricas["cuarentena"] == 0
    assert str(res.validos.schema["timestamp"].dataType) == "TimestampType()"
    assert str(res.validos.schema["session_duration_sec"].dataType) == "IntegerType()"


@pytest.mark.parametrize("cambio, reglas_esperadas", [
    ({"event_id": "evt_1"}, ["L-I01"]),
    ({"user_id": None}, ["L-C01"]),
    ({"user_id": "usr_12"}, ["L-I04"]),
    ({"session_duration_sec": 0}, ["L-I05"]),
    ({"timestamp": "ayer"}, ["L-C01", "L-I03"]),
])
def test_cada_regla_critica_de_logs_rechaza(spark, tmp_path, cambio, reglas_esperadas):
    res = validar_json(spark, tmp_path, BASE_LOGS + [evento(900, **cambio)])
    assert res.metricas["cuarentena"] == 1
    assert res.validos.count() == 20
    assert list(cuarentena_por(res, "event_id").values()) == [reglas_esperadas]


def test_valor_mal_tipado_solo_rechaza_ese_registro(spark, tmp_path):
    """Regresión: en un JSON array, un valor mal tipado no debe marcar todo el archivo como corrupto."""
    res = validar_json(spark, tmp_path, BASE_LOGS + [evento(900, timestamp="ayer")])
    assert (res.metricas["leidos"], res.metricas["validos"], res.metricas["cuarentena"]) == (21, 20, 1)


def test_evento_duplicado_va_a_cuarentena(spark, tmp_path):
    res = validar_json(spark, tmp_path, BASE_LOGS + [evento(3, session_duration_sec=999)])
    assert cuarentena_por(res, "event_id") == {"evt_10003": ["L-I02"]}


def test_dominios_y_rango_de_logs_son_advertencia(spark, tmp_path):
    extra = [evento(900, device="tv"), evento(901, product_category="Juguetes"),
             evento(902, session_duration_sec=5000), evento(903, event_type="login")]
    res = validar_json(spark, tmp_path, BASE_LOGS + extra)
    assert res.metricas["cuarentena"] == 0
    assert advertencias_por(res, "event_id") == {
        "evt_10900": ["L-D02"], "evt_10901": ["L-D03"], "evt_10902": ["L-D04"], "evt_10903": ["L-D01"],
    }
