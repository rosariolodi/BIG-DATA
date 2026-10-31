"""
Estimación de costos operativos en Azure (tarea 6.1)
DATAMARCA S.A.

Calcula un escenario optimista y uno pesimista del costo mensual del pipeline batch
(Azure Databricks + ADLS Gen2) en la región Mexico Central, con precios oficiales de Azure
y los supuestos documentados en docs/costos.md. Desglosa el costo por servicio, por capa del
lago y por etapa del pipeline. Solo usa la librería estándar y solo imprime:

    python reports/costos/estimacion_costos.py
    python reports/costos/estimacion_costos.py ruta/a/tiempos_<corrida>.json   # otra corrida

Para actualizar la estimación basta con cambiar las constantes de precios o los escenarios.
"""
import json
import sys
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Precios oficiales (USD, pago por uso), región Mexico Central, consultados el 2026-10-30 en
# la Azure Retail Prices API (prices.azure.com) y verificados en la Azure Pricing Calculator.
# ---------------------------------------------------------------------------
PRECIO_DBU = {                      # Azure Databricks Premium, USD por DBU-hora
    "jobs": 0.30,                   # Jobs Compute (job cluster)
    "all_purpose": 0.55,            # All-purpose Compute (cluster interactivo)
}
DBU_NODO_HORA = 0.75                # Standard_DS3_v2 (4 vCPU, 14 GB): 0,75 DBU por hora (calculadora oficial)
PRECIO_VM = 0.251                   # Standard_DS3_v2 Linux, USD por hora
PRECIO_VM_SPOT = 0.05635            # Standard_DS3_v2 Linux spot, USD por hora (precio variable)
ALMACENAMIENTO = [                  # ADLS Gen2 (Hierarchical Namespace) Hot LRS, USD por GB-mes
    (51_200, 0.02288),              # primeros 50 TB
    (512_000, 0.021965),            # siguientes 450 TB
    (float("inf"), 0.02105),
]
ESCRITURA_10K = 0.0715              # Hot LRS Write Operations, USD por 10.000
LECTURA_10K = 0.00572               # Hot Read Operations, USD por 10.000
MIB_POR_OPERACION = 4               # ADLS Gen2 cuenta las operaciones por bloques de 4 MiB

# ---------------------------------------------------------------------------
# Supuestos del caso (informe de la Unidad 2) y medidas del repositorio
# ---------------------------------------------------------------------------
TECHO_MENSUAL = 20_000              # USD, restricción no negociable
TX_DIA = 50_000_000                 # transacciones diarias promedio
FACTOR_PICO = 3                     # pico estacional
DIAS_NORMALES, DIAS_PICO = 24, 6    # mes ponderado del informe de la Unidad 2
RETENCION_MESES = 12                # política de retención del lago

# Medido en el repositorio (5.000 transacciones + 5.000 eventos):
BYTES_CRUDO = 332_697 + 1_137_605   # data/raw: CSV + JSON (capa bronze)
BYTES_SILVER = 207_522 + 198_185    # data/processed/silver en Parquet
BYTES_GOLD = 19_951                 # data/processed/gold en Parquet
KB_POR_TX_MEDIDO = BYTES_CRUDO / 5_000 / 1_000

# Corrida de referencia para repartir el cómputo por etapa: pipeline completo con la persistencia
# particionada de la tarea 4.3 (Colab, 2 núcleos, 200 particiones de shuffle; registro de la tarea 5.2)
TIEMPOS_REFERENCIA = (Path(__file__).resolve().parent.parent
                      / "tiempos" / "evidencia" / "tiempos_20261031_022527.json")


@dataclass
class Escenario:
    nombre: str
    kb_por_tx: float                # tamaño del dato crudo por transacción (incluye su evento web)
    capas_bytes: dict               # bytes relativos por capa (bronze, silver, gold); su suma / bronze = factor
    nodos: int                      # driver + workers del job cluster (DS3 v2)
    horas_dia: float                # horas de cluster en un día normal (en día pico se multiplican x3)
    lecturas_por_escritura: float   # operaciones de lectura por cada operación de escritura
    imprevistos: float              # fracción por costos no modelados (discos de los nodos, IPs, logs)

    @property
    def factor_capas(self) -> float:
        return sum(self.capas_bytes.values()) / self.capas_bytes["bronze"]


ESCENARIOS = [
    Escenario("Optimista", KB_POR_TX_MEDIDO,
              {"bronze": BYTES_CRUDO, "silver": BYTES_SILVER, "gold": BYTES_GOLD},
              nodos=3, horas_dia=1, lecturas_por_escritura=1, imprevistos=0.05),
    # Pesimista: 2 KB por registro (informe de la Unidad 2) y silver sin ganancia de compresión
    # más las versiones antiguas de Delta antes del VACUUM; gold con el mismo peso relativo medido.
    Escenario("Pesimista", 2.0,
              {"bronze": 1.0, "silver": 1.0, "gold": BYTES_GOLD / BYTES_CRUDO},
              nodos=9, horas_dia=3, lecturas_por_escritura=3, imprevistos=0.15),
]

PERFILES_MES = {                    # (días normales, días pico)
    "Mes sin pico": (30, 0),
    "Mes ponderado (24 + 6)": (DIAS_NORMALES, DIAS_PICO),
    "Mes completo en pico": (0, 30),
}


# ---------------------------------------------------------------------------
# Cálculos
# ---------------------------------------------------------------------------
def costo_almacenamiento(gb: float) -> float:
    costo, desde = 0.0, 0.0
    for hasta, precio in ALMACENAMIENTO:
        tramo = min(gb, hasta) - desde
        if tramo <= 0:
            break
        costo += tramo * precio
        desde = hasta
    return costo


def horas_cluster(e: Escenario, dias_normales: int, dias_pico: int) -> float:
    return dias_normales * e.horas_dia + dias_pico * e.horas_dia * FACTOR_PICO


def costo_computo(e: Escenario, dias_normales: int, dias_pico: int, tipo="jobs", workers_spot=False) -> dict:
    """Costo de máquinas virtuales y de DBU del job cluster (driver + workers)."""
    horas = horas_cluster(e, dias_normales, dias_pico)
    vm_workers = PRECIO_VM_SPOT if workers_spot else PRECIO_VM
    vm = horas * (PRECIO_VM + (e.nodos - 1) * vm_workers)
    dbu = horas * e.nodos * DBU_NODO_HORA * PRECIO_DBU[tipo]
    return {"vm": vm, "dbu": dbu, "total": vm + dbu, "nodo_horas": horas * e.nodos}


def costo_mes(e: Escenario, dias_normales: int, dias_pico: int, mes_operacion: int, **computo) -> dict:
    tx_mes = TX_DIA * (dias_normales + dias_pico * FACTOR_PICO)
    gb_crudo_mes = tx_mes * e.kb_por_tx / 1e6
    gb_escritos = gb_crudo_mes * e.factor_capas
    gb_almacenados = gb_escritos * min(mes_operacion, RETENCION_MESES)
    ops_escritura = gb_escritos * 1024 / MIB_POR_OPERACION
    c = costo_computo(e, dias_normales, dias_pico, **computo)
    filas = {
        "vm": c["vm"],
        "dbu": c["dbu"],
        "almacenamiento": costo_almacenamiento(gb_almacenados),
        "escrituras": ops_escritura / 1e4 * ESCRITURA_10K,
        "lecturas": ops_escritura * e.lecturas_por_escritura / 1e4 * LECTURA_10K,
    }
    filas["imprevistos"] = e.imprevistos * sum(filas.values())
    filas["total"] = sum(filas.values())
    filas.update(computo=c["total"], operaciones=filas["escrituras"] + filas["lecturas"],
                 gb_almacenados=gb_almacenados, ops_escritura=ops_escritura, tx_mes=tx_mes,
                 nodo_horas=c["nodo_horas"], usd_por_millon_tx=filas["total"] / (tx_mes / 1e6))
    return filas


def costo_corrida_prueba(nodos: int, horas: float, tipo: str) -> float:
    """Costo de una sesión de la prueba académica (5.000 transacciones) en Databricks."""
    return nodos * horas * (PRECIO_VM + DBU_NODO_HORA * PRECIO_DBU[tipo])


def cargar_tiempos(ruta: Path) -> dict:
    with open(ruta, encoding="utf-8") as f:
        datos = json.load(f)
    etapas = dict(datos["etapas_s"])
    etapas["arranque y otros"] = datos.get("sin_etapa_s", 0.0)
    return {"corrida": datos["corrida_id"], "total_s": datos["total_s"], "etapas": etapas,
            "subetapas": datos["subetapas"]}


# ---------------------------------------------------------------------------
# Salida
# ---------------------------------------------------------------------------
def usd(valor: float) -> str:
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def num(valor: float, decimales: int = 0) -> str:
    return usd(valor)[:-3] if decimales == 0 else f"{valor:,.{decimales}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(valor: float) -> str:
    return f"{100 * valor:.1f} %".replace(".", ",")


def imprimir(ruta_tiempos: Path):
    print("## Supuestos derivados")
    print(f"- Tamaño medido por transacción: {num(KB_POR_TX_MEDIDO, 3)} KB")
    for e in ESCENARIOS:
        print(f"- {e.nombre}: factor de capas = {num(e.factor_capas, 3)}; costo nodo-hora "
              f"jobs = {num(PRECIO_VM + DBU_NODO_HORA * PRECIO_DBU['jobs'], 4)}, "
              f"all-purpose = {num(PRECIO_VM + DBU_NODO_HORA * PRECIO_DBU['all_purpose'], 4)}")

    print("\n## Producción (50 M tx/día), job cluster con tarifa Jobs Compute")
    print("| Escenario | Perfil | Cómputo | Almac. mes 1 | Almac. mes 12 | Operaciones | Imprevistos | "
          "Total mes 1 | Total mes 12 | % techo (mes 12) | USD por millón de tx |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for e in ESCENARIOS:
        for perfil, (dn, dp) in PERFILES_MES.items():
            m1 = costo_mes(e, dn, dp, mes_operacion=1)
            m12 = costo_mes(e, dn, dp, mes_operacion=12)
            print(f"| {e.nombre} | {perfil} | {usd(m12['computo'])} | {usd(m1['almacenamiento'])} | "
                  f"{usd(m12['almacenamiento'])} | {usd(m12['operaciones'])} | {usd(m12['imprevistos'])} | "
                  f"{usd(m1['total'])} | {usd(m12['total'])} | {pct(m12['total'] / TECHO_MENSUAL)} | "
                  f"{usd(m12['usd_por_millon_tx'])} |")

    print("\n## Desglose por servicio (mes ponderado, mes 12)")
    print("| Servicio | Medidor | Cantidad optimista | Optimista | Cantidad pesimista | Pesimista |")
    print("|---|---|---|---|---|---|")
    mo, mp = (costo_mes(e, DIAS_NORMALES, DIAS_PICO, 12) for e in ESCENARIOS)
    filas = [
        ("Máquinas virtuales DS3 v2", "VM Linux, USD 0,251/h", "nodo_horas", "nodo-h", "vm"),
        ("Azure Databricks Premium", "Jobs Compute, 0,75 DBU/h x USD 0,30", "nodo_horas", "nodo-h", "dbu"),
        ("ADLS Gen2 Hot LRS", "Datos almacenados", "gb_almacenados", "GB", "almacenamiento"),
        ("ADLS Gen2 Hot LRS", "Operaciones de escritura (4 MiB)", "ops_escritura", "ops", "escrituras"),
        ("ADLS Gen2 Hot", "Operaciones de lectura (4 MiB)", None, "ops", "lecturas"),
    ]
    for servicio, medidor, cantidad, unidad, clave in filas:
        if cantidad:
            qo, qp = num(mo[cantidad]), num(mp[cantidad])
        else:
            qo = num(mo["ops_escritura"] * ESCENARIOS[0].lecturas_por_escritura)
            qp = num(mp["ops_escritura"] * ESCENARIOS[1].lecturas_por_escritura)
        print(f"| {servicio} | {medidor} | {qo} {unidad} | {usd(mo[clave])} | {qp} {unidad} | {usd(mp[clave])} |")
    print(f"| Imprevistos | {pct(ESCENARIOS[0].imprevistos)} / {pct(ESCENARIOS[1].imprevistos)} del subtotal | | "
          f"{usd(mo['imprevistos'])} | | {usd(mp['imprevistos'])} |")
    print(f"| **Total** | | | **{usd(mo['total'])}** | | **{usd(mp['total'])}** |")

    print("\n## Almacenamiento por capa del lago (mes ponderado, mes 12)")
    print("| Capa | Contenido | % optimista | GB optimista | USD optimista | % pesimista | GB pesimista | USD pesimista |")
    print("|---|---|---|---|---|---|---|---|")
    contenido = {"bronze": "CSV y JSON crudos", "silver": "Datos limpios en Parquet/Delta, por fecha",
                 "gold": "Agregaciones de negocio"}
    for capa in ("bronze", "silver", "gold"):
        celdas = []
        for e, m in zip(ESCENARIOS, (mo, mp)):
            parte = e.capas_bytes[capa] / sum(e.capas_bytes.values())
            celdas += [pct(parte), num(m["gb_almacenados"] * parte), usd(m["almacenamiento"] * parte)]
        print(f"| {capa} | {contenido[capa]} | " + " | ".join(celdas) + " |")

    tiempos = cargar_tiempos(ruta_tiempos)
    print(f"\n## Cómputo por etapa del pipeline (mes ponderado), según la corrida {tiempos['corrida']}")
    print("| Etapa | Subetapas | Segundos medidos | % del tiempo | USD optimista | USD pesimista |")
    print("|---|---|---|---|---|---|")
    for etapa, segundos in tiempos["etapas"].items():
        subs = ", ".join(s["subetapa"] for s in tiempos["subetapas"] if s["etapa"] == etapa) or "arranque de Spark, consola"
        parte = segundos / tiempos["total_s"]
        print(f"| {etapa} | {subs} | {num(segundos, 2)} | {pct(parte)} | "
              f"{usd(mo['computo'] * parte)} | {usd(mp['computo'] * parte)} |")

    print("\n## Sensibilidad del cómputo (mes ponderado)")
    print("| Escenario | Job cluster (Jobs Compute) | Cluster existente (All-purpose) | "
          "Job cluster con workers spot | Ahorro jobs vs all-purpose | Ahorro con spot |")
    print("|---|---|---|---|---|---|")
    for e in ESCENARIOS:
        jobs = costo_computo(e, DIAS_NORMALES, DIAS_PICO, "jobs")["total"]
        allp = costo_computo(e, DIAS_NORMALES, DIAS_PICO, "all_purpose")["total"]
        spot = costo_computo(e, DIAS_NORMALES, DIAS_PICO, "jobs", workers_spot=True)["total"]
        print(f"| {e.nombre} | {usd(jobs)} | {usd(allp)} | {usd(spot)} | {pct(1 - jobs / allp)} | {pct(1 - spot / jobs)} |")

    print("\n## Prueba académica (5.000 transacciones), costo por sesión")
    print("| Escenario | Configuración | Horas facturadas | Costo |")
    print("|---|---|---|---|")
    pruebas = [
        ("Optimista", "Job cluster de 1 nodo DS3 v2, se apaga al terminar (10 min con arranque)", 1, 10 / 60, "jobs"),
        ("Pesimista", "Cluster all-purpose de 3 nodos encendido hasta el auto-apagado (120 min)", 3, 2, "all_purpose"),
    ]
    for nombre, desc, nodos, horas, tipo in pruebas:
        print(f"| {nombre} | {desc} | {num(nodos * horas, 2)} nodo-h | {usd(costo_corrida_prueba(nodos, horas, tipo))} |")


if __name__ == "__main__":
    imprimir(Path(sys.argv[1]) if len(sys.argv) > 1 else TIEMPOS_REFERENCIA)
