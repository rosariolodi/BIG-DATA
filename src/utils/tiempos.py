"""
Registro de tiempos de ejecución por etapa (Inciso 5.2)
DATAMARCA S.A.

Spark evalúa de forma perezosa: medir el tiempo alrededor de un join o de una agregación solo
mide la construcción del plan, no su ejecución. Por eso cada medición debe terminar forzando la
ejecución del DataFrame de salida con `materializar(df)` (cache + count); así el tiempo queda en
la etapa que realmente hizo el trabajo.

Uso:
    registro = RegistroTiempos()
    with registro.medir("ingestion", "transacciones_csv") as m:
        m.filas = materializar(df)
    registro.imprimir()
    registro.guardar("reports/tiempos", spark, {"transacciones_leidas": 5000})
"""
import csv
import json
import platform
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

COLUMNAS_HISTORIAL = ["corrida_id", "inicio", "etapa", "subetapa", "duracion_s", "filas", "estado",
                      "transacciones_leidas", "logs_leidos", "shuffle_partitions"]


def _migrar_historial(ruta_csv: Path) -> None:
    """Si el historial se creó con menos columnas, lo reescribe con las actuales (sin perder filas)."""
    if not ruta_csv.exists():
        return
    with open(ruta_csv, encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f)
        if lector.fieldnames == COLUMNAS_HISTORIAL:
            return
        filas = list(lector)
    with open(ruta_csv, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUMNAS_HISTORIAL, extrasaction="ignore", restval="")
        escritor.writeheader()
        escritor.writerows(filas)


def materializar(df) -> int:
    """Fuerza la ejecución del DataFrame (cache + count) y devuelve el número de filas."""
    df.cache()
    return df.count()


class Medicion:
    """Objeto que recibe el bloque `with` para anotar datos de la subetapa (por ejemplo, las filas)."""

    def __init__(self):
        self.filas: Optional[int] = None


class RegistroTiempos:
    def __init__(self):
        self._t0 = time.perf_counter()
        self._inicio = datetime.now()
        self.corrida_id = self._inicio.strftime("%Y%m%d_%H%M%S")
        self.subetapas: List[Dict] = []
        self._fin_s: Optional[float] = None

    # -- medición ---------------------------------------------------------
    @contextmanager
    def medir(self, etapa: str, subetapa: str):
        medicion = Medicion()
        registro = {"etapa": etapa, "subetapa": subetapa, "inicio": datetime.now().isoformat(timespec="seconds"),
                    "duracion_s": None, "filas": None, "estado": "ok"}
        t0 = time.perf_counter()
        try:
            yield medicion
        except Exception:
            registro["estado"] = "error"
            raise
        finally:
            registro["duracion_s"] = round(time.perf_counter() - t0, 3)
            registro["filas"] = medicion.filas
            self.subetapas.append(registro)

    def finalizar(self) -> None:
        if self._fin_s is None:
            self._fin_s = time.perf_counter() - self._t0

    # -- resultados -------------------------------------------------------
    @property
    def total_s(self) -> float:
        fin = self._fin_s if self._fin_s is not None else time.perf_counter() - self._t0
        return round(fin, 3)

    @property
    def estado(self) -> str:
        return "error" if any(s["estado"] == "error" for s in self.subetapas) else "ok"

    def por_etapa(self) -> Dict[str, float]:
        """Suma de duraciones de las subetapas de cada etapa, en el orden en que aparecieron."""
        etapas: Dict[str, float] = {}
        for s in self.subetapas:
            etapas[s["etapa"]] = round(etapas.get(s["etapa"], 0.0) + s["duracion_s"], 3)
        return etapas

    @property
    def otros_s(self) -> float:
        """Tiempo del total que no pertenece a ninguna etapa (arranque de Spark, mostrar resultados)."""
        return round(max(self.total_s - sum(self.por_etapa().values()), 0.0), 3)

    def imprimir(self) -> None:
        total = self.total_s or 1e-9
        print(f"\n[Tiempos] Corrida {self.corrida_id} - estado: {self.estado} - total: {self.total_s:.2f} s")
        for etapa, segundos in self.por_etapa().items():
            print(f"  {etapa:<16}{segundos:>9.2f} s  ({100 * segundos / total:4.1f} %)")
            for s in self.subetapas:
                if s["etapa"] == etapa:
                    filas = f"{s['filas']:,} filas".replace(",", ".") if s["filas"] is not None else ""
                    marca = "" if s["estado"] == "ok" else "  [ERROR]"
                    print(f"      {s['subetapa']:<34}{s['duracion_s']:>8.2f} s  {filas}{marca}")
        print(f"  {'sin etapa':<16}{self.otros_s:>9.2f} s  ({100 * self.otros_s / total:4.1f} %)  "
              f"arranque de Spark y salida por consola")

    def a_dict(self, spark=None, extra: Optional[Dict] = None) -> Dict:
        entorno = {"python": platform.python_version(), "sistema": platform.platform()}
        if spark is not None:
            entorno.update({"spark": spark.version,
                            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                            "zona_horaria": spark.conf.get("spark.sql.session.timeZone")})
            try:
                entorno["master"] = spark.sparkContext.master
                entorno["paralelismo"] = spark.sparkContext.defaultParallelism
            except Exception:
                # En clusters compartidos (Spark Connect) sparkContext no está disponible
                entorno["master"] = "spark-connect"
                entorno["paralelismo"] = None
        return {
            "corrida_id": self.corrida_id,
            "inicio": self._inicio.isoformat(timespec="seconds"),
            "total_s": self.total_s,
            "estado": self.estado,
            "etapas_s": self.por_etapa(),
            "sin_etapa_s": self.otros_s,
            "subetapas": self.subetapas,
            "datos": extra or {},
            "entorno": entorno,
        }

    def guardar(self, directorio, spark=None, extra: Optional[Dict] = None) -> Path:
        """
        Escribe `tiempos_<corrida>.json` (detalle de esta corrida) y agrega una fila por subetapa
        a `historial_tiempos.csv` (para comparar corridas, por ejemplo con distinto volumen).
        """
        self.finalizar()
        carpeta = Path(directorio)
        carpeta.mkdir(parents=True, exist_ok=True)
        datos = self.a_dict(spark, extra)

        ruta_json = carpeta / f"tiempos_{self.corrida_id}.json"
        with open(ruta_json, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)

        ruta_csv = carpeta / "historial_tiempos.csv"
        _migrar_historial(ruta_csv)
        nuevo = not ruta_csv.exists()
        with open(ruta_csv, "a", encoding="utf-8", newline="") as f:
            escritor = csv.DictWriter(f, fieldnames=COLUMNAS_HISTORIAL)
            if nuevo:
                escritor.writeheader()
            for s in self.subetapas:
                escritor.writerow({"corrida_id": self.corrida_id, "inicio": s["inicio"], "etapa": s["etapa"],
                                   "subetapa": s["subetapa"], "duracion_s": s["duracion_s"], "filas": s["filas"],
                                   "estado": s["estado"],
                                   "transacciones_leidas": datos["datos"].get("transacciones_leidas"),
                                   "logs_leidos": datos["datos"].get("logs_leidos"),
                                   "shuffle_partitions": datos["entorno"].get("shuffle_partitions")})
        print(f"[Tiempos] Log guardado en {ruta_json}")
        return ruta_json