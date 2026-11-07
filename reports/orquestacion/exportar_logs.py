"""
Exporta los logs de Airflow de una corrida del DAG `datamarca_pipeline` como evidencia (tarea T1.3).

Se ejecuta dentro del contenedor de Airflow, donde están los logs (/opt/airflow/logs) y el CLI:

    docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py --listar
    docker compose exec airflow-scheduler python /opt/datamarca/reports/orquestacion/exportar_logs.py \
        --run-id "manual__2026-11-07T15:04:10.123456+00:00" --nombre fallo_forzado

Deja en reports/orquestacion/evidencia/logs/<nombre>/:
  * <task_id>.intento<N>.txt  log de cada intento, legible (Airflow 3 guarda una línea JSON por evento);
  * estados_airflow.txt       estado final de cada tarea según el metastore (airflow tasks states-for-dag-run);
  * resumen.md                resumen que escribió la tarea `notificar`, si existe.

Los nombres de archivo no llevan ':' ni '+' (el run_id sí), para que el repositorio se pueda clonar en Windows.
Solo usa la biblioteca estándar.
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

DAG_ID = "datamarca_pipeline"
RAIZ = Path(__file__).resolve().parents[2]
DESTINO = RAIZ / "reports" / "orquestacion" / "evidencia" / "logs"
REPORTES = RAIZ / "reports" / "orquestacion"
CAMPOS_BASE = ("timestamp", "level", "logger", "event")


def nombre_seguro(texto: str) -> str:
    """Mismo criterio que src/orquestacion/tareas.py: limpiar_run_id."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", texto)


def linea_legible(linea: str) -> str:
    """Convierte una línea JSON del log de Airflow 3 en texto; deja igual las que no son JSON."""
    try:
        registro = json.loads(linea)
    except ValueError:
        return linea.rstrip("\n")
    if not isinstance(registro, dict) or "event" not in registro:
        return linea.rstrip("\n")
    texto = (f"{registro.get('timestamp', '')} {str(registro.get('level', '')).upper()} "
             f"[{registro.get('logger', '')}] {registro['event']}")
    extra = {k: v for k, v in registro.items() if k not in CAMPOS_BASE}
    if extra:  # no se descarta nada: el resto de campos va al final de la línea
        texto += "  | " + json.dumps(extra, ensure_ascii=False, default=str)
    return texto


def listar(logs: Path) -> None:
    base = logs / f"dag_id={DAG_ID}"
    corridas = sorted(base.glob("run_id=*"), key=lambda p: p.stat().st_mtime) if base.is_dir() else []
    if not corridas:
        print(f"No hay logs de {DAG_ID} en {base}")
    for corrida in corridas:
        print(f"{corrida.name.removeprefix('run_id=')}  ({len(list(corrida.glob('task_id=*')))} tareas)")


def estados_airflow(run_id: str) -> str:
    comando = ["airflow", "tasks", "states-for-dag-run", DAG_ID, run_id, "-o", "table"]
    try:
        r = subprocess.run(comando, capture_output=True, text=True, timeout=180)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"(no se pudo consultar el CLI de Airflow: {e})\n"
    if r.returncode != 0:
        return f"(el CLI de Airflow terminó con código {r.returncode})\n{r.stderr}"
    return r.stdout


def exportar(logs: Path, run_id: str, nombre: str, destino: Path) -> Path:
    origen = logs / f"dag_id={DAG_ID}" / f"run_id={run_id}"
    if not origen.is_dir():
        sys.exit(f"No existe {origen}. Use --listar para ver los run_id disponibles.")
    salida = destino / nombre_seguro(nombre)
    if salida.exists():
        shutil.rmtree(salida)
    salida.mkdir(parents=True)

    archivos = sorted(origen.glob("task_id=*/**/attempt=*.log"))
    for archivo in archivos:
        tarea = next(p for p in archivo.parts if p.startswith("task_id=")).removeprefix("task_id=")
        intento = archivo.stem.removeprefix("attempt=")
        lineas = archivo.read_text(encoding="utf-8", errors="replace").splitlines()
        (salida / f"{nombre_seguro(tarea)}.intento{intento}.txt").write_text(
            "\n".join(linea_legible(l) for l in lineas) + "\n", encoding="utf-8")

    (salida / "estados_airflow.txt").write_text(
        f"$ airflow tasks states-for-dag-run {DAG_ID} {run_id} -o table\n\n{estados_airflow(run_id)}",
        encoding="utf-8")
    resumen = REPORTES / f"resumen_{nombre_seguro(run_id)}.md"
    if resumen.is_file():
        shutil.copyfile(resumen, salida / "resumen.md")

    print(f"{len(archivos)} logs exportados a {salida}")
    return salida


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta los logs de Airflow de una corrida de datamarca_pipeline")
    parser.add_argument("--listar", action="store_true", help="lista los run_id que tienen logs")
    parser.add_argument("--run-id", help="run_id de la corrida, tal como lo muestra Airflow")
    parser.add_argument("--nombre", help="nombre de la carpeta de evidencia, por ejemplo fallo_forzado")
    parser.add_argument("--logs", type=Path, default=Path("/opt/airflow/logs"), help="carpeta de logs de Airflow")
    parser.add_argument("--destino", type=Path, default=DESTINO, help="carpeta donde se guarda la evidencia")
    args = parser.parse_args()
    if args.listar:
        listar(args.logs)
    elif args.run_id and args.nombre:
        exportar(args.logs, args.run_id, args.nombre, args.destino)
    else:
        parser.error("use --listar, o --run-id junto con --nombre")


if __name__ == "__main__":
    main()
