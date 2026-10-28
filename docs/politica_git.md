# Política de control de versiones (Tarea 3.2)

Proyecto Spark Pipeline - DATAMARCA S.A.

Este documento describe cómo trabaja hoy el equipo en Git y fija unas pocas reglas para que el historial muestre un trabajo colaborativo organizado. El repositorio está en la cuenta de GitHub `rosariolodi` (`rosariolodi/BIG-DATA`); quien quiera hacer push necesita permiso de escritura como colaborador.

## 1. Ramas

| Rama | Uso hoy |
|---|---|
| `main` | Rama estable. Solo recibe cambios mediante Pull Request. |
| `rosario`, `ivanna`, `joel`, `julian`, `kinan` | **Rama personal** de cada integrante. Cada quien trabaja sus tareas del sprint en la suya. |
| `<tipo>/<descripcion>` | Uso ocasional para un cambio aislado. Ejemplo ya usado: `docs/justificacion-batch-streaming` (tarea 2.3). |

Reglas:
- Se trabaja en la rama personal; no se hacen commits en la rama de otra persona.
- No se hace push directo a `main`.
- Las ramas personales no se borran después de un merge.

## 2. Flujo de trabajo actual

1. **Empezar con lo último de `main`:** `git merge origin/main` en la rama personal.
2. **Trabajar y hacer commits** (ver sección 3).
3. **Subir la rama personal:** `git push`.
4. **Abrir un Pull Request** en GitHub desde la rama personal hacia `main`. Si ya hay uno abierto de esa rama, los commits nuevos se suman solos.
5. **Integrar** con *Merge pull request* de GitHub. Hasta ahora lo hace principalmente Rosario (por ejemplo los PR #2 a #8); en el PR #9 el merge lo hizo Julián, quien había hecho el cambio.
6. **Actualizar la rama personal** con `git merge origin/main` antes de seguir trabajando.

### Conflictos
Cuando dos personas tocan el mismo archivo (ha pasado con `src/ingestion/reader.py`), el conflicto se resuelve **en la rama personal** trayendo `main` (`git merge origin/main`) antes de abrir el PR. Si el conflicto llega al momento del PR, se resuelve en una rama temporal (como se hizo con la rama `conflictos` en el PR #8) y se avisa a la persona dueña del archivo. Antes de editar un archivo compartido, avisar en el chat del grupo.

## 3. Commits semánticos

Formato:

```
<tipo>(<alcance>): <descripción en imperativo y en minúsculas>
```

El alcance es opcional y nombra el módulo (`ingestion`, `transformations`, `config`, `azure`). Cuando aplica, se indica la tarea del sprint al final: `(tarea 2.2)`.

| Tipo | Cuándo se usa |
|---|---|
| `feat` | Funcionalidad nueva del pipeline |
| `fix` | Corrección de un error |
| `docs` | Documentación, README, informes |
| `refactor` | Cambio de código que no altera el comportamiento |
| `test` | Pruebas |
| `perf` | Mejora de rendimiento |
| `chore` | Mantenimiento: `.gitignore`, dependencias, configuración |

Ejemplos reales del proyecto:

```
docs: agrega estrategia de particionamiento (tarea 1.4)
feat(ingestion): conectores de lectura con esquema estricto (tarea 2.1)
docs: justificar procesamiento batch vs streaming
```

Reglas:
- **Un commit por cambio lógico.** No mezclar, por ejemplo, un módulo nuevo con la documentación de otro.
- Como la rama personal agrupa varias tareas, el mensaje indica cuál es la tarea.
- El mensaje dice *qué* se hizo, no solo "arreglo" ni "update". Los commits que dicen únicamente "arreglo cleaning.py", "Create .git-temp" o "Add files via upload" no sirven para entender el historial; en adelante se evitan.
- Si un commit depende de otro (el reader importa el módulo de validación), el segundo va después.

## 4. Qué no se versiona

El archivo `.gitignore` excluye:
- `__pycache__/` y `*.pyc` (se regeneran solos).
- Salidas que cambian en cada ejecución: `data/quarantine/` y `src/data/` (esta última aparece al ejecutar `main.py` desde `src/`; la salida oficial es `data/processed/`).
- Archivos auxiliares que Spark escribe junto a los Parquet: `*.crc`, `_SUCCESS` y `_temporary/` (tarea 4.3).
- Credenciales y configuración local: `.env`, claves y certificados. **Nunca se suben secretos al repositorio.**
- Estado de Terraform (`*.tfstate`, `.terraform/`).
- Archivos de editor y del sistema operativo.

Sí se versionan los datos de entrada pequeños de `data/raw/` y el resultado de referencia de `data/processed/`, porque forman parte de la evidencia de ejecución.

## 5. Recomendaciones

- Que otra persona del equipo revise el Pull Request antes del merge, aunque hoy no se exija.
- Usar la plantilla de `.github/pull_request_template.md` al abrir el PR.
- Evitar subir archivos desde la interfaz web de GitHub ("Add files via upload"); es preferible `git add`, `git commit` y `git push` para que cada cambio quede con su mensaje.

## 6. Responsables

La configuración del repositorio es la tarea 3.2 del sprint planning (Rosi). Cualquier integrante puede proponer cambios a esta política mediante un Pull Request.
