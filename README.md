# BIG-DATA · Pipeline batch de DATAMARCA S.A.

Pipeline de procesamiento batch con **PySpark** que ingiere transacciones de puntos de venta (CSV) y eventos de e-commerce (JSON), valida su calidad, los limpia, los cruza y genera KPIs de negocio.

Puede ejecutarse de dos formas con el mismo código:

| Modo | Entrada | Salida | Cuándo usarlo |
|---|---|---|---|
| **Local** (por defecto) | `data/raw/` | `data/processed/silver` y `data/processed/gold` (Parquet particionado) | Desarrollo y pruebas en el computador |
| **Azure** | ADLS Gen2, contenedor `bronze` | ADLS Gen2, `silver` y `gold` (Delta Lake particionado) | Ejecución en la nube con Databricks |

El modo se elige con la variable de entorno `DATAMARCA_STORAGE`: si no existe, corre en local; si contiene el nombre de la cuenta de almacenamiento, corre en Azure (ver `src/config/rutas.py`).

---

## Arquitectura

```mermaid
flowchart LR
    A[CSV transacciones POS] --> B[bronze]
    C[JSON logs e-commerce] --> B
    B --> D[Ingestión + validación de calidad]
    D -->|registros inválidos| Q[silver/quarantine]
    D -->|registros válidos| E[Limpieza]
    E --> S[silver · transacciones y logs por fecha]
    E --> F[Join transacciones + logs por user_id]
    F --> G[Agregaciones de negocio · SparkSQL y DataFrames]
    G --> H[gold · agregaciones]
```

Se sigue una arquitectura **Medallion** sobre ADLS Gen2. La estrategia de particionamiento está en [`docs/particionamiento.md`](docs/particionamiento.md) y su implementación en [`docs/persistencia.md`](docs/persistencia.md).

### Resultados generados

| Capa | Tablas | Partición |
|---|---|---|
| `silver` | `transacciones`, `logs_ecommerce` (válidos y limpios, con `dq_warnings`) | `fecha` (día en hora de Bogotá) |
| `gold` | `ventas_por_dia` | `mes` |
| `gold` | `kpis_negocio_categoria`, `kpis_por_categoria`, `top_clientes`, `ventas_por_tienda`, `ventas_por_metodo_pago` | `fecha_proceso` |

La cuarentena de calidad se guarda aparte (`data/quarantine/` en local, `silver/quarantine` en Azure).

---

## Estructura del repositorio

```
.
├── azure/
│   ├── infrastructure/     Terraform: grupo de recursos, ADLS Gen2, Databricks
│   └── databricks-jobs/    Definición del job que ejecuta el pipeline
├── data/
│   ├── raw/                Datos de entrada de ejemplo (CSV y JSON)
│   └── processed/          Resultados del modo local (silver y gold)
├── docs/                   Reglas de calidad, particionamiento, persistencia, costos, batch vs streaming, política Git
├── notebooks/              Análisis exploratorio (EDA)
├── reports/
│   ├── tiempos/            Logs de tiempos por etapa y su evidencia
│   ├── optimizacion/       Evidencia del optimizador Catalyst (planes, benchmark, pushdown)
│   └── costos/             Script que calcula la estimación de costos
├── src/
│   ├── main.py             Orquestador del pipeline
│   ├── config/             Esquemas estrictos y rutas (local / Azure)
│   ├── ingestion/          Lectura y validación de calidad (cuarentena y métricas)
│   ├── transformations/    Limpieza, joins y agregaciones
│   ├── persistence/        Escritura en Parquet o Delta particionado
│   ├── performance/        Pruebas de rendimiento y volumen (tarea 5.1)
│   ├── optimization/       Benchmarks y análisis del optimizador Catalyst
│   └── utils/              Registro de tiempos por etapa
└── tests/                  Pruebas con pytest sobre Spark local (y pruebas manuales en tests/manual)
```

---

## Ejecución local

**Requisitos:** Python 3.12 y Java 17 o 21 (PySpark 3.5 no soporta Python 3.14).

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r tests/requirements-test.txt
python src/main.py
```

Ejecútalo desde la **raíz** del repositorio. Los resultados quedan en `data/processed/silver` y `data/processed/gold`, la cuarentena y las métricas de calidad en `data/quarantine/` y los tiempos por etapa en `reports/tiempos/`. Una corrida completa con los 5.000 registros de cada fuente tarda entre uno y tres minutos según el equipo.

**Notas para Windows:**
- Si el nombre del equipo contiene un guion bajo, Spark falla con `Invalid Spark URL`. Se resuelve definiendo, antes de ejecutar, `SPARK_LOCAL_IP=127.0.0.1` y `SPARK_LOCAL_HOSTNAME=localhost` (en PowerShell: `$env:SPARK_LOCAL_IP="127.0.0.1"; $env:SPARK_LOCAL_HOSTNAME="localhost"`).
- Sin `hadoop.dll`, Spark no puede escribir en disco local. La persistencia lo detecta y escribe el mismo layout con `pyarrow`, avisándolo en consola; en Linux o Databricks no ocurre. Los mensajes `UnsatisfiedLinkError` que aparecen en ese caso no son un fallo.

### Pruebas

```bash
pytest tests -v
```

Las pruebas generan sus propios datos pequeños y no leen `data/raw/`. Cubren la validación de calidad, las agregaciones, el registro de tiempos, la idempotencia (reprocesar el mismo lote da el mismo resultado) y el pushdown sobre Parquet particionado. Más detalles en [`tests/README.md`](tests/README.md). La guía de pruebas manuales, con el resultado esperado de cada una, está en [`tests/manual/README.md`](tests/manual/README.md).

---

## Variables de entorno y credenciales

Ninguna variable ni credencial se versiona. Los valores se definen en cada entorno.

| Variable / credencial | Dónde se define | Obligatoria | Para qué sirve |
|---|---|---|---|
| `DATAMARCA_STORAGE` | Entorno del proceso o del cluster | Solo en Azure | Nombre de la cuenta ADLS Gen2. Si existe, el pipeline lee de `bronze` y escribe en `silver`/`gold`. Si no existe, corre en local |
| `SHUFFLE_PARTITIONS` | Entorno del proceso o del cluster | No | Número de particiones de shuffle de Spark (`spark.sql.shuffle.partitions`). Por defecto, el de Spark (200). Se usa en las pruebas de rendimiento |
| `DATAMARCA_STORAGE_ROOT` | Entorno del proceso | No | Raíz donde se guarda la persistencia local. Por defecto, `data/processed/`. Ver [`docs/persistencia.md`](docs/persistencia.md) |
| `DATAMARCA_FORMATO` | Entorno del proceso | No | `auto` (por defecto: Delta si la sesión lo tiene, si no Parquet), `delta` o `parquet` |
| `AZURE_STORAGE_ACCOUNT` y `AZURE_STORAGE_KEY` | Entorno o `.env` (nunca en Git) | Solo si se escribe en ADLS desde fuera de Databricks | Cuenta y clave de ADLS Gen2 para Spark local |
| `SPARK_LOCAL_IP` y `SPARK_LOCAL_HOSTNAME` | Entorno del proceso | Solo en equipos Windows con guion bajo en el nombre | Valores `127.0.0.1` y `localhost`, para evitar `Invalid Spark URL` |
| `subscription_id` | `azure/infrastructure/terraform.tfvars` | Para Terraform | ID de la suscripción de Azure |
| `location` | `azure/infrastructure/terraform.tfvars` | No | Región de Azure (`mexicocentral`) |
| Clave de la cuenta de almacenamiento | Secret scope `datamarca`, secreto `storage-key` | Para Databricks | Acceso del cluster a ADLS Gen2. El cluster la lee con `{{secrets/datamarca/storage-key}}` |
| Inicio de sesión de Azure y Databricks | `az login` y `databricks auth login` | Para el despliegue | Autenticación de las CLI |

## Reproducir la ejecución en un entorno limpio

**Local** (desde la raíz del repositorio, con Python 3.12 y Java 17 o 21):

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r tests/requirements-test.txt
python src/main.py
```

**Azure** (desde cero; el detalle de cada paso está en la sección siguiente):

```powershell
# 1. Infraestructura
cd azure\infrastructure
terraform init; terraform apply
# 2. Datos, acceso al storage, código y job: pasos 2 a 4
# 3. Ejecución
databricks jobs run-now <job_id>
```

---

## Despliegue en Azure

### Requisitos previos

- Suscripción de Azure (por ejemplo, Azure for Students).
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli), [Terraform](https://developer.hashicorp.com/terraform/install) y [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) instalados.
- Sesión iniciada con `az login`.
- Proveedores registrados en la suscripción:

```bash
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.Databricks
az provider register --namespace Microsoft.Compute
az provider register --namespace Microsoft.Network
az provider register --namespace Microsoft.ManagedIdentity
```

### 1. Crear la infraestructura

En `azure/infrastructure/` crea el archivo `terraform.tfvars` (está en `.gitignore`, no se sube):

```hcl
subscription_id = "<id-de-tu-suscripcion>"
location        = "<region-permitida>"   # opcional, por defecto eastus2
```

El ID se obtiene con `az account show --query id -o tsv`. Las suscripciones de estudiante solo permiten ciertas regiones; si Terraform responde `RequestDisallowedByAzure`, cambia `location`.

```bash
terraform init
terraform plan
terraform apply
```

Se crean: un grupo de recursos, una cuenta ADLS Gen2 con los contenedores `bronze`, `silver` y `gold`, y un workspace de Azure Databricks (SKU `premium`, el SKU `standard` ya no se acepta).

### 2. Subir los datos crudos

```powershell
$cuenta = terraform output -raw storage_account_name
az storage blob upload-batch --account-name $cuenta --destination bronze --source ..\..\data\raw --auth-mode key
```

### 3. Configurar el acceso de Databricks al storage

Guarda la clave de la cuenta en un secret scope (nunca en el código ni en Git):

```powershell
databricks auth login --host (terraform output -raw databricks_url)
$clave = az storage account keys list --account-name $cuenta --resource-group rg-datamarca-dev --query "[0].value" -o tsv
databricks secrets create-scope datamarca
databricks secrets put-secret datamarca storage-key --string-value $clave
```

En el cluster (**Compute → Edit → Advanced options**):

- **Spark config:**
  ```
  spark.hadoop.fs.azure.account.key.<cuenta>.dfs.core.windows.net {{secrets/datamarca/storage-key}}
  ```
- **Environment variables:**
  ```
  DATAMARCA_STORAGE=<cuenta>
  ```

Reinicia el cluster después de guardar.

### 4. Subir el código y crear el job

Desde la raíz del repositorio:

```powershell
$email = (databricks current-user me --output json | ConvertFrom-Json).userName
databricks workspace import-dir src "/Workspace/Users/$email/BIG-DATA/src" --overwrite

# Rellena la plantilla del job con tu cluster y tu usuario
$clusterId = "<id-del-cluster>"
(Get-Content azure\databricks-jobs\job_pipeline.json -Raw) -replace "__CLUSTER_ID__", $clusterId -replace "__EMAIL__", $email | Set-Content job_listo.json
databricks jobs create --json "@job_listo.json"
```

`job_listo.json` contiene datos de tu entorno: bórralo y no lo subas a Git.

### 5. Ejecutar y verificar

```powershell
databricks jobs run-now <job_id>
az storage blob list --account-name $cuenta --container-name gold --auth-mode key --query "[].name" -o tsv
```

En `silver` deben aparecer las carpetas Delta `transacciones` y `logs_ecommerce` (particionadas por `fecha`) y la cuarentena de la validación en `silver/quarantine`; en `gold`, las carpetas Delta de los resultados (particionadas por `mes` o `fecha_proceso`).

### 6. Evidencia de la ejecución (tiempos por etapa)

En cada corrida en Azure, `main.py` publica el log de tiempos en el Data Lake, porque el disco del driver de Databricks se pierde al apagar el cluster:

```
gold/reports/tiempos/json/tiempos_<AAAAMMDD_HHMMSS>/part-*.txt   detalle de la corrida (JSON)
gold/reports/tiempos/csv/tiempos_<AAAAMMDD_HHMMSS>/part-*.txt    una fila por subetapa (CSV)
```

El JSON incluye tiempo total, tiempo por etapa (ingestión, transformación, persistencia), filas, estado y el entorno (versiones de Python y Spark). Para guardarlo como evidencia en el repositorio:

```powershell
az storage blob download-batch --account-name $cuenta --source gold --pattern "reports/tiempos/*part-*" --destination .\tmp_tiempos --auth-mode key
New-Item -ItemType Directory -Force reports\tiempos\evidencia\azure | Out-Null
Get-ChildItem .\tmp_tiempos -Recurse -Filter "part-*" | ForEach-Object {
    Copy-Item $_.FullName "reports\tiempos\evidencia\azure\$($_.Directory.Name).$($_.Directory.Parent.Name)"
}
Remove-Item .\tmp_tiempos -Recurse
```

Quedan archivos como `reports/tiempos/evidencia/azure/tiempos_<id>.json` y `.csv`. La salida completa del job (calidad de datos, tabla de tiempos y `PIPELINE COMPLETADO`) se puede ver en la corrida, dentro de **Jobs & Pipelines** en Databricks.

### Supuestos del entorno cloud

- Proveedor: Microsoft Azure, suscripción Azure for Students (regiones restringidas por política).
- Almacenamiento: ADLS Gen2 (cuenta `StorageV2` con namespace jerárquico, LRS) con los contenedores `bronze`, `silver` y `gold`.
- Cómputo: Azure Databricks, workspace SKU `premium`, cluster de acceso compartido (Spark Connect) con auto-apagado. Versión de Databricks Runtime: `<completar>`.
- Formato de salida: Delta Lake particionado en `silver` y `gold`; la cuarentena de calidad se guarda en Parquet en `silver/quarantine`.
- Autenticación al storage: clave de la cuenta en un secret scope (adecuado para desarrollo).
- Volumen de la prueba: 5.000 transacciones y 5.000 eventos de e-commerce.
- El pipeline no mantiene estado entre corridas: cada ejecución sobrescribe sus resultados.

### Costos y limpieza

- El workspace y el almacenamiento cuestan poco; el gasto principal es el **cluster de Databricks**. Activa el auto-apagado y termínalo al acabar.
- Para eliminar todo lo creado (**incluidos los datos**): `terraform destroy` en `azure/infrastructure/`.

### Notas de seguridad

- Nunca versiones `terraform.tfvars`, `*.tfstate`, claves, tokens ni archivos `.env`. Antes de cada commit revisa `git status`.
- La clave de cuenta en un secret scope sirve para desarrollo. Para producción es preferible usar identidades administradas o Unity Catalog.
- En clusters de acceso compartido no está disponible `spark.sparkContext`; el código ya lo tiene en cuenta.

---

## Calidad de datos

Cada fuente se lee con **esquema estricto** (sin `inferSchema`) y se valida con reglas documentadas en [`docs/reglas_calidad.md`](docs/reglas_calidad.md). Los registros que incumplen reglas críticas se envían a **cuarentena** con el ID de la regla, y cada ejecución guarda métricas de calidad. Las advertencias no rechazan registros.

## Rendimiento y optimización

- **Tiempos por etapa:** cada ejecución registra duración y filas por subetapa (ingestión, transformación y persistencia). Spark es perezoso, por eso cada subetapa termina con `cache` y `count`: así el tiempo cae en la etapa que hizo el trabajo. En local se guardan en `reports/tiempos/`; en Azure, en `gold/reports/tiempos/` (ver paso 6). Evidencias en `reports/tiempos/evidencia/` (corridas locales y una corrida en Linux) y en `reports/tiempos/evidencia/azure/` (corridas en Azure Databricks); detalle en [`reports/tiempos/README.md`](reports/tiempos/README.md). Los tiempos varían mucho entre corridas y entre equipos: sirven para comparar etapas y configuraciones, no como cifra absoluta.
- **Corridas en Azure:** `reports/tiempos/evidencia/azure/` tiene 7 corridas en Azure Databricks (Spark 4.1.0 por Spark Connect, Python 3.12.3). Cinco procesaron los 5.000 registros de cada fuente, con totales entre 52 s y 165 s. Solo la última (`tiempos_20261031_034517`) usa la persistencia particionada en `silver` y `gold` (35 s y 30 s); las anteriores corresponden a versiones previas del pipeline. Las otras dos (`032732` y `034151`) duran 0,5 s y no registran etapas.
- **Particiones de shuffle:** con 5.000 filas, `SHUFFLE_PARTITIONS=8` bajó el total de unos 54 s (valor por defecto, 200) a unos 23 s en una comparación de dos corridas por configuración. La comparación se repite con más volumen en la tarea 5.1; el valor por defecto de `main.py` no se ha cambiado.
- **Volumen y carga 3x (tarea 5.1):** `src/performance/benchmark_5_1.py`, con resultados e informe en `docs/Rendimiento 5.1/` y `data/processed/DATAMARCA_5_1_*.csv`.
- **Optimizador Catalyst (tarea 5.3):** los análisis y benchmarks (column pruning, funciones nativas contra UDF de Python, joins) están en `src/optimization/` y sus evidencias en `reports/optimizacion/`. La evidencia de pushdown sobre el Parquet particionado (`src/optimization/pushdown_parquet.py`) muestra que filtrar por `fecha` abre 1 de 31 archivos (3,3 % de los bytes) y que el filtro de monto llega al lector de Parquet; con estos datos no descarta lectura, porque cada archivo tiene un solo grupo de filas. Con una UDF de Python el filtro no se empuja.
- **Costos (tarea 6.1):** [`docs/costos.md`](docs/costos.md) estima el costo mensual en Azure en un escenario optimista y uno pesimista frente al techo de USD 20.000, con el cálculo reproducible en `reports/costos/estimacion_costos.py`.
- **Idempotencia:** reprocesar el mismo lote da el mismo resultado, sin duplicados (`tests/test_idempotencia.py`).

## Documentación

| Documento | Contenido |
|---|---|
| [`docs/reglas_calidad.md`](docs/reglas_calidad.md) | Reglas de validación de datos |
| [`docs/particionamiento.md`](docs/particionamiento.md) | Estrategia de particionamiento y capas Medallion |
| [`docs/persistencia.md`](docs/persistencia.md) | Escritura en Parquet o Delta particionado |
| [`docs/costos.md`](docs/costos.md) | Estimación de costos operativos en Azure |
| [`docs/Justificacion_batch_vs_streaming.md`](docs/Justificacion_batch_vs_streaming.md) | Por qué se eligió procesamiento batch, con Apache Beam como referente |
| [`docs/politica_git.md`](docs/politica_git.md) | Ramas, commits semánticos y revisión |
| [`tests/README.md`](tests/README.md) y [`tests/manual/README.md`](tests/manual/README.md) | Pruebas automáticas y manuales |

## Flujo de trabajo en Git

Se trabaja en ramas propias y se integra a `main` mediante pull request (plantilla en `.github/pull_request_template.md`). Los commits siguen el formato semántico descrito en [`docs/politica_git.md`](docs/politica_git.md), por ejemplo `feat(azure): ...`.
