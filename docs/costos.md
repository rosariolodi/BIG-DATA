# Estimación de costos operativos en Azure (Tarea 6.1)

Proyecto Spark Pipeline - DATAMARCA S.A. · Responsable: Kinan · Precios consultados el 30/10/2026

Este documento estima cuánto cuesta operar en Azure el pipeline batch implementado en la Unidad 3: Azure Databricks para el cómputo y ADLS Gen2 para el almacenamiento. Desglosa el costo **por servicio, por capa del lago y por etapa del pipeline**, para la prueba académica y para la operación de producción del caso (50 millones de transacciones diarias con picos de 3×). Responde al paso 4 de la guía de la Unidad 3: «estimación de costo en el proveedor cloud y comportamiento ante incrementos de volumen».

Al ser un caso académico sin mediciones de producción, se trabaja con un **escenario optimista y uno pesimista**. Cada cifra indica su origen: precio oficial, dato medido en el repositorio, dato del caso o supuesto.

**Fuentes:**
- Precios: Azure Retail Prices API de Microsoft y Azure Pricing Calculator, región Mexico Central (sección 3).
- Cálculo reproducible: [`reports/costos/estimacion_costos.py`](../reports/costos/estimacion_costos.py), solo con librería estándar:

```bash
python reports/costos/estimacion_costos.py
```

## 1. Resumen ejecutivo

| Indicador (mes ponderado: 24 días normales + 6 de pico, régimen estable) | Optimista | Pesimista |
|---|---|---|
| Costo mensual del pipeline | USD 294,19 | USD 3.260,16 |
| Porcentaje del techo de USD 20.000 | 1,5 % | 16,3 % |
| Mes completo en pico (30 días a 3×) | USD 630,41 (3,2 %) | USD 6.924,48 (34,6 %) |
| Costo por millón de transacciones | USD 0,14 | USD 1,55 |
| Rubro principal | Almacenamiento (74 %) | Almacenamiento (70 %) |
| Prueba académica (5.000 transacciones), por sesión | USD 0,08 | USD 3,98 |

El pipeline cumple el techo en todos los escenarios. El costo lo domina el almacenamiento acumulado (retención de 12 meses), no el cómputo. La optimización más directa es ejecutar el job en un *job cluster* en lugar del cluster interactivo que usa hoy.

## 2. Alcance y recursos costeados

| Incluido | Fuera de alcance |
|---|---|
| Cómputo de Azure Databricks (máquinas virtuales + DBU) del job del pipeline | Resto de la arquitectura de la Unidad 2 (Event Hubs, Synapse, Purview, Power BI, seguridad, monitoreo, soporte); ver sección 9 |
| ADLS Gen2: datos almacenados y operaciones de lectura y escritura | Orquestación de la Unidad 4 (Airflow o Data Factory) |
| Prueba académica y producción (sin pico, ponderado y pico completo) | Descuentos por reserva o plan de ahorro |

Recursos definidos en `azure/infrastructure/main.tf`, `azure/databricks-jobs/job_pipeline.json` y el `README.md`:

| Recurso | Configuración | Cómo se factura |
|---|---|---|
| ADLS Gen2 | `StorageV2`, Standard, LRS, namespace jerárquico, nivel Hot; contenedores `bronze`, `silver`, `gold` | GB almacenados por mes + operaciones |
| Workspace de Azure Databricks | SKU `premium` | Sin costo fijo; se paga el cómputo usado |
| Cluster del job | Hoy `existing_cluster_id` (cluster interactivo, tarifa All-purpose). Se costea como *job cluster* (tarifa Jobs Compute) y el actual se muestra como sensibilidad | Máquina virtual por hora + DBU por hora |
| Tipo de nodo | Standard_DS3_v2 (4 vCPU, 14 GB RAM) | Ver sección 3 |
| Región | `mexicocentral` (decisión del equipo; `variables.tf` trae `eastus2` por defecto y se cambia en `terraform.tfvars`) | Los precios dependen de la región |

## 3. Precios oficiales (USD, pago por uso, Mexico Central)

Fuente primaria: Azure Retail Prices API (`https://prices.azure.com/api/retail/prices`), consultada el 30/10/2026 con estos filtros (se pueden abrir en el navegador):

- Databricks: `$filter=serviceName eq 'Azure Databricks' and armRegionName eq 'mexicocentral'`
- Máquina virtual: `$filter=serviceName eq 'Virtual Machines' and armRegionName eq 'mexicocentral' and armSkuName eq 'Standard_DS3_v2'`
- Almacenamiento: `$filter=serviceName eq 'Storage' and armRegionName eq 'mexicocentral' and productName eq 'General Block Blob v2 Hierarchical Namespace'`

| Servicio | Medidor | Precio | Fuente |
|---|---|---|---|
| Azure Databricks Premium | Jobs Compute | 0,30 por DBU-hora | API y calculadora |
| Azure Databricks Premium | All-purpose Compute | 0,55 por DBU-hora | API |
| Azure Databricks | DBU por hora de un nodo DS3 v2 | 0,75 DBU | Calculadora oficial («DS3 v2: 4 vCPUs, 14 GB RAM, 0.75 Databricks Unit(s), $0.251/hour») |
| Máquina virtual Standard_DS3_v2 (Linux) | Pago por uso | 0,251 por hora | API y calculadora |
| Máquina virtual Standard_DS3_v2 (Linux) | Spot (precio variable, puede ser desalojada) | 0,05635 por hora | API |
| ADLS Gen2 Hot LRS | Datos almacenados, primeros 50 TB | 0,02288 por GB-mes | API y calculadora |
| ADLS Gen2 Hot LRS | Datos almacenados, siguientes 450 TB | 0,021965 por GB-mes | API y calculadora |
| ADLS Gen2 Hot LRS | Operaciones de escritura (bloques de 4 MiB) | 0,0715 por 10.000 | API y calculadora |
| ADLS Gen2 Hot | Operaciones de lectura (bloques de 4 MiB) | 0,00572 por 10.000 | API y calculadora |
| ADLS Gen2 Cool LRS (referencia para ciclo de vida) | Datos almacenados | 0,01672 por GB-mes | API |

Costo de un nodo-hora DS3 v2: **0,251 + 0,75 × 0,30 = USD 0,476** con Jobs Compute, y **0,251 + 0,75 × 0,55 = USD 0,6635** con All-purpose.

## 4. Supuestos

| # | Variable | Optimista | Pesimista | Origen |
|---|---|---|---|---|
| S1 | Transacciones diarias promedio | 50.000.000 | 50.000.000 | Caso |
| S2 | Pico estacional | 3× | 3× | Caso |
| S3 | Composición del mes | 24 días normales + 6 de pico | Igual | Informe de la Unidad 2 |
| S4 | Transacciones del mes ponderado | 2.100 millones (50 M × (24 + 6 × 3)) | Igual | Derivado de S1 a S3 |
| S5 | Tamaño crudo por transacción (con su evento web) | 0,294 KB | 2 KB | Optimista: **medido** en `data/raw` (1.470.302 B para 5.000 transacciones + 5.000 eventos). Pesimista: supuesto del informe de la Unidad 2 |
| S6 | Peso de cada capa respecto al crudo (bronze = 1) | silver 0,276; gold 0,014 | silver 1,0; gold 0,014 | Optimista: **medido** en `data/processed` (silver 405.707 B, gold 19.951 B). Pesimista: silver sin ganancia de compresión + versiones antiguas de Delta antes del `VACUUM` |
| S7 | Retención en el lago | 12 meses | 12 meses | Informe de la Unidad 2 |
| S8 | Tipo de nodo | DS3 v2 (0,75 DBU/h) | DS3 v2 (0,75 DBU/h) | Calculadora oficial |
| S9 | Job cluster en día normal | 3 nodos (1 driver + 2 workers) durante 1 h | 9 nodos (1 + 8) durante 3 h | Supuesto de dimensionamiento: 14,7 GB/día con 8 núcleos de worker frente a 100 GB/día con 32. A validar con la tarea 5.1 |
| S10 | Día de pico | El mismo cluster, 3× horas | Igual | Supuesto: el volumen se triplica y el autoescalado se refleja en horas-nodo |
| S11 | Operaciones de ADLS | 1 escritura por cada 4 MiB escritos; 1 lectura por escritura | 1 escritura por cada 4 MiB; 3 lecturas por escritura (reprocesos, consultas) | Forma de facturación de ADLS Gen2 + supuesto |
| S12 | Costos no modelados (discos de los nodos, IPs, logs, medidores secundarios) | +5 % | +15 % | Supuesto de contingencia |
| S13 | Reparto del cómputo por etapa | Proporción de tiempo de la corrida `20261031_022527`, pipeline completo con la persistencia de la tarea 4.3 (registro de la tarea 5.2) | Igual | **Medido** en `reports/tiempos/evidencia/` |

## 5. Metodología

| Componente | Fórmula |
|---|---|
| Horas de cluster al mes | días normales × horas/día + días de pico × horas/día × 3 |
| Máquinas virtuales | horas × nodos × 0,251 |
| DBU | horas × nodos × 0,75 × 0,30 |
| GB escritos al mes | transacciones del mes × KB por transacción × (suma de los pesos de las capas) / 10⁶ |
| GB almacenados en el mes *n* | GB escritos al mes × mín(*n*, 12), con precio por tramos |
| Operaciones de escritura | GB escritos × 1.024 / 4 |
| Operaciones de lectura | operaciones de escritura × lecturas por escritura |
| Imprevistos | porcentaje S12 sobre el subtotal |

«Mes 1» es el primer mes de operación. «Mes 12» es el régimen estable, con el lago lleno por la retención de 12 meses.

## 6. Resultados

### 6.1 Producción por perfil de mes (USD por mes)

| Escenario | Perfil | Cómputo | Almac. mes 1 | Almac. mes 12 | Operaciones | Imprevistos | Total mes 1 | Total mes 12 | % del techo (mes 12) | USD por millón de tx |
|---|---|---|---|---|---|---|---|---|---|---|
| Optimista | Mes sin pico | 42,84 | 13,01 | 156,17 | 1,12 | 10,01 | 59,83 | 210,14 | 1,1 % | 0,14 |
| Optimista | Mes ponderado (24 + 6) | 59,98 | 18,22 | 218,63 | 1,57 | 14,01 | 83,76 | 294,19 | 1,5 % | 0,14 |
| Optimista | Mes completo en pico | 128,52 | 39,04 | 468,50 | 3,37 | 30,02 | 179,48 | 630,41 | 3,2 % | 0,14 |
| Pesimista | Mes sin pico | 385,56 | 138,21 | 1.639,06 | 13,71 | 305,75 | 618,10 | 2.344,08 | 11,7 % | 1,56 |
| Pesimista | Mes ponderado (24 + 6) | 539,78 | 193,50 | 2.275,94 | 19,19 | 425,24 | 865,35 | 3.260,16 | 16,3 % | 1,55 |
| Pesimista | Mes completo en pico | 1.156,68 | 414,63 | 4.823,48 | 41,13 | 903,19 | 1.854,31 | 6.924,48 | 34,6 % | 1,54 |

### 6.2 Desglose por servicio (mes ponderado, mes 12)

| Servicio | Medidor | Cantidad optimista | USD optimista | Cantidad pesimista | USD pesimista |
|---|---|---|---|---|---|
| Máquinas virtuales DS3 v2 | VM Linux, USD 0,251/h | 126 nodo-h | 31,63 | 1.134 nodo-h | 284,63 |
| Azure Databricks Premium | Jobs Compute, 0,75 DBU/h × USD 0,30 | 126 nodo-h | 28,35 | 1.134 nodo-h | 255,15 |
| ADLS Gen2 Hot LRS | Datos almacenados | 9.555 GB | 218,63 | 101.483 GB | 2.275,94 |
| ADLS Gen2 Hot LRS | Operaciones de escritura | 203.853 ops | 1,46 | 2.164.989 ops | 15,48 |
| ADLS Gen2 Hot | Operaciones de lectura | 203.853 ops | 0,12 | 6.494.969 ops | 3,72 |
| Imprevistos | 5 % / 15 % del subtotal | | 14,01 | | 425,24 |
| **Total** | | | **294,19** | | **3.260,16** |

### 6.3 Almacenamiento por capa del lago (mes ponderado, mes 12)

| Capa | Contenido | % optimista | GB optimista | USD optimista | % pesimista | GB pesimista | USD pesimista |
|---|---|---|---|---|---|---|---|
| bronze | CSV y JSON crudos | 77,5 % | 7.410 | 169,55 | 49,7 % | 50.400 | 1.130,30 |
| silver | Datos limpios en Parquet/Delta, particionados por fecha | 21,4 % | 2.044 | 46,78 | 49,7 % | 50.400 | 1.130,30 |
| gold | Agregaciones de negocio | 1,1 % | 100 | 2,30 | 0,7 % | 683 | 15,34 |

### 6.4 Cómputo por etapa del pipeline (mes ponderado)

Reparto del costo de cómputo según la proporción de tiempo de cada etapa en la corrida `20261031_022527`. Es el pipeline completo con la persistencia particionada de la tarea 4.3, ejecutado en Colab (2 núcleos, 200 particiones de shuffle), 182,16 s en total. Evidencia en `reports/tiempos/evidencia/tiempos_20261031_022527.json`.

| Etapa | Subetapas medidas | Segundos | % del tiempo | USD optimista | USD pesimista |
|---|---|---|---|---|---|
| Ingestión | `transacciones_csv` (33,80 s), `logs_json` (16,54 s): lectura con esquema estricto + validación de calidad | 50,34 | 27,6 % | 16,58 | 149,18 |
| Transformación | `limpieza_transacciones` (5,35 s), `limpieza_logs` (4,95 s), `join` (15,71 s), `agregaciones` (34,33 s) | 60,34 | 33,1 % | 19,87 | 178,81 |
| Persistencia | `silver` (35,46 s; 10.000 filas en 62 particiones), `gold` (23,99 s; 8 tablas) | 59,45 | 32,6 % | 19,57 | 176,17 |
| Arranque y otros | Arranque de Spark, salida por consola | 12,02 | 6,6 % | 3,96 | 35,62 |
| **Total cómputo** | | **182,16** | **100 %** | **59,98** | **539,78** |

La transformación sigue siendo la etapa más cara (validaciones con ventanas, join y agregaciones), muy cerca de la persistencia. La persistencia pesa más que en la corrida anterior a la tarea 4.3 (`20261029_191518`, donde era el 4 %), porque antes solo guardaba 71 filas agregadas con pandas y ahora escribe silver completo y gold, particionados. Con datos tan pequeños, las 200 particiones de shuffle generan muchas tareas vacías: en `reports/tiempos/evidencia/historial_tiempos.csv` (tarea 5.2), la suma de etapas en local fue de 47–55 s con 200 particiones y de 20–23 s con `SHUFFLE_PARTITIONS=8`. Con el log de otra corrida se recalcula así:

```bash
python reports/costos/estimacion_costos.py reports/tiempos/tiempos_<corrida>.json
```

En Databricks, la máquina virtual se factura desde que se aprovisiona, así que el arranque del cluster (varios minutos) también tiene costo; por eso conviene agrupar el trabajo en una sola corrida diaria.

### 6.5 Picos y crecimiento

- **Picos:** cada día de pico triplica el cómputo de ese día. En el mes ponderado esto añade un 40 % frente a un mes sin pico (59,98 frente a 42,84 en el optimista). Un mes entero en pico multiplica el total por 2,1 (630,41 frente a 294,19 en el optimista; 6.924,48 frente a 3.260,16 en el pesimista) y aun así no supera el 35 % del techo.
- **Crecimiento del lago:** el almacenamiento crece durante 12 meses hasta llenar la retención. En el pesimista pasa de USD 193,50 en el mes 1 a USD 2.275,94 en el mes 12. Sin política de retención seguiría creciendo cada mes.
- **Costo unitario estable:** entre USD 0,14 y 1,56 por millón de transacciones, sin importar el perfil. El costo escala de forma lineal con el volumen.

### 6.6 Sensibilidad del cómputo (mes ponderado, USD)

| Escenario | Job cluster (Jobs Compute) | Cluster existente (All-purpose, configuración actual) | Job cluster con workers spot | Ahorro del job cluster | Ahorro adicional con spot |
|---|---|---|---|---|---|
| Optimista | 59,98 | 83,60 | 43,63 | 28,3 % | 27,3 % |
| Pesimista | 539,78 | 752,41 | 343,58 | 28,3 % | 36,3 % |

### 6.7 Prueba académica (5.000 transacciones), costo por sesión

| Escenario | Configuración | Horas facturadas | USD |
|---|---|---|---|
| Optimista | Job cluster de 1 nodo DS3 v2 que se apaga al terminar (10 min con arranque) | 0,17 nodo-h | 0,08 |
| Pesimista | Cluster all-purpose de 3 nodos encendido hasta el auto-apagado (120 min) | 6,00 nodo-h | 3,98 |

Con 5.000 filas el costo depende del tiempo que el cluster está encendido, no del volumen: las corridas registradas duran entre 19 y 23 s y la mayor parte es arranque fijo.

## 7. Verificación en la calculadora oficial de Azure

Se configuró la estimación en la [Azure Pricing Calculator](https://azure.microsoft.com/en-us/pricing/calculator/) (región Mexico Central, pago por uso, contrato MCA, USD) con cuatro partidas:

| Partida en la calculadora | Configuración | USD/mes en la calculadora | USD/mes en este modelo |
|---|---|---|---|
| Optimista - Databricks | Jobs Compute, Premium, 3 × DS3 v2 × 42 h; DBU 42 h × 3 × 0,75 × 0,30 | 59,98 (VM 31,63 + DBU 28,35) | 59,98 |
| Pesimista - Databricks | Jobs Compute, Premium, 9 × DS3 v2 × 126 h; DBU 126 h × 9 × 0,75 × 0,30 | 539,78 (VM 284,63 + DBU 255,15) | 539,78 |
| Optimista - ADLS Gen2 | Data Lake Storage Gen2, Standard, LRS, Hot, namespace jerárquico; 9.555 GB; 20,39 × 10.000 escrituras y lecturas | 221,71 (datos 218,62 + transacciones 1,57 + otros 1,52) | 220,21 |
| Pesimista - ADLS Gen2 | Igual; 101.483 GB; 216,5 × 10.000 escrituras y 649,5 × 10.000 lecturas | 2.296,64 (datos 2.275,92 + transacciones 19,20 + otros 1,52) | 2.295,14 |

La diferencia de USD 1,52 por cuenta corresponde a medidores secundarios que la calculadora incluye con valores por defecto (operaciones iterativas de listado y renombrado, otras operaciones y 1 GB de metadatos). En el modelo quedan cubiertos por los imprevistos.

Para conservar la estimación como enlace: iniciar sesión en la calculadora con una cuenta de Microsoft, recrear las cuatro partidas con la tabla anterior y usar **Share** o **Export** (la calculadora exige sesión iniciada para compartir).

## 8. Lectura de los resultados

- **Techo de USD 20.000:** el pipeline consume entre el **1,5 % y el 16,3 %** en el mes ponderado en régimen estable, y como máximo el 34,6 % en un mes completo en pico pesimista.
- **Qué pesa más:** el almacenamiento (74 % del optimista y 70 % del pesimista en el mes 12). El cómputo es estable y predecible.
- **Qué etapa cuesta más:** la transformación (33,1 % del cómputo), casi empatada con la persistencia particionada (32,6 %), seguidas de la ingestión con validación de calidad (27,6 %).
- **Costo por millón de transacciones:** entre USD 0,14 y 1,56. Es el indicador del criterio C3 de la prueba de concepto de la Unidad 2.

## 9. Relación con la arquitectura completa de la Unidad 2

El informe de la Unidad 2 estimó la arquitectura completa en Azure en USD 3.105/mes (ponderado, cotizado en Brazil South). Las líneas equivalentes a este pipeline eran Databricks (USD 346) y ADLS Gen2 (USD 432); ambas caen dentro del rango calculado aquí. Si se reemplazan por este rango:

| Componente | Valor | Origen |
|---|---|---|
| Resto de servicios de la Unidad 2 (Event Hubs, Synapse, Purview, Power BI, seguridad y redes, Data Factory, monitoreo, soporte) | USD 2.327 | Informe de la Unidad 2 (3.105 − 346 − 432) |
| Pipeline batch (este documento) | USD 294,19 – 3.260,16 | Sección 6 |
| **Total de la arquitectura** | **USD 2.621 – 5.587 por mes** | 13,1 % – 27,9 % del techo |

La conclusión de la Unidad 2 se mantiene: Azure cumple el techo con amplio margen.

**Para corregir en el informe de la Unidad 2:** dice que Azure no tiene región en México. La API y la calculadora de precios ofrecen la región **Mexico Central**, con Databricks, máquinas virtuales y ADLS Gen2, que es la que eligió el equipo.

## 10. Optimizaciones recomendadas

| Optimización | Efecto estimado | Dónde |
|---|---|---|
| Ejecutar el job en un **job cluster** (`new_cluster`) en lugar de `existing_cluster_id` | DBU de 0,55 a 0,30; −28,3 % del cómputo (tabla 6.6) | `azure/databricks-jobs/job_pipeline.json` |
| Workers en **spot**, con el driver bajo demanda | −27 % a −36 % adicional del cómputo; riesgo de desalojo, por eso el driver no es spot | Configuración del cluster |
| Auto-apagado corto (10–20 min) en clusters interactivos | Evita sesiones como la pesimista de la tabla 6.7 (USD 3,98 frente a 0,08) | Compute → Edit |
| Ciclo de vida: pasar bronze a **Cool** después de 30 días | Cool LRS 0,01672 frente a Hot 0,02288 USD/GB-mes (−27 % en la capa más pesada) | Lifecycle management de la cuenta |
| `VACUUM` periódico en tablas Delta y retención de 12 meses | Contiene el factor de capas (diferencia entre 1,29 y 2,01 en S6) | Mantenimiento de tablas |
| Partición por `fecha` y *partition pruning* | Las consultas leen solo los días necesarios: menos lecturas y menos cómputo | Implementado en la tarea 4.3 |

## 11. Cómo afinar la estimación con datos reales

Basta con cambiar las constantes de `estimacion_costos.py`:

| Dato | Dónde obtenerlo | Reemplaza |
|---|---|---|
| Configuración y DBU/h del cluster | Databricks → **Compute** → el cluster → **Edit**: el resumen de la derecha muestra «X DBU/h» y el tipo de nodo | S8, S9 |
| Duración real del job | **Jobs & Pipelines** → `datamarca-pipeline-batch` → **Runs** (duración y arranque), o `gold/reports/tiempos/` en ADLS | S9, S13 |
| DBU consumidos | Notebook SQL: `SELECT usage_date, sku_name, SUM(usage_quantity) AS dbu FROM system.billing.usage WHERE usage_metadata.job_id = '<job_id>' GROUP BY ALL` (requiere tablas de sistema habilitadas) | Cómputo medido |
| Costo real facturado | Portal de Azure → **Cost Management** → **Cost analysis**, ámbito `rg-datamarca-dev`, agrupado por *Service name* o *Meter*, exportar CSV (en suscripciones de estudiante puede estar limitado) | Validación de todo el modelo |
| Volumen real en ADLS | `az storage blob list --account-name <cuenta> --container-name silver --query "[].properties.contentLength"` | S6 |
| Escalado con el volumen | Resultados de la tarea 5.1 (corridas ×10, ×100) | S9, S10 |

## 12. Limitaciones

- El dimensionamiento del cluster de producción (S9) es un supuesto; no hay corridas en Azure con volumen creciente (tarea 5.1). Los rangos amplios lo compensan.
- Se supone un escalado lineal del costo con el volumen. Los joins y las ventanas pueden escalar peor; el escenario pesimista deja margen.
- El reparto por etapa usa una corrida en Colab con 5.000 filas y 200 particiones de shuffle, donde los costos fijos de Spark pesan más que en producción.
- Los precios son de pago por uso en USD a la fecha de consulta. El precio spot cambia con frecuencia.
- No incluye el resto de la arquitectura de la Unidad 2 ni la orquestación de la Unidad 4.
