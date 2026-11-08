# Documentación técnica de la orquestación — Unidad 4, T1.4

**Proyecto:** DATAMARCA S.A. — Pipeline Batch con PySpark

**Responsable:** Julián Camargo

**Fecha:** 7 de noviembre de 2026

**Tecnología de orquestación:** Apache Airflow 3.3.2

**Motor de procesamiento:** Apache Spark / PySpark

**Entorno de ejecución documentado:** Docker local y GitHub Codespaces

---

## 1. Introducción

DATAMARCA S.A. desarrolla un pipeline de procesamiento batch orientado a integrar información de transacciones de puntos de venta (POS) y registros de eventos de comercio electrónico. El procesamiento utiliza Apache Spark mediante PySpark para ejecutar operaciones de ingestión, validación, limpieza, integración, agregación y persistencia de información.

Como parte de la Unidad 4, se incorporó Apache Airflow para coordinar la ejecución del pipeline mediante un grafo dirigido acíclico (DAG), denominado `datamarca_pipeline`.

La incorporación de una herramienta de orquestación permite organizar las diferentes etapas del procesamiento, establecer dependencias entre tareas, ejecutar operaciones independientes en paralelo, controlar errores, administrar reintentos y mantener trazabilidad sobre las ejecuciones.

La solución documentada utiliza Apache Airflow 3.3.2 en un entorno local basado en Docker, con `LocalExecutor` y PostgreSQL como base de datos de metadatos.

Es importante diferenciar esta implementación del procesamiento desplegado en Azure Databricks. Aunque ambos utilizan el mismo proyecto de procesamiento, la orquestación con Airflow funciona mediante las rutas locales configuradas para Docker y no corresponde al job de Azure Databricks que ejecuta directamente `src/main.py`.

## 2. Objetivos

### 2.1. Objetivo general

Documentar y justificar técnicamente la arquitectura de orquestación implementada para DATAMARCA S.A., evaluando la selección de Apache Airflow, sus componentes operativos, la configuración de tolerancia a fallos y los resultados obtenidos durante las pruebas.

### 2.2. Objetivos específicos

- Justificar la selección de Apache Airflow frente a AWS Step Functions.

- Describir la arquitectura de ejecución y los componentes que participan en la orquestación.

- Analizar la responsabilidad operativa del Scheduler, DAG Processor, API Server, ejecutor y base de datos de metadatos.

- Explicar la configuración de reintentos, tiempos de espera y backoff exponencial.

- Evaluar las mediciones de ejecución y el aprovechamiento del paralelismo.

- Documentar las estrategias de retención, trazabilidad y recuperación ante fallos.

- Identificar limitaciones de la implementación y recomendaciones para un entorno productivo.

## 3. Justificación de la tecnología seleccionada

### 3.1. Alternativas evaluadas

Para la orquestación del pipeline se considera Apache Airflow como tecnología principal y AWS Step Functions como alternativa administrada.

Ambas soluciones permiten definir flujos de trabajo, establecer dependencias, controlar estados y gestionar fallos. Sin embargo, presentan diferencias en su modelo de operación y en la integración con la arquitectura existente.

| Criterio | Apache Airflow | AWS Step Functions |

|---|---|---|

| Modelo de operación | Plataforma de orquestación que puede autogestionarse o desplegarse mediante servicios administrados | Servicio administrado de AWS |

| Definición de flujos | DAG definidos mediante Python | Máquinas de estados definidas mediante Amazon States Language |

| Integración con Python | Integración directa con el código del proyecto | Integración mediante estados, tareas y servicios compatibles |

| Integración con Spark | Permite ejecutar procesos PySpark mediante operadores y procesos externos | Puede coordinar ejecuciones de servicios de procesamiento compatibles con AWS |

| Infraestructura | Requiere administrar el entorno de ejecución, almacenamiento de metadatos y componentes de Airflow cuando es autogestionado | AWS administra la infraestructura del servicio |

| Portabilidad | Puede ejecutarse localmente, en contenedores y en diferentes infraestructuras | Está integrado principalmente con el ecosistema AWS |

| Observabilidad | Interfaz web, estados de tareas y registros de ejecución | Historial de ejecuciones, estados y herramientas de observabilidad de AWS |

| Operación | Requiere administración de servicios, actualizaciones, monitoreo y almacenamiento | Reduce la administración de infraestructura de orquestación |

| Adecuación al proyecto | Alta para la arquitectura Python y Docker existente | Requeriría adaptar la solución al ecosistema AWS |

### 3.2. Justificación de Apache Airflow

Se seleccionó Apache Airflow porque se ajusta a la arquitectura tecnológica que ya utiliza DATAMARCA.

El pipeline está desarrollado en Python y PySpark, por lo que Airflow permite reutilizar los módulos existentes mediante operadores que ejecutan cada tarea de manera independiente.

Además, la herramienta permite representar explícitamente las dependencias del procesamiento y establecer reglas de ejecución para escenarios de éxito, fallo y recuperación.

Las principales razones de la selección son:

**1. Compatibilidad con el desarrollo existente.**

El código de procesamiento se encuentra implementado en Python. Airflow permite coordinar las funciones y los procesos existentes sin tener que rediseñar el pipeline mediante otro lenguaje de definición de estados.

**2. Control de dependencias y paralelismo.**

El DAG permite ejecutar simultáneamente las tareas de ingestión, validación y limpieza de las dos fuentes. También permite que la persistencia de Silver avance de forma independiente de la rama de integración y agregación que produce Gold.

**3. Manejo de errores y reintentos.**

Airflow permite definir políticas de reintento, tiempos máximos de ejecución y reglas de disparo. Esto facilita la recuperación ante fallos transitorios y la identificación de las tareas que impiden completar una corrida.

**4. Portabilidad del entorno.**

La implementación utiliza Docker Compose, lo que facilita reproducir el entorno de orquestación en diferentes equipos y en GitHub Codespaces.

**5. Visibilidad operativa.**

La interfaz de Airflow permite consultar el estado de cada tarea, los intentos realizados, los registros de ejecución y el historial de las corridas.

### 3.3. ¿Por qué no se seleccionó AWS Step Functions ni Google Cloud Composer?

AWS Step Functions constituye una alternativa válida, especialmente cuando una arquitectura utiliza principalmente servicios administrados de AWS. Sin embargo, su adopción debe analizarse en función del ecosistema tecnológico definido para DATAMARCA.

En la Unidad 2 se seleccionó Microsoft Azure como plataforma de nube para la arquitectura del proyecto. Por esta razón, incorporar AWS Step Functions supondría introducir una dependencia de un proveedor diferente al seleccionado, lo que aumentaría la complejidad de integración, administración y gobierno de la solución.

Adicionalmente, aunque Step Functions permite coordinar flujos de trabajo y servicios de procesamiento, su integración con el pipeline existente requeriría adaptar la ejecución de los procesos PySpark a los mecanismos y servicios compatibles con AWS.

También se considera Google Cloud Composer como una alternativa basada en Apache Airflow. No obstante, Cloud Composer es un servicio administrado propio de Google Cloud y no una alternativa nativa de Azure. Por tanto, no se selecciona como mecanismo de despliegue para la arquitectura definida.

Apache Airflow ofrece una alternativa independiente del proveedor, que puede desplegarse sobre infraestructura compatible con Azure y permite conservar la definición de los flujos mediante DAG en Python. Esto facilita mantener la coherencia con la estrategia tecnológica del proyecto y reutilizar el código de procesamiento existente.

La implementación documentada actualmente utiliza Docker Compose para ejecutar y validar la orquestación. Esto no significa que Airflow ya esté desplegado productivamente en Azure, sino que la tecnología seleccionada permite una evolución hacia ese entorno sin depender de los servicios de orquestación de AWS o Google Cloud.

### 3.4. Decisión arquitectónica

Se mantiene Apache Airflow como herramienta de orquestación para la solución documentada.

La decisión se fundamenta en la reutilización del código Python, la flexibilidad del DAG, el paralelismo, el control de errores, la portabilidad y la coherencia con la selección de Microsoft Azure realizada en la Unidad 2.

AWS Step Functions y Google Cloud Composer podrían evaluarse si en el futuro se modifica la estrategia de infraestructura y se justifica adoptar servicios administrados de sus respectivos proveedores.

---

## 4. Arquitectura de orquestación

### 4.1. Descripción general

La arquitectura implementada utiliza Apache Airflow 3.3.2 y Docker Compose.

Los servicios definidos son:

- PostgreSQL: almacenamiento de metadatos de Airflow.

- API Server: proporciona la interfaz y los servicios de API.

- Scheduler: programa las ejecuciones y administra el estado de las tareas.

- DAG Processor: analiza y procesa las definiciones de los DAG.

- Airflow Init: servicio de inicialización y migración de la base de datos.

El entorno utiliza `LocalExecutor`, por lo que no cuenta con un servicio de workers independientes. Las tareas se ejecutan localmente desde el entorno del scheduler.

### 4.2. Componentes y responsabilidades

| Componente | Responsabilidad | Consideración operativa |

|---|---|---|

| Scheduler | Programa las tareas, evalúa dependencias y administra su ejecución | Requiere disponibilidad continua para coordinar el DAG |

| DAG Processor | Analiza los archivos Python que definen los DAG | Su carga depende de la cantidad y complejidad de los DAG |

| API Server | Expone la interfaz web y los servicios de API | Permite consultar ejecuciones y administrar el entorno |

| LocalExecutor | Ejecuta las tareas en el entorno local de Airflow | Comparte los recursos del entorno de ejecución |

| PostgreSQL | Almacena metadatos, estados, ejecuciones y otra información operativa | Requiere persistencia, respaldo y mantenimiento |

| Docker Compose | Coordina los servicios y sus volúmenes | Facilita la reproducción del entorno |

La implementación utiliza un volumen persistente denominado `pgdata` para PostgreSQL y otro denominado `airflow-logs` para los registros de ejecución.

El repositorio se monta dentro de los contenedores en `/opt/datamarca`, permitiendo que Airflow acceda al DAG, al código fuente, a los datos y a los reportes.

### 4.3. Flujo de procesamiento

El DAG `datamarca_pipeline` está compuesto por doce tareas de procesamiento y una tarea adicional de control denominada `corrida_completa`.

Las tareas se organizan de la siguiente manera:

| Identificador | Tarea | Función |

|---|---|---|

| T1 | `ingestar_transacciones` | Verifica el origen de los datos de transacciones |

| T2 | `ingestar_logs` | Verifica el origen de los registros de e-commerce |

| T3 | `validar_transacciones` | Valida la calidad de las transacciones |

| T4 | `validar_logs` | Valida la calidad de los registros de e-commerce |

| T5 | `compuerta_calidad` | Evalúa el porcentaje de registros rechazados |

| T6 | `limpiar_transacciones` | Limpia los datos de transacciones |

| T7 | `limpiar_logs` | Limpia los registros de e-commerce |

| T8 | `unir` | Integra las fuentes de información |

| T9 | `agregar` | Calcula las agregaciones de negocio |

| T10 | `persistir_silver` | Guarda los datos limpios en la capa Silver |

| T11 | `persistir_gold` | Guarda las agregaciones en la capa Gold |

| T12 | `notificar` | Genera el resumen del resultado de la corrida |

La tarea `corrida_completa` permite determinar si las etapas de persistencia finalizaron correctamente.

### 4.4. Paralelismo

Una de las principales ventajas de la arquitectura es que no obliga a ejecutar todas las operaciones de manera secuencial.

Las tareas T1 y T2 pueden ejecutarse en paralelo porque verifican fuentes independientes. Lo mismo sucede con T3 y T4, y posteriormente con T6 y T7.

Adicionalmente, Silver no depende de la unión ni de las agregaciones. Por lo tanto, T10 puede ejecutarse mientras avanzan T8, T9 y T11.

Esta separación permite aprovechar mejor los recursos disponibles y reducir el tiempo total de procesamiento.

El diseño completo de dependencias se encuentra en `docs/orquestacion/dependencias.md`.

### 4.5. Compuerta de calidad

La tarea T5 implementa una compuerta de calidad que evalúa las métricas generadas durante la validación de cada fuente.

El umbral establecido es del 0,1 % de registros rechazados por fuente, configurable mediante la variable:

`UMBRAL_RECHAZO_PCT`

Si una fuente supera este porcentaje, la compuerta detiene las etapas posteriores de transformación y persistencia.

Los registros rechazados individualmente se mantienen en cuarentena y las métricas permiten identificar el comportamiento de cada fuente.

Esta decisión evita incorporar información de una corrida que no cumple los criterios de calidad establecidos para el proyecto.

### 4.6. Elección de los operadores

Las doce tareas de procesamiento usan `BashOperator`, y `corrida_completa` usa `EmptyOperator`, que solo refleja el resultado de la corrida. Cada tarea ejecuta `python src/orquestacion/tareas.py <tarea> --run-id <corrida> --intento <n>` en su propio proceso.

Se eligió `BashOperator` y no `SparkSubmitOperator` ni `HttpOperator` por las siguientes razones:

- **No todas las tareas usan Spark.** Ocho de las doce abren una sesión de Spark (validación, limpieza, unión, agregación y persistencia). Las otras cuatro no: `ingestar_transacciones` e `ingestar_logs` verifican los archivos de origen con Python, `compuerta_calidad` lee métricas en JSON y `notificar` escribe el resumen y envía el webhook opcional. Un operador específico de Spark solo serviría para las ocho primeras, y el DAG tendría que mezclar operadores distintos.
- **Spark corre en modo local dentro del contenedor de Airflow.** La imagen instala `pyspark` (`airflow/requirements-airflow.txt`) y el master por defecto es `local[2]` (`DATAMARCA_SPARK_MASTER`). No hay un clúster de Spark al que enviar una aplicación con `spark-submit`, que es el caso para el que está pensado `SparkSubmitOperator`. Este operador no se evaluó en el proyecto.
- **Un solo punto de entrada.** El mismo comando sirve para el DAG y para depurar una tarea a mano (sección "Una tarea suelta" de `airflow/README.md`). Si la tarea falla, el script termina con código de salida distinto de cero: Airflow la marca como fallida y aplica los reintentos.
- **`HttpOperator` no aplica.** Ninguna tarea llama a un servicio HTTP como paso del flujo. El webhook de `notificar` es opcional y lo envía el propio `tareas.py`.

Esta elección tiene costos. Cada tarea arranca su propia sesión de Spark, con su tiempo de arranque, y los procesos comparten los recursos del contenedor (sección 11.2). Si el pipeline se traslada a un clúster, por ejemplo al job de Azure Databricks que ya existe en el repositorio, convendría reemplazar `BashOperator` por un operador que envíe el trabajo al clúster. Esa alternativa no se evaluó.

---

## 5. Configuración operativa

### 5.1. Programación de las ejecuciones

El DAG cuenta con una programación diaria mediante la expresión cron:

`0 2 * * *`

La ejecución está configurada para las 02:00, utilizando la zona horaria `America/Bogota`.

También se establecen los siguientes parámetros:

| Parámetro | Valor | Justificación |

|---|---|---|

| `schedule` | `0 2 * * *` | Ejecuta el pipeline diariamente a las 02:00, hora de Colombia, cuando la partición correspondiente al día anterior ya ha finalizado su ventana de generación de datos. Esto permite procesar información diaria completa y reducir el riesgo de leer una partición todavía en actualización. |

| `catchup` | `False` | Evita ejecutar automáticamente todas las fechas históricas pendientes |

| `max_active_runs` | `1` | Impide que varias corridas del mismo DAG se ejecuten simultáneamente |

| `retries` | `3` | Permite recuperar tareas ante fallos transitorios |

| `retry_delay` | 60 segundos | Define la espera inicial antes del primer reintento |

| `retry_exponential_backoff` | `True` | Incrementa progresivamente el tiempo de espera |

| `max_retry_delay` | 600 segundos | Establece el límite máximo de espera |

La programación se establece a las 02:00 (America/Bogota) para permitir que los procesos de generación y disponibilidad de datos del día anterior hayan finalizado antes de iniciar el procesamiento. De esta manera, el pipeline trabaja sobre una partición diaria completa, disminuyendo el riesgo de inconsistencias ocasionadas por información parcial. Esta decisión supone que las fuentes de datos cumplen con la disponibilidad esperada antes de la hora programada.

La programación diaria está definida en el código, aunque la evidencia disponible no demuestra una prueba específica del disparo automático a las 02:00.

### 5.2. Tiempos máximos de ejecución

Se establecen límites de tiempo para evitar que las tareas permanezcan ejecutándose indefinidamente.

| Etapa | Timeout |

|---|---:|

| Ingestión | 5 minutos |

| Compuerta de calidad | 2 minutos |

| Validación | 15 minutos |

| Limpieza | 15 minutos |

| Unión | 15 minutos |

| Agregación | 15 minutos |

| Persistencia | 20 minutos |

| Notificación | 5 minutos |

| Corrida completa | 60 minutos |

Los tiempos de ejecución reales de las tareas son considerablemente inferiores a estos límites en las pruebas realizadas.

Sin embargo, las pruebas específicas de timeout de tarea y de corrida todavía están preparadas, pero no ejecutadas según la documentación de T1.3.

### 5.3. Configuración de reintentos

La configuración permite tres reintentos adicionales por tarea, lo que representa hasta cuatro intentos en total contando el intento inicial.

La espera inicial es de 60 segundos y el mecanismo de backoff exponencial aumenta el intervalo entre reintentos, hasta alcanzar un máximo de 600 segundos.

Esta configuración busca evitar que una falla transitoria provoque la pérdida definitiva de una corrida y permite que el sistema se recupere sin intervención manual cuando las condiciones que originaron el error desaparecen.

---

## 6. Análisis del backoff exponencial

### 6.1. Funcionamiento esperado

El backoff exponencial incrementa progresivamente el tiempo de espera entre intentos.

Con una espera inicial de 60 segundos, los intervalos teóricos de referencia son aproximadamente:

- Primer reintento: 60 segundos.

- Segundo reintento: 120 segundos.

- Tercer reintento: 240 segundos.

La configuración establece un máximo de 600 segundos.

El propósito de esta estrategia es evitar que una tarea que presenta fallos temporales vuelva a ejecutarse inmediatamente y genere presión adicional sobre los recursos o servicios involucrados.

### 6.2. Primera medición: Docker local

Durante la primera ronda de pruebas, realizada el 6 de noviembre de 2026, se provocó un fallo controlado en la tarea `limpiar_logs`.

La configuración utilizada fue:

- `DATAMARCA_RETRIES=3`

- `DATAMARCA_RETRY_DELAY_SEG=60`

- Fallo controlado mediante `DATAMARCA_FALLAR_EN=limpiar_logs`

Los tiempos observados fueron:

| Intento | Hora de inicio (UTC) | Espera desde el intento anterior |

|---|---|---:|

| 1 | 02:23:21 | — |

| 2 | 02:24:52 | 91 segundos |

| 3 | 02:27:54 | 183 segundos |

| 4 | 02:34:14 | 380 segundos |

Las esperas observadas muestran un incremento progresivo y se encuentran dentro de los rangos esperados para un comportamiento exponencial.

La documentación de evidencias señala que la variación frente a los valores teóricos podría corresponder a una variación aleatoria de los intervalos. Esta explicación es una interpretación del equipo y no fue verificada independientemente en la documentación de Airflow.

### 6.3. Segunda medición: GitHub Codespaces

El 7 de noviembre de 2026 se realizó una segunda ronda de pruebas en GitHub Codespaces, utilizando la misma configuración de reintentos.

Los resultados fueron:

| Intento | Hora de inicio | Espera desde el fallo anterior |

|---|---|---:|

| 1 | 11:19:07 | — |

| 2 | 11:20:43 | 96 segundos |

| 3 | 11:24:15 | 212 segundos |

| 4 | 11:29:01 | 286 segundos |

Los tres intervalos se encuentran dentro de los rangos observados en la primera ronda.

Además, ninguno de los intervalos alcanzó el límite máximo de 600 segundos.

### 6.4. Comparación de resultados

| Reintento | Docker local | Codespaces |

|---|---:|---:|

| Primero | 91 s | 96 s |

| Segundo | 183 s | 212 s |

| Tercero | 380 s | 286 s |

Las dos mediciones confirman que el tiempo de espera aumenta progresivamente entre los intentos.

La diferencia entre los valores de ambas rondas evidencia que los intervalos reales no son idénticos en cada ejecución.

### 6.5. Justificación de la configuración

La política de tres reintentos ofrece un equilibrio entre tolerancia a fallos y tiempo de recuperación.

Un número inferior podría resultar insuficiente cuando existen fallos temporales. Por otra parte, aumentar considerablemente los reintentos podría prolongar innecesariamente una corrida cuando el error es permanente.

Por esta razón, la configuración actual permite tres reintentos y establece un máximo de espera para limitar el tiempo consumido por una tarea fallida.

Es importante considerar que no todos los errores son transitorios. La documentación de T1.3 identifica como mejora propuesta que los errores permanentes de la compuerta de calidad no deberían necesariamente heredar los mismos reintentos de las tareas técnicas.

---

## 7. Evaluación del rendimiento y la ruta crítica

### 7.1. Concepto de ruta crítica

La ruta crítica corresponde a la secuencia de tareas dependientes que determina el tiempo mínimo necesario para completar el flujo de procesamiento.

Cuando existen operaciones independientes, el tiempo total no corresponde necesariamente a la suma de todas las duraciones individuales, porque algunas tareas pueden ejecutarse simultáneamente.

En DATAMARCA, la rama Silver y la rama Gold presentan dependencias diferentes, por lo que su ejecución paralela permite reducir el tiempo total.

### 7.2. Estimación inicial

Antes de implementar el DAG, se realizó una estimación utilizando mediciones del pipeline de la Unidad 3.

Los resultados fueron:

| Indicador | Tiempo |

|---|---:|

| Ejecución secuencial estimada | 140,75 s |

| Rama Silver estimada | 55,88 s |

| Rama Gold estimada | 87,84 s |

| Ruta crítica estimada | 87,84 s |

La ruta crítica estimada correspondía a la rama Gold, debido a las operaciones de unión, agregación y persistencia.

Esta estimación suponía que las tareas paralelas no competían por recursos y no incluía completamente los costos de iniciar procesos independientes de Spark.

### 7.3. Medición con Airflow en Docker

El 6 de noviembre de 2026 se ejecutó una corrida exitosa del DAG en Docker local.

Las duraciones individuales registradas fueron:

| Tarea | Duración |

|---|---:|

| `ingestar_transacciones` | 1,00 s |

| `ingestar_logs` | 0,14 s |

| `validar_transacciones` | 22,30 s |

| `validar_logs` | 22,47 s |

| `compuerta_calidad` | 0,08 s |

| `limpiar_transacciones` | 14,88 s |

| `limpiar_logs` | 14,96 s |

| `unir` | 18,23 s |

| `agregar` | 27,13 s |

| `persistir_silver` | 45,79 s |

| `persistir_gold` | 19,98 s |

Los indicadores generales fueron:

| Indicador | Tiempo |

|---|---:|

| Suma de las tareas en secuencia | 187,0 s |

| Ruta crítica calculada | 103,7 s |

| Duración real de la corrida | 116,2 s |

La ruta crítica medida corresponde a la rama Gold.

La duración real fue aproximadamente 12,5 segundos superior a la ruta crítica calculada. Esta diferencia se atribuye posiblemente a costos de planificación y arranque de procesos, aunque no fue medida de forma independiente.

### 7.4. Comparación de rendimiento

La diferencia entre la ejecución secuencial y la duración real fue:

187,0 s − 116,2 s = 70,8 s

El ahorro relativo frente a la suma secuencial es aproximadamente:

(70,8 / 187,0) × 100 = 37,86 %

Esto demuestra que el paralelismo del DAG permitió reducir el tiempo de ejecución en la prueba realizada.

No obstante, el resultado corresponde a un conjunto de 5.000 registros por fuente y a un equipo local. No debe interpretarse como una estimación del rendimiento en producción.

### 7.5. Variación entre ejecuciones

Las cuatro corridas exitosas de la primera ronda registraron duraciones entre 116 y 157 segundos.

En la segunda ronda, realizada en Codespaces, la ejecución exitosa tuvo una duración aproximada de 1 minuto y 33 segundos.

Las diferencias pueden estar relacionadas con el entorno de ejecución, los recursos disponibles y el costo de iniciar cada proceso. Sin embargo, la evidencia disponible no permite establecer una causa única.

Por esta razón, se recomienda realizar pruebas adicionales con diferentes volúmenes de datos y recursos computacionales antes de establecer compromisos de rendimiento.

---

## 8. Manejo de errores y recuperación

### 8.1. Estrategia de manejo de errores

Cada tarea ejecuta el archivo:

`src/orquestacion/tareas.py`

mediante un `BashOperator`.

El código implementa un mecanismo común de ejecución que captura excepciones, registra el error, almacena el estado de la tarea y devuelve un código de salida que permite a Airflow identificar el fallo.

Entre los mecanismos implementados se encuentran:

- Captura de excepciones y registro del traceback.

- Almacenamiento del estado de ejecución por tarea.

- Verificación previa de la existencia y estructura de los archivos de entrada.

- Separación de los errores técnicos y los errores de calidad mediante `CompuertaCalidadError`.

- Verificación de la existencia de los resultados intermedios.

- Cierre de las sesiones Spark mediante `spark.stop()`.

- Aislamiento de errores en la tarea de notificación.

- Registro de estados que permiten identificar tareas interrumpidas.

### 8.2. Comportamiento ante una falla

La tarea `notificar` utiliza la regla de disparo `all_done`, lo que permite que se ejecute después de que las tareas anteriores hayan finalizado, independientemente de su resultado.

Esta decisión es importante porque una notificación que dependa únicamente del éxito de las tareas no podría informar correctamente cuando ocurre un fallo.

Por otro lado, la tarea `corrida_completa` permite que el estado final del DAG represente el resultado del procesamiento y no solamente el éxito de la notificación.

Por lo tanto, una corrida puede tener una notificación exitosa y, al mismo tiempo, permanecer marcada como fallida si alguna etapa de datos no se completó correctamente.

### 8.3. Prueba de fallo controlado

En las pruebas se provocó un fallo en `limpiar_logs` mediante:

`DATAMARCA_FALLAR_EN=limpiar_logs`

El resultado observado fue:

| Grupo de tareas | Resultado |

|---|---|

| Ingestión | Success |

| Validación | Success |

| Compuerta de calidad | Success |

| `limpiar_transacciones` | Success |

| `limpiar_logs` | Failed después de cuatro intentos |

| `unir` | Upstream_failed |

| `agregar` | Upstream_failed |

| `persistir_silver` | Upstream_failed |

| `persistir_gold` | Upstream_failed |

| `notificar` | Success |

| `corrida_completa` | Upstream_failed |

La prueba demuestra que las tareas dependientes de la operación fallida no continúan ejecutándose, mientras que la rama independiente puede finalizar.

También demuestra que la notificación se ejecuta a pesar del fallo y que la corrida completa no se considera exitosa.

### 8.4. Idempotencia y seguridad de los reintentos

El diseño busca que las tareas puedan repetirse sin generar duplicación de resultados.

Las tareas T1 a T9 leen o calculan resultados intermedios. Las tareas de persistencia T10 y T11 utilizan sobrescritura dinámica de particiones, lo que permite reemplazar los resultados correspondientes a una corrida sin duplicarlos.

La notificación puede repetirse porque su operación consiste en generar y enviar un resumen.

La estrategia de idempotencia se complementa con la separación de resultados por identificador de corrida.

---

## 9. Retención y almacenamiento intermedio

### 9.1. Zona de trabajo

Las tareas comparten resultados intermedios mediante una zona de trabajo:

`data/_staging/<id_corrida>/`

La ruta se configura mediante la variable:

`DATAMARCA_STAGING_ROOT`

Esta estructura permite almacenar los resultados necesarios entre tareas independientes, incluyendo:

- Registros válidos de cada fuente.

- Métricas de calidad.

- Registros limpios.

- Datos unidos.

- Agregaciones.

- Estados individuales de las tareas.

El identificador de corrida permite separar los resultados de diferentes ejecuciones y facilita la recuperación y el análisis de fallos.

### 9.2. Política de retención

La implementación documentada en T1.3 establece la siguiente política:

| Tipo de corrida | Política | Justificación |

|---|---|---|

| Exitosa | Se eliminan los archivos intermedios y se conservan estados y métricas | Los resultados finales ya están almacenados en Silver y Gold |

| Fallida | Se conserva completa durante siete días | Permite investigar el error o reanudar la corrida |

| Corridas antiguas | Se eliminan las carpetas cuya antigüedad supera siete días | Evita acumulación innecesaria de almacenamiento |

La configuración utiliza:

- `DATAMARCA_LIMPIAR_STAGING=true`

- `DATAMARCA_RETENCION_DIAS=7`

Es importante distinguir que estos valores se proponen en el archivo `.env.example`, mientras que los valores predeterminados del código no activan automáticamente la limpieza ni la purga por antigüedad.

La política debe configurarse en el entorno para que se aplique.

### 9.3. Justificación

La retención busca equilibrar la disponibilidad de información para auditoría con el uso eficiente del almacenamiento.

Las corridas exitosas no necesitan conservar indefinidamente los archivos intermedios, porque pueden regenerarse desde las fuentes originales.

En cambio, las corridas fallidas requieren conservar los datos temporales durante un periodo que permita investigar los errores y reanudar las tareas pendientes.

La política de siete días proporciona un margen de revisión que cubre varios ciclos de ejecución diaria.

---

## 10. Monitoreo y trazabilidad

La arquitectura genera diferentes tipos de registros para permitir el seguimiento de las ejecuciones.

| Elemento | Ubicación | Información |

|---|---|---|

| Logs de Airflow | Volumen `airflow-logs` | Eventos de cada tarea e intento |

| Estado por tarea | `data/_staging/<corrida>/estado/` | Estado, intento, duración y error |

| Métricas de calidad | `data/_staging/<corrida>/metricas/` | Registros leídos, válidos, rechazados y porcentaje de rechazo |

| Resumen de corrida | `reports/orquestacion/resumen_<corrida>.md` y `.json` | Resultado general y estado de las tareas |

| Metadatos de Airflow | PostgreSQL | Ejecuciones, estados e información operativa |

| Evidencias versionadas | `reports/orquestacion/evidencia/` | Resúmenes, capturas y mediciones |

La información generada permite identificar cuándo comienza una tarea, cuánto tarda, cuántos intentos requiere y cuál fue el resultado final.

Adicionalmente, el script:

`reports/orquestacion/exportar_logs.py`

permite exportar los logs y estados de una corrida para conservar evidencias fuera del entorno de ejecución.

### 10.1. Limitaciones identificadas

La documentación de T1.3 identifica algunas brechas que deben considerarse:

- Los logs de Airflow se almacenan en un volumen local de Docker y pueden perderse si se elimina dicho volumen.

- No existe un canal externo de notificación configurado por defecto.

- El historial de intentos almacenado en los archivos de estado puede conservar únicamente el último intento.

- La cuarentena no está separada por corrida.

- El timeout global del DAG puede finalizar una corrida sin que se ejecute la tarea `notificar`.

- Algunas pruebas de timeout y evidencias de contenido de logs continúan pendientes.

Estas limitaciones deben considerarse antes de trasladar la arquitectura a un entorno productivo.

---

## 11. Evaluación de carga operativa

### 11.1. Distribución de responsabilidades

La carga operativa de Airflow se distribuye entre los componentes que administran la planificación, la ejecución, la interfaz y el almacenamiento de metadatos.

En DATAMARCA, el comportamiento esperado es:

**Scheduler**

Administra la planificación de las tareas, evalúa las dependencias y coordina los estados del DAG. Su carga está relacionada con la frecuencia de ejecución, la cantidad de tareas y el número de corridas activas.

**DAG Processor**

Analiza los archivos que definen los DAG y mantiene disponibles sus estructuras para el Scheduler. Su carga depende de la cantidad y complejidad de las definiciones.

**API Server**

Proporciona la interfaz web y los servicios de consulta y administración. Su utilización depende de las consultas realizadas por los usuarios y de la interacción con el entorno.

**LocalExecutor**

Ejecuta las tareas localmente dentro del entorno de Airflow. Como DATAMARCA utiliza procesos Spark independientes, el consumo de CPU y memoria de las tareas debe considerarse junto con el consumo de los servicios de orquestación.

**PostgreSQL**

Almacena metadatos de las ejecuciones, estados y registros operativos. Su carga depende de la frecuencia de las corridas y de la cantidad de información que debe consultar y actualizar Airflow.

### 11.2. Consideraciones sobre los recursos

La arquitectura actual utiliza `LocalExecutor`, por lo que no dispone de workers independientes.

Esto significa que los procesos de ejecución y los servicios de orquestación comparten los recursos del entorno Docker.

A medida que aumente la cantidad de tareas o el volumen de datos, será necesario evaluar:

- CPU disponible para el scheduler y los procesos Spark.

- Memoria disponible para las sesiones de Spark.

- Capacidad de almacenamiento para los logs y resultados intermedios.

- Recursos destinados a PostgreSQL.

- Tiempo de inicio de los procesos independientes.

- Cantidad de tareas que pueden ejecutarse simultáneamente.

No se dispone de mediciones de CPU, memoria o consumo de disco por componente en las evidencias analizadas. Por lo tanto, no es posible establecer todavía una capacidad máxima de procesamiento o dimensionamiento productivo.

### 11.3. Escalabilidad

La arquitectura actual es adecuada para la prueba de concepto y las corridas de desarrollo realizadas.

Sin embargo, si el volumen de datos y la frecuencia de ejecución aumentan, será necesario evaluar una arquitectura de ejecución distribuida y una separación más clara entre los servicios de orquestación y los recursos de procesamiento.

También se recomienda implementar monitoreo de infraestructura y políticas de respaldo para PostgreSQL y los logs.

### 11.4. Conclusión sobre la carga operativa para DATAMARCA

La implementación de Apache Airflow proporciona a DATAMARCA control sobre la planificación, ejecución, seguimiento y recuperación de los procesos del pipeline. Sin embargo, al utilizar una arquitectura autogestionada con Docker Compose y LocalExecutor, el equipo debe asumir directamente la administración y disponibilidad de los componentes de orquestación.

Esto implica mantener los servicios de Airflow y PostgreSQL, supervisar las ejecuciones del DAG, revisar los logs, atender fallos, gestionar los reintentos, controlar el almacenamiento y realizar respaldos de la información operativa. También será necesario monitorear el consumo de CPU y memoria, especialmente porque los procesos Spark comparten recursos con los servicios de orquestación.

En consecuencia, Airflow reduce la necesidad de desarrollar mecanismos propios para coordinar las tareas, pero no elimina la carga de administración de infraestructura. DATAMARCA deberá contar con responsables técnicos que garanticen el mantenimiento, monitoreo y recuperación del entorno.

Para una etapa productiva se recomienda evaluar el esfuerzo de administración frente a una alternativa administrada compatible con Azure, considerando los costos, la disponibilidad, la escalabilidad y las capacidades operativas del equipo.

---

## 12. Resultados de las pruebas

La validación de la arquitectura se realizó en dos rondas.

| Ronda | Fecha | Entorno | Resultado |

|---|---|---|---|

| 1 | 6 de noviembre de 2026 | Docker local | Corrida exitosa y fallo controlado con medición de backoff |

| 2 | 7 de noviembre de 2026 | GitHub Codespaces | Corrida exitosa y fallo controlado con capturas de Airflow y auditoría |

### 12.1. Corrida exitosa

En la segunda ronda se obtuvo una ejecución exitosa con una duración aproximada de 1 minuto y 33 segundos.

Las trece tareas registradas finalizaron correctamente en el primer intento.

La evidencia permite verificar el orden de ejecución, los estados y la duración de las tareas mediante las vistas de Airflow.

### 12.2. Corrida fallida

En la corrida con fallo controlado, la tarea `limpiar_logs` agotó los tres reintentos adicionales.

La corrida tuvo una duración de 10 minutos y 24 segundos.

El resultado fue:

- Siete tareas en estado success.

- Una tarea en estado failed.

- Cinco tareas en estado upstream_failed.

La tarea `notificar` finalizó correctamente, aunque la corrida completa se marcó como fallida.

### 12.3. Valor de las pruebas

Las pruebas permiten validar tres aspectos fundamentales:

1. Las dependencias del DAG impiden que las tareas posteriores se ejecuten cuando falla una tarea requerida.

2. Los reintentos permiten observar el comportamiento del backoff exponencial.

3. La notificación se ejecuta incluso cuando ocurre un fallo intermedio.

Las evidencias se encuentran documentadas en:

`reports/orquestacion/evidencia/`

Las pruebas se realizaron con 5.000 registros por fuente, por lo que sus resultados deben interpretarse como evidencia de funcionamiento y no como una prueba de capacidad productiva.

---

## 13. Riesgos y recomendaciones

A partir de la implementación y de las brechas identificadas en T1.3, se establecen las siguientes recomendaciones:

| Riesgo o limitación | Impacto | Recomendación |

|---|---|---|

| Uso de LocalExecutor | Competencia por CPU y memoria entre tareas Spark y servicios de Airflow | Evaluar un ejecutor distribuido cuando aumente la carga |

| Logs en volumen local | Pérdida de registros al eliminar el volumen | Configurar almacenamiento remoto y retención |

| Ausencia de alertas externas por defecto | Un fallo podría pasar inadvertido si nadie consulta Airflow | Configurar un webhook seguro o SMTP |

| Timeout global sin notificación garantizada | Una corrida puede finalizar sin resumen | Implementar y probar un callback de fallo del DAG |

| Historial de intentos limitado | Dificultad para reconstruir la secuencia completa de recuperación | Registrar los eventos de cada intento |

| Cuarentena compartida entre corridas | Posible pérdida de registros rechazados anteriores | Separar la cuarentena por identificador de corrida |

| Ausencia de métricas de recursos | No es posible dimensionar con precisión el entorno | Medir CPU, memoria, almacenamiento y duración por servicio |

| Pruebas limitadas a volúmenes pequeños | No se conoce el comportamiento a escala productiva | Ejecutar pruebas con mayor volumen y carga concurrente |

Estas recomendaciones no deben interpretarse como funcionalidades que ya estén implementadas. Corresponden a oportunidades de mejora identificadas durante la revisión técnica.

---

## 14. Conclusiones

La implementación de Apache Airflow permitió incorporar una capa de orquestación al pipeline batch de DATAMARCA S.A., organizando las tareas mediante un DAG con dependencias explícitas, ejecución paralela y mecanismos de recuperación ante fallos.

La selección de Airflow se considera adecuada para la arquitectura actual porque permite reutilizar los módulos Python y PySpark existentes, mantener el control sobre las dependencias y ejecutar el entorno mediante Docker.

Las pruebas realizadas demuestran que el DAG puede completar una corrida exitosa y gestionar un fallo controlado en una tarea intermedia. También se verificó que las tareas dependientes no continúan cuando falta un resultado requerido y que la notificación puede ejecutarse independientemente del éxito de las etapas de procesamiento.

En términos de rendimiento, la medición de Docker local registró una duración real de 116,2 segundos frente a una suma secuencial de 187,0 segundos. La diferencia de 70,8 segundos evidencia el beneficio del paralelismo para el conjunto de datos utilizado.

Asimismo, las dos rondas de pruebas permitieron observar el comportamiento del backoff exponencial, con intervalos crecientes entre reintentos.

Sin embargo, la arquitectura todavía presenta limitaciones relacionadas con el almacenamiento local de logs, la ausencia de alertas externas por defecto, el timeout global y la falta de mediciones de recursos por componente.

En consecuencia, se concluye que la solución cumple su propósito como arquitectura de orquestación para el entorno de desarrollo y validación de DATAMARCA. Para un despliegue productivo será necesario fortalecer la observabilidad, la infraestructura de ejecución, la persistencia de los metadatos y las pruebas de escalabilidad.

---

## 15. Referencias internas del proyecto

La elaboración de esta documentación se fundamenta en los siguientes archivos del repositorio:

1. `airflow/README.md` — Configuración, instalación y ejecución de Airflow.

2. `airflow/docker-compose.yaml` — Definición de servicios y volúmenes.

3. `airflow/dags/datamarca_pipeline.py` — Implementación del DAG.

4. `src/orquestacion/tareas.py` — Lógica de ejecución de las tareas.

5. `docs/orquestacion/dependencias.md` — Dependencias, paralelismo y ruta crítica.

6. `docs/orquestacion/manejo_errores_logging.md` — Manejo de errores, trazabilidad y retención.

7. `reports/orquestacion/evidencia/README.md` — Resultados de las dos rondas de pruebas.

8. `reports/orquestacion/evidencia/corrida_exitosa.md` — Evidencia de ejecución exitosa.

9. `reports/orquestacion/evidencia/fallo_forzado_limpiar_logs.md` — Evidencia de recuperación y fallo controlado.

10. `docs/Justificacion_batch_vs_streaming.md` — Justificación del enfoque de procesamiento batch.