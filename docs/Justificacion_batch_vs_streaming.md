# 2.3. Justificación técnica de la modalidad de procesamiento: Batch vs. Streaming

## 1. Objetivo

El objetivo de esta documentación es justificar técnicamente la elección de la modalidad de procesamiento de datos para el proyecto DATAMARCA, comparando las alternativas batch y streaming.

La decisión tendrá en cuenta las características de las fuentes de información, los resultados del análisis exploratorio de datos (EDA), las necesidades de procesamiento y la complejidad técnica de cada alternativa.

Se busca seleccionar una modalidad que permita procesar las transacciones de los puntos de venta (POS) y los eventos del comercio electrónico de manera eficiente, confiable y acorde con los objetivos analíticos del proyecto.

## 2. Características de las fuentes de datos

El proyecto DATAMARCA trabaja con dos fuentes principales de información: las transacciones generadas en los puntos de venta (POS) y los eventos registrados en el canal de comercio electrónico. El análisis exploratorio de datos permite identificar sus características y establecer criterios para seleccionar la modalidad de procesamiento.

### 2.1. Transacciones de puntos de venta (POS)

La primera fuente corresponde al archivo `DATAMARCA_transacciones_5000_real.csv`, que contiene 5.000 registros de transacciones comerciales. Entre las variables analizadas se encuentran el identificador de la transacción, la marca de tiempo, la tienda, la categoría del producto, el monto en pesos colombianos y el método de pago.

La cobertura temporal comprende desde el 15 de septiembre hasta el 15 de octubre de 2026. El análisis de calidad identificó ausencia de valores nulos, identificadores de transacción duplicados y montos iguales o inferiores a cero. También se encontraron cinco marcas de tiempo repetidas, correspondientes a transacciones que pueden ser distintas.

El notebook incluye análisis de ventas por tienda, categoría y método de pago, además de patrones temporales por fecha, día de la semana y hora. Estos análisis permiten obtener indicadores comerciales a partir de conjuntos de datos consolidados.

### 2.2. Eventos de comercio electrónico

La segunda fuente corresponde a un archivo JSON con 5.000 eventos relacionados con la actividad de los usuarios en el canal de comercio electrónico. El conjunto de datos incluye variables como el identificador del evento, la marca de tiempo, el usuario, el tipo de evento, el dispositivo, la categoría del producto y la duración de la sesión.

El archivo vigente contiene 672 usuarios únicos, sin valores nulos ni identificadores de evento duplicados, y todos los eventos son de tipo `purchase`. Las marcas de tiempo llevan el sufijo `Z` (UTC) y cubren del 15 de septiembre al 15 de octubre de 2026, el mismo período que las transacciones POS.

Ambas fuentes comparten el identificador de usuario (`user_id`): los 672 usuarios de los logs son los mismos de las transacciones, y las 5.000 transacciones tienen un usuario presente en los logs. Las categorías de producto también coinciden entre las dos fuentes. Los eventos no contienen monto de compra, tienda ni identificador de pedido, por lo que el cruce se hace por `user_id` y las métricas de actividad web se resumen por usuario antes de unirlas a las transacciones.

### 2.3. Implicaciones para la modalidad de procesamiento

Las dos fuentes permiten realizar análisis históricos y calcular indicadores mediante procesos periódicos. Sin embargo, el hecho de disponer de archivos CSV y JSON no demuestra por sí solo que el procesamiento batch sea la única alternativa viable.

La selección debe considerar también la frecuencia con la que llegan los datos, la latencia requerida por el negocio, el volumen esperado en producción y la necesidad de generar alertas inmediatas.

Por lo tanto, las características observadas justifican evaluar el procesamiento batch como alternativa inicial, mientras se determina si existen requerimientos operativos que hagan necesario incorporar streaming.

## 3. Comparación técnica entre Batch y Streaming

Para seleccionar la modalidad de procesamiento adecuada para DATAMARCA, se comparan las alternativas batch y streaming teniendo en cuenta las características de los datos, los requerimientos analíticos y la complejidad de implementación.

| Criterio técnico | Procesamiento Batch | Procesamiento Streaming |
|---|---|---|
| Funcionamiento | Procesa conjuntos de datos acumulados en intervalos definidos. | Procesa eventos continuamente a medida que se reciben. |
| Latencia | Los resultados están disponibles después de ejecutar cada lote. | Permite obtener resultados con baja latencia. |
| Fuentes de datos | Adecuado para archivos CSV, JSON y datos históricos. | Adecuado para flujos continuos de eventos procedentes de sistemas operativos. |
| Análisis histórico | Facilita agregaciones, comparaciones temporales y generación de reportes. | Permite analizar eventos en tiempo real y conservarlos para análisis históricos. |
| Calidad de los datos | Facilita validar conjuntos completos antes de publicar los resultados. | Requiere gestionar validaciones, eventos tardíos, duplicados y errores durante el flujo. |
| Complejidad operativa | Generalmente menor cuando las cargas son periódicas. | Requiere administrar la recepción continua de eventos, la recuperación y el procesamiento del flujo. |
| Uso de recursos | Puede ejecutarse en ventanas programadas según la demanda. | Puede requerir recursos disponibles continuamente. |
| Aplicación en DATAMARCA | Análisis de ventas, indicadores comerciales y comportamiento histórico de usuarios. | Alertas inmediatas, monitoreo de transacciones y detección de anomalías durante la operación. |

### 3.1. Análisis de la comparación

La comparación muestra que batch y streaming responden a necesidades diferentes. Batch resulta apropiado cuando los resultados pueden generarse a partir de información acumulada y no existe una exigencia de respuesta inmediata. Streaming es más conveniente cuando el negocio necesita reaccionar a los eventos poco después de que ocurren.

En DATAMARCA, el notebook se enfoca en el análisis exploratorio de transacciones POS y eventos de comercio electrónico, incluyendo distribuciones, patrones temporales y características de las fuentes. Estos análisis pueden realizarse mediante ejecuciones periódicas.

Por otra parte, el cruce de las fuentes por `user_id` (resumiendo antes los logs a una fila por usuario para no multiplicar las ventas) y la necesidad de validar la consistencia de los datos refuerzan la conveniencia de establecer primero un proceso de transformación y control de calidad reproducible.

## 4. Decisión: procesamiento batch

**Se implementa procesamiento batch.** El pipeline lee los archivos CSV y JSON, valida su calidad, los limpia, los cruza, calcula las agregaciones de negocio y persiste los resultados en cada ejecución.

### 4.1. Criterios que sustentan la decisión

| Criterio | Evidencia en el proyecto | Conclusión |
|---|---|---|
| Forma de entrega de los datos | Los datos llegan como archivos (`DATAMARCA_transacciones_5000_real.csv` y `ecommerce_logs_5000.json`), no como un flujo de eventos | Favorece batch |
| Tipo de consulta | Los resultados son históricos: ventas por día, tienda, categoría y método de pago, y clientes con más compras | Favorece batch |
| Latencia | El alcance de esta unidad no define un requisito de respuesta por evento | Streaming no es necesario |
| Calidad de datos | Las reglas de `docs/reglas_calidad.md` (unicidad, formatos, cuarentena y métricas de rechazo) se evalúan sobre el lote completo | Favorece batch |
| Reproceso | Reprocesar el mismo lote da el mismo resultado, sin duplicados (`tests/test_idempotencia.py`) | Favorece batch |
| Costo y operación | El pipeline corre como un job programado que se ejecuta, termina y libera el clúster; streaming exigiría recursos disponibles de forma continua | Favorece batch |

### 4.2. Relación con la arquitectura de la Unidad 2

El informe de la Unidad 2 no recomienda una arquitectura basada exclusivamente en streaming: plantea un procesamiento híbrido, similar a una arquitectura Lambda simplificada, que separa las cargas batch de las de streaming. La modalidad implementada corresponde a la **mitad batch** de esa arquitectura. La mitad streaming (ingesta de eventos con Azure Event Hubs) no se implementa en esta unidad y queda como evolución.

### 4.3. Alternativa no implementada: streaming

Streaming sería la opción adecuada si el negocio exigiera alertas inmediatas, monitoreo de transacciones o detección de anomalías durante la operación (ver la última fila de la comparación de la sección 3). Adoptarla implicaría:

- Administrar la recepción continua de eventos y la recuperación ante fallos.
- Definir **ventanas temporales** para agrupar los eventos por tiempo.
- Gestionar los **datos tardíos** (*late data*) mediante marcas de agua (*watermarks*).
- Mantener recursos disponibles de forma permanente, con el costo asociado.

## 5. Referente conceptual: Apache Beam

Apache Beam propone un **modelo unificado** para batch y streaming: sus SDK usan las mismas clases y las mismas transformaciones para datos acotados (por ejemplo, un archivo) y no acotados (por ejemplo, un flujo de eventos). Si la fuente es acotada, el pipeline puede ejecutarse en batch; si es no acotada, debe ejecutarse en modo streaming. Beam hereda del modelo Dataflow una forma de describir el procesamiento mediante cuatro preguntas:

| Pregunta | Concepto en Beam | Equivalente en este proyecto |
|---|---|---|
| ¿Qué se calcula? | Transformaciones | Limpieza, join por `user_id` y agregaciones (`src/transformations/`) |
| ¿En qué ventana de tiempo? | Ventanas por tiempo del evento | Hoy se agrupa por día (`fecha`); en streaming serían ventanas |
| ¿Cuándo se emite el resultado? | *Triggers* y *watermarks* | En batch, al terminar cada ejecución |
| ¿Cómo se refinan los resultados? | Modo de acumulación | En batch, cada corrida sobrescribe el resultado anterior |

La columna "Equivalente en este proyecto" es una analogía con nuestro pipeline batch, no una descripción de cómo funciona Beam.

La consecuencia práctica para DATAMARCA es que la lógica de negocio (limpieza, cruce y agregaciones) se conserva en gran parte al pasar a streaming, pero **no basta con cambiar la fuente de datos**: sobre datos no acotados hay que definir explícitamente ventanas por tiempo del evento y una política para los datos tardíos. En Beam, agrupar una colección no acotada exige una ventana distinta de la global o un *trigger* no predeterminado; en Spark Structured Streaming, el equivalente es definir ventanas por tiempo del evento y una marca de agua (`withWatermark`), que además no tiene efecto en una consulta batch. Elegir batch ahora no cierra el camino hacia streaming, pero esa migración requiere ese rediseño.

## 6. Conclusión

Con base en la forma de entrega de los datos, el tipo de consultas, la validación de calidad sobre lotes completos y el menor costo operativo, se adopta el procesamiento **batch** y se descarta, por ahora, streaming. La decisión es coherente con la arquitectura híbrida de la Unidad 2 y puede revisarse si el negocio incorpora un requisito de latencia por evento.

## Referencias

- Apache Beam, [Beam Overview](https://beam.apache.org/get-started/beam-overview/): modelo unificado para batch y streaming.
- Apache Beam, [Basics of the Beam model](https://beam.apache.org/documentation/basics/): colecciones acotadas y no acotadas.
- Apache Beam, [Programming Guide](https://beam.apache.org/documentation/programming-guide/): ventanas, *watermarks*, *triggers* y datos tardíos.
- Apache Spark, [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html): ventanas por tiempo del evento y marcas de agua (*watermarking*).
- Las cuatro preguntas (qué, dónde, cuándo, cómo) provienen del modelo Dataflow, origen de Beam, y se describen en el artículo "Streaming 102" de Tyler Akidau. En esta revisión se consultaron fuentes secundarias que lo resumen, no el artículo original.