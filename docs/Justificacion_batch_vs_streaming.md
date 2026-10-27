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

El análisis identificó 3.792 usuarios únicos, sin valores nulos ni identificadores de evento duplicados. Las marcas de tiempo están expresadas en UTC y el archivo se encuentra ordenado por identificador del evento, no por fecha.

Se identificaron 1.066 eventos, equivalentes al 21,3 % del total, con fechas posteriores al último registro de transacciones POS. Esta diferencia debe considerarse al integrar las fuentes y requiere verificar la cobertura temporal de cada conjunto de datos.

También se encontraron limitaciones para relacionar ambos sistemas: los eventos de comercio electrónico no contienen monto de compra, tienda ni identificador de pedido, y las categorías de productos no coinciden con las utilizadas en el POS.

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

Por otra parte, las limitaciones identificadas para relacionar las fuentes y la necesidad de validar la consistencia de los datos refuerzan la conveniencia de establecer primero un proceso de transformación y control de calidad reproducible.

No obstante, la elección definitiva también debe considerar los requerimientos de actualización de los indicadores y la frecuencia real de llegada de los datos, aspectos que no pueden determinarse únicamente a partir de los archivos analizados.