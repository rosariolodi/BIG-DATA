# Reglas de validación de calidad de datos (Tarea 1.3)

Proyecto Spark Pipeline - DATAMARCA S.A.

Este documento **define** las reglas de calidad (integridad y completitud) que se aplicarán en la fase de ingestión. La implementación en Spark corresponde a la tarea 2.2. Los formatos, tipos y dominios salen de `src/config/schemas.py` y del EDA (`notebooks/EDA_DATAMARCA_transacciones (1).ipynb`).

## 1. Dimensiones

| Dimensión | Qué verifica |
|---|---|
| Completitud | Los campos obligatorios no son nulos ni vacíos. |
| Integridad | El dato tiene sentido: formato de llaves, unicidad, tipo parseable, rangos y dominios. |

## 2. Severidad y tratamiento

| Severidad | Qué pasa con el registro |
|---|---|
| **Crítica** | Se rechaza y va a **cuarentena** con el motivo. No continúa a las transformaciones. |
| **Advertencia** | Continúa su camino y queda **marcado** con el ID de la regla en la columna `dq_warnings`. |

Un registro puede fallar varias reglas a la vez. En cuarentena se guardan **todos** los IDs que falló, no solo el primero.

Los dominios (tiendas, categorías, métodos de pago, eventos, dispositivos) son advertencia y no rechazo, porque pueden crecer con datos reales.

## 3. Reglas para transacciones POS (CSV)

Columnas: `transaction_id`, `timestamp`, `store_id`, `product_category`, `amount_cop`, `payment_method`.

| ID | Dimensión | Columna(s) | Condición válida | Severidad |
|---|---|---|---|---|
| T-C01 | Completitud | las 6 columnas | No son nulas ni cadenas vacías | Crítica |
| T-I01 | Integridad | `transaction_id` | Cumple `^TXN\d{5}$` | Crítica |
| T-I02 | Integridad | `transaction_id` | Es único en la carga | Crítica |
| T-I03 | Integridad | `timestamp` | Se puede interpretar como `yyyy-MM-dd HH:mm:ss` | Crítica |
| T-I04 | Integridad | `store_id` | Cumple `^[A-Z]{3}-\d{2}$` | Crítica |
| T-I05 | Integridad | `amount_cop` | Es numérico y mayor que 0 | Crítica |
| T-I06 | Integridad | `_corrupt_record` | Es nulo (el registro se leyó bien) | Crítica |
| T-D01 | Integridad | `store_id` | Está en `BAR-01, BOG-01, BOG-02, CAL-01, MED-01, MED-02` | Advertencia |
| T-D02 | Integridad | `product_category` | Está en `Aseo, Bebidas, Carnes, Frutas, Granos, Licores, Lácteos, Panadería, Snacks, Verduras` | Advertencia |
| T-D03 | Integridad | `payment_method` | Está en `App, Bono, Efectivo, Tarjeta` | Advertencia |
| T-D04 | Integridad | `amount_cop` | No es un outlier extremo (fuera de Q3 + 3·IQR) | Advertencia |

## 4. Reglas para logs de e-commerce (JSON)

Columnas: `event_id`, `timestamp`, `user_id`, `event_type`, `device`, `product_category`, `session_duration_sec`.

| ID | Dimensión | Columna(s) | Condición válida | Severidad |
|---|---|---|---|---|
| L-C01 | Completitud | las 7 columnas | No son nulas ni cadenas vacías | Crítica |
| L-I01 | Integridad | `event_id` | Cumple `^evt_\d{5}$` | Crítica |
| L-I02 | Integridad | `event_id` | Es único en la carga | Crítica |
| L-I03 | Integridad | `timestamp` | Se puede interpretar como ISO-8601 UTC (`yyyy-MM-dd'T'HH:mm:ssX`) | Crítica |
| L-I04 | Integridad | `user_id` | Cumple `^usr_\d{4}$` | Crítica |
| L-I05 | Integridad | `session_duration_sec` | Es entero y mayor que 0 | Crítica |
| L-I06 | Integridad | `_corrupt_record` | Es nulo (el registro se leyó bien) | Crítica |
| L-D01 | Integridad | `event_type` | Está en `page_view, search, add_to_cart, remove_from_cart, checkout_start, purchase` | Advertencia |
| L-D02 | Integridad | `device` | Está en `desktop, mobile, tablet` | Advertencia |
| L-D03 | Integridad | `product_category` | Está en `beauty, clothing, electronics, home, sports` | Advertencia |
| L-D04 | Integridad | `session_duration_sec` | Está entre 10 y 1800 (rango observado) | Advertencia |

## 5. Registro de resultados

**Tabla de cuarentena** (una por fuente, o una con columna `fuente`):

| Columna | Contenido |
|---|---|
| `fuente` | `transacciones` o `logs_ecommerce` |
| `registro_original` | El registro tal como llegó |
| `reglas_fallidas` | Lista de IDs de reglas críticas incumplidas |
| `fecha_ingestion` | Marca de tiempo de la ejecución |

**Métricas por ejecución** :

| Métrica | Cálculo |
|---|---|
| Registros leídos | Total de la carga |
| Registros válidos | Los que pasan todas las reglas críticas |
| Registros en cuarentena | Los que incumplen al menos una regla crítica |
| % de rechazo | Cuarentena / leídos |
| Fallos por regla | Conteo y porcentaje por cada ID (críticas y advertencias) |

## 6. Estado de los datos actuales

Con los 5.000 registros de cada archivo en `data/raw/`, **ninguna regla crítica rechaza registros**: no hay nulos ni duplicados, los formatos de llave se cumplen y todos los timestamps se interpretan. Los dominios coinciden con los listados arriba. Por eso, para evidenciar que las reglas funcionan, la tarea 2.2 puede probarlas con registros dañados a propósito (llave mal formada, monto negativo, campo vacío, duplicado).