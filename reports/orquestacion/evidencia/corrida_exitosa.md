# Corrida manual__2026-11-07T02-49-19.216547-00-00: EXITOSA

- transacciones: leídos=5000 válidos=5000 rechazados=0
- logs_ecommerce: leídos=5000 válidos=5000 rechazados=0
- Tiempo acumulado de tareas: 187.0 s

| Tarea | Estado | Segundos | Intento |
|---|---|---|---|
| ingestar_transacciones | exitosa | 1.0 | 1 |
| ingestar_logs | exitosa | 0.14 | 1 |
| validar_transacciones | exitosa | 22.3 | 1 |
| validar_logs | exitosa | 22.47 | 1 |
| compuerta_calidad | exitosa | 0.08 | 1 |
| limpiar_transacciones | exitosa | 14.88 | 1 |
| limpiar_logs | exitosa | 14.96 | 1 |
| unir | exitosa | 18.23 | 1 |
| agregar | exitosa | 27.13 | 1 |
| persistir_silver | exitosa | 45.79 | 1 |
| persistir_gold | exitosa | 19.98 | 1 |