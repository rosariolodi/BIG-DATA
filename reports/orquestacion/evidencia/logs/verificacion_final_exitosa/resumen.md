# Corrida manual__2026-11-09T14-50-01.226020-00-00: EXITOSA

- transacciones: leídos=5000 válidos=5000 rechazados=0
- logs_ecommerce: leídos=5000 válidos=5000 rechazados=0
- Tiempo acumulado de tareas: 130.7 s

| Tarea | Estado | Segundos | Intento |
|---|---|---|---|
| ingestar_transacciones | exitosa | 0.17 | 1 |
| ingestar_logs | exitosa | 0.01 | 1 |
| validar_transacciones | exitosa | 20.32 | 1 |
| validar_logs | exitosa | 20.02 | 1 |
| compuerta_calidad | exitosa | 0.0 | 1 |
| limpiar_transacciones | exitosa | 13.27 | 1 |
| limpiar_logs | exitosa | 13.15 | 1 |
| unir | exitosa | 17.43 | 1 |
| agregar | exitosa | 14.75 | 1 |
| persistir_silver | exitosa | 19.35 | 1 |
| persistir_gold | exitosa | 12.19 | 1 |