# Corrida manual__2026-11-07T02-22-55.545687-00-00: CON FALLO

- FALLÓ `limpiar_logs` (intento 4): RuntimeError: FALLO FORZADO en 'limpiar_logs' (intento 4) por DATAMARCA_FALLAR_EN
- No se ejecutaron por la falla: unir, agregar, persistir_silver, persistir_gold
- Completadas antes/durante el fallo: ingestar_transacciones, ingestar_logs, validar_transacciones, validar_logs, compuerta_calidad, limpiar_transacciones

| Tarea | Estado | Segundos | Intento |
|---|---|---|---|
| ingestar_transacciones | exitosa | 0.96 | 1 |
| ingestar_logs | exitosa | 0.27 | 1 |
| validar_transacciones | exitosa | 17.09 | 1 |
| validar_logs | exitosa | 17.72 | 1 |
| compuerta_calidad | exitosa | 0.06 | 1 |
| limpiar_transacciones | exitosa | 10.75 | 1 |
| limpiar_logs | fallida | 0.01 | 4 |
| unir | no_ejecutada | - | - |
| agregar | no_ejecutada | - | - |
| persistir_silver | no_ejecutada | - | - |
| persistir_gold | no_ejecutada | - | - |