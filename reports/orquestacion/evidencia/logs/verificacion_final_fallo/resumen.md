# Corrida manual__2026-11-09T14-58-17.263515-00-00: CON FALLO

- Modo demostración: DATAMARCA_FALLAR_EN=limpiar_logs
- FALLÓ `limpiar_logs` (intento 4): RuntimeError: FALLO FORZADO en 'limpiar_logs' (intento 4) por DATAMARCA_FALLAR_EN
- No se ejecutaron por la falla: unir, agregar, persistir_silver, persistir_gold
- Completadas antes/durante el fallo: ingestar_transacciones, ingestar_logs, validar_transacciones, validar_logs, compuerta_calidad, limpiar_transacciones

| Tarea | Estado | Segundos | Intento |
|---|---|---|---|
| ingestar_transacciones | exitosa | 0.38 | 1 |
| ingestar_logs | exitosa | 0.01 | 1 |
| validar_transacciones | exitosa | 22.94 | 1 |
| validar_logs | exitosa | 22.96 | 1 |
| compuerta_calidad | exitosa | 0.0 | 1 |
| limpiar_transacciones | exitosa | 8.35 | 1 |
| limpiar_logs | fallida | 0.0 | 4 |
| unir | no_ejecutada | - | - |
| agregar | no_ejecutada | - | - |
| persistir_silver | no_ejecutada | - | - |
| persistir_gold | no_ejecutada | - | - |