# Cierre de alertas y brecha B1 — Joel, 8 de noviembre de 2026

## Alcance y autorización

Joel confirmó en la conversación que Rosario ya había aprobado los cambios al DAG y la retirada de los archivos locales generados. El trabajo de `unidad4-alertas-b1` se integró en main mediante el [PR #34](https://github.com/rosariolodi/BIG-DATA/pull/34), commit `1728d23`, el 8 de noviembre de 2026 (Bogotá). Kinan debe ejecutar su corrida desde ese main o una revisión posterior acordada con el equipo.

## 1. Canal de alertas

Se eligió Microsoft Teams, equipo **DATAMARCA - Proyecto Big Data**, canal **Alertas - Pipeline DATAMARCA**. Los logs de Airflow y los resúmenes locales conservan el detalle de diagnóstico; Teams es el aviso externo. El Workflow recibe `{"text": "..."}` y publica ese texto como mensaje. La rama que procesa `attachments` y publica el `content` de cada tarjeta se conserva.

Airflow estaba iniciado con `docker-compose.b1.yaml`, que sustituía el webhook real por un receptor local que devolvía HTTP 202. Se añadió `airflow/docker-compose.teams.yaml`, aplicado después del override B1, para leer el webhook real de `.env` en scheduler, DAG Processor y API Server. No se versiona ni se reproduce la dirección secreta. El puerto local permanece en 8081.

## 2. Brecha B1: vencimiento del límite de la corrida

Airflow 3.3.2 deja las tareas pendientes en `skipped` cuando vence `dagrun_timeout`; `all_done` no permite que la tarea `notificar` continúe después de ese vencimiento. La solución no altera esos estados ni simula que la tarea se ejecutó: el DAG llama a `alerta_fallo_dag` mediante `on_failure_callback`. Ese respaldo envía una alerta externa con `dagrun_timeout`, aun cuando `notificar` no se ejecuta. Es un aviso independiente, no el resumen completo de `notificar`.

El límite normal se conserva en **60 minutos**. Para comprobar el mismo mecanismo se redujo temporalmente a un minuto y se demoró `ingestar_logs` 180 segundos, antes de Spark. El scheduler produjo el vencimiento real; no se marcaron estados manualmente.

| Caso | Corrida | Resultado observado | Teams |
|---|---|---|---|
| Éxito | `manual__verificacion_teams_real_20261108_01` | Corrida y tareas exitosas; 5.000 registros válidos por fuente y 0 rechazados | Mensaje creado, 201 |
| Fallo controlado | `manual__prueba_fallo_teams_20261108_01` | `validar_transacciones` falló; silver y gold no se ejecutaron; `notificar` terminó | Mensaje CON FALLO creado, 201 |
| Vencimiento B1 | `manual__verificacion_timeout_b1_20261108_01` | DAG failed, `notificar` skipped; aviso enviado por `callback_dag` | Mensaje con motivo `dagrun_timeout` creado, 201 |

El callback de B1 se volvió a invocar para la misma corrida y respondió `duplicado_omitido`; el recibo no cambió y no se realizó otro POST. Las alertas normales y de respaldo comparten el identificador persistente de la notificación.

### Evidencias

- [Estados y recibos filtrados](estados_y_recibos.json): metadatos reales de las tres corridas, origen de cada envío y prueba de deduplicación.
- [Teams: éxito](teams_corrida_exitosa.png), [Teams: fallo controlado](teams_fallo_controlado.png) y [Workflow: timeout B1](workflow_timeout_b1.jpg).
- [Mensaje de éxito en Teams](https://teams.microsoft.com/l/message/19:930b70a0167b4db8a87ef445bfdb4505@thread.tacv2/1794187427093).
- [Mensaje de fallo en Teams](https://teams.microsoft.com/l/message/19:930b70a0167b4db8a87ef445bfdb4505@thread.tacv2/1794187675396).
- [Mensaje de timeout B1 en Teams](https://teams.microsoft.com/l/message/19:930b70a0167b4db8a87ef445bfdb4505@thread.tacv2/1794188203316).

Verificación local: **86 pruebas pasaron** en `test_alertas.py`, `test_recuperacion_alertas.py` y `test_orquestacion.py`. No equivale a ejecutar toda la suite del repositorio.

Al terminar se restauraron scheduler y DAG Processor con `DATAMARCA_DAGRUN_TIMEOUT_MIN=60`, sin `DATAMARCA_DEMORAR_EN` ni fallo forzado activo. Las pruebas de fallo y vencimiento no ejecutaron persistencia silver/gold. El arranque permanente se documenta en `airflow/README.md`.

## 3. Archivos locales generados

`job_listo.json` y `tmp_tiempos/` se retiran únicamente del seguimiento de Git y se añaden a `.gitignore`. Las copias locales se conservan y los comandos documentados pueden volver a generarlas. La limpieza no elimina el historial ni los datos de resultados versionados del proyecto.

## Estado de entrega

Los tres puntos se integraron en main mediante el PR #34 a petición de Joel. La prueba posterior y las evidencias asignadas a Kinan siguen siendo una actividad separada. El aviso previo de Rosario se basa en la confirmación de Joel; no se envió ningún mensaje a terceros desde esta sesión.


## Revisión visual de las capturas — 9 de noviembre de 2026

Se abrieron las dos capturas de Teams. Se ven el equipo/canal del proyecto, resultados
de las corridas e identificadores de notificación; no se observan correos, nombres
personales legibles, contraseñas ni URLs del webhook. Sí hay avatares pequeños de la
interfaz de Teams. La captura del Workflow muestra la ejecución y parte del nombre de la cuenta
en la cabecera, sin el disparador abierto ni su URL secreta. Esta revisión no elimina las versiones históricas de Git.


## Corrección de compatibilidad con Windows — 9 de noviembre de 2026

El PR #34 introdujo una importación incondicional de `fcntl` que impedía recopilar
las pruebas en Windows. El commit `5f21886` de `unidad4-alertas-b1` selecciona
`msvcrt.locking` en Windows y conserva `fcntl.flock` en POSIX; mantiene el bloqueo
entre procesos, el plazo y la prohibición de enviar sin deduplicación. También evita
abrir/sincronizar directorios con APIs POSIX en Windows y hace explícita la lectura
UTF-8 en las pruebas. No se desactivan ni omiten las pruebas de alertas.

[Verificación real de GitHub Actions](https://github.com/rosariolodi/BIG-DATA/actions/runs/42604479494):
141 pruebas aprobadas en Windows y 141 en Ubuntu, con Python 3.12 y Java 17.
La suite local de macOS también aprobó las 141 pruebas. Esta corrección posterior al
PR #34 permanece en la rama de Joel; no se ha integrado todavía en main.
