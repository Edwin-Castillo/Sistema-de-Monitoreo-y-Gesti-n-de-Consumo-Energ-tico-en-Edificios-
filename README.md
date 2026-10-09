# ENERGÍA — Sistema de monitoreo y gestión del consumo energético

Programa funcional para la entrega **5.1: Programación de la ejecución** de Proyecto de Graduación II. Implementado con los requisitos y el diseño de «Prototipos PG2.docx». La adquisición y los puntos de medición se simulan; la comunicación MQTT, el backend, el almacenamiento y la interfaz se ejecutan realmente en el equipo.

## Ejecutar en Windows

1. Instalar **Node.js 24 LTS** (mínimo 22.13).
2. Extraer completamente este ZIP en una carpeta.
3. Abrir **INICIAR_WINDOWS.bat**. En la primera ejecución instala dependencias mediante Internet.
4. Abrir **http://localhost:3000**.
5. Ingresar con usuario **admin** y la contraseña que muestra la consola durante la primera creación de la base de datos. Guardar esa contraseña; no vuelve a mostrarse al reiniciar.
6. Entrar a **Simulación → Iniciar simulación**. Las lecturas aparecen aproximadamente cada cinco segundos.

Mantener abierta la consola. Para detener el programa, usar Ctrl+C. Para ejecutarlo nuevamente, abrir el mismo archivo BAT. Se conservan las cuentas, las mediciones, los eventos, los informes y la auditoría en `data/energia.sqlite`. No borrar esa carpeta para conservar el historial.

Linux/macOS: ejecutar `bash iniciar.sh`. Alternativa en cualquier sistema: `npm ci` y `npm start`. El frontend ya viene compilado y funciona sin CDN. Para modificarlo: editar `src/App.jsx` y ejecutar `npm run build`.

## Demostración sugerida para el punto 5.1

1. Iniciar sesión y abrir Vista general. Observar inicialmente «No disponible» para energía y potencia sin lecturas.
2. Iniciar el simulador. Verificar la recepción, la potencia y el incremento de energía.
3. En Activos y áreas, editar PM-001 y cambiar su intervalo a **1 segundo** para agilizar la demostración.
4. En Simulación, configurar PM-001 en **Sobrecarga**, pérdida 0 % y latencia 0 ms. Su corriente será 18.4 A con los parámetros iniciales.
5. La regla inicial detecta corriente mayor a 12 A durante dos lecturas consecutivas. Abrir Anomalías y revisar su evidencia.
6. Volver al escenario Normal. Una vez recibida la recuperación, registrar una observación y cerrar el evento. No permite cerrar mientras la condición esté activa.
7. Configurar Desconexión o pérdida 100 %. Revisar mensajes perdidos, caída de la tasa de recepción y «Sin comunicación» después de tres intervalos sin recibir.
8. Recuperar el escenario normal y probar una latencia de 2000 ms. Verificar la cola y las horas de medición y recepción.
9. Consultar los 17 indicadores, aplicar filtros y generar un informe. Descargar PDF y CSV.
10. Crear cuentas Técnico y Consulta en Usuarios. Comprobar: Técnico genera informes; Consulta solamente consulta y descarga los existentes. Solo Administrador modifica activos, reglas, anomalías, simulación, cuentas y configuración.
11. Reiniciar y verificar que el historial permanece. La simulación arranca pausada por diseño.

## Módulos implementados

- Acceso, sesión de una hora, cierre de sesión y validación de permisos también en la API.
- Vista general y monitoreo con gráficos, últimas 300 lecturas del filtro, hora de recepción y vigencia.
- Alta y edición de áreas y puntos; inactivación sin borrar el historial.
- Reglas sobre corriente, potencia y voltaje, persistencia, severidad y estado.
- Anomalías agrupadas como episodios, evidencia, revisión y cierre con observación obligatoria.
- Análisis con filtros compartidos, tres pestañas y los 17 indicadores; fórmulas visibles.
- Informes PDF/CSV guardados y descargables con filtros, fecha, origen y cobertura.
- Usuarios y permisos: contraseñas con scrypt y sal única, usuario único, cuenta activa e invariante de administrador activo.
- Configuración de vigencia y zona horaria; diagnóstico y auditoría.
- Adaptación móvil y estados de carga, ausencia de datos, errores y sesión vencida.
- Simulación de puntos, consumo normal, sobrecarga, consumo cero, desconexión, pérdida y latencia.

## Flujo de ejecución

`Simulador → MQTT local autenticado (QoS 1) → backend Node.js → repositorio persistente → frontend React`

El broker Aedes escucha exclusivamente en 127.0.0.1:18883. Sus credenciales internas son aleatorias en cada arranque. Cada mensaje incluye punto, identificador único, secuencia, fecha, corriente, voltaje, factor de potencia y energía. El backend valida, deduplica y rechaza mensajes fuera de orden. No se emula MQTT dibujando estados en pantalla: se publican y consumen mensajes de un broker real.

## Datos y cálculos

- Todo registro tiene origen `simulated`; interfaz e informes indican SIMULADO.
- Potencia monofásica simulada: `P(kW) = I(A) × V(V) × FP / 1000`. No se presenta como potencia certificada ni como lectura de hardware.
- La energía se integra usando potencia constante del intervalo: `ΔE = P × Δt / 3600000` con Δt en ms. El primer registro de una ejecución aporta 0 kWh porque todavía no existe intervalo anterior.
- La energía recibida suma los incrementos de los mensajes recibidos. Los intervalos perdidos no se rellenan ni se contabilizan como energía recibida. La cobertura se informa por separado.
- El simulador conserva un contador acumulado por punto. Puede diferir de la energía recibida cuando hay pérdida.
- Tasa de recepción: mensajes únicos recibidos / mensajes emitidos esperados del filtro × 100. Durante una latencia puede ser temporalmente menor al 100 %.
- Corriente de todos los puntos no se suma como si fuera un único circuito. La potencia total solo incluye puntos con comunicación vigente.
- Consumo promedio, máximo y mínimo se calculan por día local con registros; los días pueden ser parciales.
- Reducción, variación y tendencia comparan el período seleccionado con el período inmediatamente anterior de igual duración. Sin fechas delimitadas, sin registros anteriores o con base cero se muestra «No calculable». Son comparaciones de energía recibida; la cobertura debe revisarse antes de interpretarlas como ahorro.
- El indicador 09 cuenta lecturas que superan el umbral, conforme a la Tabla de indicadores. El indicador 11 cuenta episodios que cumplen persistencia. Una lectura que viola varias reglas se cuenta una sola vez en la tasa de mediciones fuera de umbral.
- Los intervalos sin datos interrumpen la persistencia de una condición. El evento conserva la severidad y el límite que tenía al detectarse.
- Temperatura y frecuencia no se muestran como lecturas disponibles, conforme al prototipo. Voltaje y FP son parámetros del simulador.
- No hay controles de maniobra eléctrica.

## Oracle y Azure

El modo predeterminado usa **SQLite local**, para ejecutar la entrega sin contratar ni configurar servicios. La arquitectura incluye un repositorio alternativo Oracle mediante `oracledb`, pero **la conexión a Oracle/Azure no fue probada**, porque no se suministraron servidor ni credenciales.

Ejemplo PowerShell para activar Oracle antes de iniciar:

```powershell
$env:DB_MODE = "oracle"
$env:ORACLE_USER = "usuario_del_proyecto"
$env:ORACLE_PASSWORD = "su_contraseña"
$env:ORACLE_CONNECT_STRING = "servidor:1521/servicio"
npm start
```

El usuario debe tener permiso para crear la tabla `PG2_STATE`. El modo Oracle almacena el estado del prototipo en un CLOB y confirma transacciones. No crea una cuenta en Azure ni instala Oracle; requiere una instancia previamente configurada y accesible. El cambio de modo no migra automáticamente los datos de SQLite. Si falla la conexión, el programa falla al iniciar: no simula una conexión correcta ni cambia silenciosamente a SQLite.

La persistencia usa una instantánea transaccional del estado. Es adecuada para esta demostración académica de pequeña escala; para operación prolongada, requiere migración a tablas normalizadas, índices, retención, copias de seguridad y procesamiento de mayor volumen. Los mensajes retrasados se mantienen en memoria; un reinicio pierde la cola pendiente y deja constancia de menor cobertura en los contadores. Las sesiones también se invalidan al reiniciar.

## Validación

`npm test` ejecuta una prueba de integración con base aislada y puertos 3107/18887. Verifica autenticación, permisos de los tres grupos, conservación de un administrador, validación, flujo MQTT, potencia y energía, anomalías, recuperación, pérdida total, latencia, PDF/CSV, filtros inválidos, cierre de sesión y persistencia después de reiniciar.

La interfaz se verificó con Chromium de escritorio y móvil: navegación de las diez vistas, inicio de simulación, formulario de área, exportaciones y ausencia de errores de JavaScript. Las capturas en `evidencias/` corresponden al programa ejecutado, no a los prototipos.

## Archivos

`server.js`: backend, broker y simulador. `repository.js`: persistencia SQLite/Oracle. `src/App.jsx`: interfaz React. `public/`: frontend compilado. `tests/`: integración. `package-lock.json`: versiones de dependencias. `INICIAR_WINDOWS.bat` e `iniciar.sh`: ejecución. `.env.example`: referencia de variables; no se carga automáticamente. `evidencias/`: capturas y resultados.

Entorno local de demostración: HTTP en loopback. Para publicar fuera del equipo, se requiere HTTPS, configuración segura de cookies, despliegue y gestión de secretos. No se ha realizado esa publicación ni una instalación física.
