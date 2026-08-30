# Informe de validación de la ejecución

**Módulo 4 · Ejercicio 1 · Opción A. Telemetría IoT en tiempo real e ingesta en Delta Lake**

Los datos de este informe salen de la ejecución completa de
[Entregable-Databricks.ipynb](./Entregable-Databricks.ipynb) sobre un workspace de
Databricks, con los resultados incrustados en el propio notebook.

---

## 1. Qué se ha construido

| Lo que pedía el enunciado | Dónde está |
|---|---|
| Databricks Notebook | ejecutado sobre cómputo serverless, Spark 4.2.0 |
| Leer telemetría simulada en streaming | `readStream` sobre `landing/` (sección 3) |
| Desde almacenamiento cloud | Unity Catalog Volume (sección 1) |
| Parsear el JSON | Esquema declarado a mano, sin inferencia (sección 3) |
| Limpiar anomalías | Seis reglas + tabla de cuarentena (sección 4) |
| Tabla Delta particionada por ciudad | `partitionBy("city")`, comprobado en 7.2 |

Recorrido de los datos:

```
generador (hilo) → landing/ (JSON) → BRONCE → [quitar duplicados] → PLATA + CUARENTENA → ORO
```

## 2. Entorno

| Componente | Versión |
|---|---|
| Spark | 4.2.0 (Databricks, cómputo serverless) |
| Almacenamiento | Unity Catalog Volume, `/Volumes/workspace/default/telemetria` |
| Catálogo | Unity Catalog, esquema `workspace.default` |
| `spark.sql.shuffle.partitions` | 8 |
| Zona horaria de sesión | UTC |

## 3. Origen de los datos

Base: **[Vehicle Energy Dataset](https://github.com/gsoh/VED)** (Univ. de Michigan,
Apache-2.0). 27 coches eléctricos reales, OBD-II a 1 Hz, Ann Arbor (Michigan),
2017-2018. Tras filtrar a los vehículos que miden batería y limpiar: 1.494.385
registros de 1.590 viajes.

Sobre esos datos reales se simulan tres cosas:

1. **La geografía.** Cada vehículo se asigna a una de 5 ciudades españolas y su
   recorrido se traslada allí, reescalando la longitud por `cos(latitud)` para no
   deformar las distancias.
2. **El tamaño de la flota.** Cada viaje real pasa a ser un vehículo distinto
   (1.590 en total), porque los 27 coches originales darían solo 5 por ciudad.
3. **La velocidad de reproducción.** 60× más rápido que el tiempo real.

La velocidad y el nivel de batería no se modifican en ningún momento.

## 4. Adaptaciones que exigió el cómputo serverless

Tres restricciones de la plataforma obligaron a cambiar el diseño respecto a una
ejecución local. Ninguna afecta a la lógica del pipeline, pero conviene declararlas.

**Trigger.** Serverless rechaza el streaming continuo:

```
[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not
supported for this cluster type. Use a different trigger type e.g. AvailableNow, Once.
```

Las dos consultas usan `Trigger.AvailableNow`: procesan lo pendiente en micro-lotes y
terminan solas. Eso obliga a generar los datos antes de arrancar el flujo, en vez de a
la vez. Se sigue ejerciendo lo mismo (checkpoints, offsets, watermark, estado de
`dropDuplicates`, `foreachBatch`) y `maxFilesPerTrigger` se respeta igual, así que el
trabajo se sigue partiendo en varios micro-lotes.

**Métricas por lote.** `recentProgress` devuelve los objetos de progreso con
`numInputRows` a `None`. Las cifras por micro-lote se leen del log de transacciones de
Delta: cada micro-lote deja una versión con las filas que escribió y su marca de
tiempo. El sink de streaming la registra como `STREAMING UPDATE` y las escrituras de
`foreachBatch` como `WRITE`.

**Caché.** `DataFrame.persist()` está prohibido:

```
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.
```

`foreachBatch` recorre el lote dos veces, una por tabla, sin cachearlo. Con lotes de
unos miles de filas el recálculo no se nota.

Además, Unity Catalog prohíbe `input_file_name()` (se usa `_metadata.file_path`) y no
admite tablas externas con `LOCATION` dentro de un Volume, así que la sección 10 las
crea gestionadas con `CREATE TABLE ... AS SELECT`.

## 5. Resultado de la ejecución

Generación de 60 segundos, lotes de 500 eventos cada 2 segundos: 26 ficheros y 13.204
eventos.

### 5.1 Reconciliación de filas

```
  generadas por el generador    13,204
  leidas en bronce              13,204   (todas)

  plata                         11,931
  cuarentena                     1,069
  duplicados quitados              204
  --------------------------------------
  diferencia                         0   <- descartados por el watermark
  eventos tardios inyectados       192   <- deberia parecerse
```

Bronce recoge las 13.204 filas generadas: no se perdió ni un fichero.

La diferencia es **cero**: el watermark no descartó ningún evento pese a haberse
inyectado 192 atrasados. Con `AvailableNow` el reloj interno de Spark avanza al ritmo
de los datos que va viendo, y como todo el lote entra de golpe, los atrasados llegan
todavía dentro de la ventana de 10 minutos y se aceptan como válidos.

### 5.2 Trabajo por micro-lote

```
landing_a_bronce                    bronce_a_plata
   lote     filas        ms            lote     filas        ms
      1     1,522     1,000               0     4,132         0
      2     1,523     1,000               1     4,118     3,000
      3     1,516     1,000               2     3,681     3,000
      4     1,531     1,000
      5     1,522     2,000
      6     1,527     1,000
      7     1,523     1,000
      8     1,527     1,000
      9     1,013     2,000
```

Bronce procesa en 9 lotes de unas 1.520 filas, que es lo que impone
`maxFilesPerTrigger=3` sobre ficheros de 500 eventos. Las sumas cuadran: 13.204 en
bronce y 11.931 en plata.

### 5.3 Particionado por ciudad

Cinco carpetas en el volumen, una por ciudad, tal y como pide el enunciado:

```
['city=Barcelona', 'city=Bilbao', 'city=Madrid', 'city=Sevilla', 'city=Valencia']

Barcelona 2520 | Bilbao 2319 | Madrid 2411 | Sevilla 2347 | Valencia 2334
```

Suman 11.931, las filas de plata.

### 5.4 La limpieza detecta lo que se rompió

Cada registro se estropea con un solo fallo, así que las cifras tienen que coincidir
exactamente:

```
motivo                      roto   detectado
--------------------------------------------
BATERIA_FUERA_DE_RANGO       191         191  OK
CAMPO_VACIO                  196         196  OK
CIUDAD_DESCONOCIDA           126         126  OK
COORDENADA_IMPOSIBLE         182         182  OK
FUERA_DE_CIUDAD              131         131  OK
VELOCIDAD_ABSURDA            243         243  OK
DUPLICADO                    204           0  lo quita dropDuplicates
EVENTO_TARDIO                192           0  lo descarta el watermark
```

Las seis reglas cuadran al registro. Las dos últimas etiquetas no llegan a cuarentena
por diseño: los duplicados los elimina `dropDuplicates` y los atrasados los descarta el
watermark, y ambos se contabilizan en la reconciliación de 5.1.

Además, al volver a aplicar las reglas sobre la tabla plata ya escrita:

```
filas de plata que incumplen alguna regla: 0
CORRECTO
```

### 5.5 Capa Oro

```
city      | eventos | vehiculos | km_recorridos | vel_media | vel_en_marcha | vel_max | bat_media | pct_bat_critica
Barcelona |    2520 |       304 |         584.6 |      40.9 |          45.2 |   124.6 |      31.3 |            48.6
Bilbao    |    2319 |       296 |         503.9 |      38.6 |          43.5 |   132.8 |      30.7 |            52.2
Madrid    |    2411 |       292 |         544.7 |      38.9 |          43.2 |   131.9 |      32.5 |            47.1
Sevilla   |    2347 |       303 |         534.8 |      39.0 |          43.7 |   128.1 |      37.0 |            43.3
Valencia  |    2334 |       296 |         536.5 |      39.2 |          44.0 |   120.5 |      33.5 |            46.9
```

Las cinco ciudades salen parecidas porque los viajes se reparten entre ellas de forma
rotatoria tras barajarlos. La tabla demuestra que las agregaciones por partición
funcionan, no que en Barcelona se conduzca distinto que en Bilbao.

Los kilómetros se calculan con la fórmula de haversine entre puntos consecutivos del
mismo viaje.

La segunda tabla Oro agrupa la cuarentena por motivo y ciudad. Encabezan
`CAMPO_VACIO / (sin ciudad)` con 65 filas y `VELOCIDAD_ABSURDA / Sevilla` con 61.

### 5.6 Rendimiento

```
lotes procesados   3
filas en total     11,931
filas por lote     3977 de media, 4132 maximo
tiempo por lote    3000 ms de media
```

Gráfica: sección 9 del notebook.

### 5.7 Delta Lake

Historial de la tabla plata: 3 escrituras, una por micro-lote, más la compactación.

```
|version|timestamp                |operation|operationMetrics
|      3|2026-08-29T12:28:38.000Z | OPTIMIZE|numRemovedFiles -> 15, numRemovedBytes -> 949398
|      2|2026-08-29T12:28:31.000Z |    WRITE|numFiles -> 5, numOutputRows -> 3681
|      1|2026-08-29T12:28:28.000Z |    WRITE|numFiles -> 5, numOutputRows -> 4118
|      0|2026-08-29T12:28:25.000Z |    WRITE|numFiles -> 5, numOutputRows -> 4132
```

Se puede leer la tabla en cualquier momento anterior, porque cada escritura es una
transacción numerada:

```
version  0 ->    4,132 filas
version  1 ->    8,250 filas
version  3 ->   11,931 filas
```

Efecto de la compactación:

```
ficheros que usa plata: 15 antes -> 5 despues del OPTIMIZE
ficheros que hay en disco: 20 (los viejos esperan a un VACUUM)
```

De 15 ficheros a 5, uno por partición. Es el problema de los ficheros pequeños del
streaming: escribir por micro-lote y en 5 particiones genera muchos ficheros diminutos,
y leerlos cuesta más en abrirlos que en leer los datos. En esta ejecución el efecto es
moderado porque `AvailableNow` produjo solo 3 micro-lotes; en un flujo continuo con
lotes de 5 segundos la proporción es mucho mayor.

### 5.8 Registro en el catálogo

Las cinco tablas quedan registradas en Unity Catalog y son consultables por nombre:

```
workspace.default.iot_bronce
workspace.default.iot_plata
workspace.default.iot_cuarentena
workspace.default.iot_oro_ciudades
workspace.default.iot_oro_calidad
```

Consulta SQL de comprobación sobre `iot_plata`:

```
city      | eventos | velocidad_media | bateria_media
Barcelona |    2520 |            40.9 |          31.3
Bilbao    |    2319 |            38.6 |          30.7
Madrid    |    2411 |            38.9 |          32.5
Sevilla   |    2347 |            39.0 |          37.0
Valencia  |    2334 |            39.2 |          33.5
```

### 5.9 Evidencia de la ejecución

| Captura | Qué muestra |
|---|---|
| [Captura databricks.png](./Capturas/Captura%20databricks.png) | El notebook en el workspace, con cómputo *Serverless* y la salida de la última celda: flujos parados y las siete carpetas escritas en el volumen |
| [Captura datos.png](./Capturas/Captura%20datos.png) | Catalog Explorer con el catálogo `workspace`, el esquema `default` y el volumen `telemetria` |
| [Captura Computo.png](./Capturas/Captura%20Computo.png) | El cómputo serverless del workspace |

Las estadísticas por micro-lote que pide el enunciado están en 5.2 y 5.6, medidas
sobre el log de transacciones por el motivo explicado en la sección 4.

## 6. Limitaciones

`foreachBatch` garantiza *al menos una vez*, no *exactamente una vez*. Si el proceso se
cayera después de escribir plata pero antes de confirmar el avance, ese lote se
reescribiría al reiniciar. Como cada evento tiene identificador único y los duplicados
se quitan antes, en la práctica no ocurre.

El watermark descarta eventos sin avisar. Es el precio de que la memoria de duplicados
no crezca sin límite, y por eso la comprobación 5.1 mide esa diferencia en vez de dar la
suma por buena. En esta ejecución no descartó ninguno, pero eso es consecuencia de
`AvailableNow` y no debe leerse como que el watermark sea inocuo.

Particionar por ciudad funciona porque son cinco valores. Por vehículo (1.590) o por
segundo darían miles de carpetas con un fichero minúsculo dentro.

Los datos se reproducen 60× acelerados. Sin esa compresión, un solo evento del final de
un viaje de 96 minutos empujaría el watermark casi una hora hacia delante y el resto de
eventos se descartarían por atrasados.

`AvailableNow` impide que productor y consumidor trabajen en paralelo. En un cluster
clásico basta con cambiar el trigger de las dos consultas por
`.trigger(processingTime="5 seconds")` y lanzar el generador a la vez, que es lo que
hace la versión local del notebook.

---

[⬅ Volver al README](./README.md)
