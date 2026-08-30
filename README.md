# Module 4: Cloud Infrastructure & Modern Data Platforms

This module explores cloud service models, cloud-native storage/compute, serverless data pipelines, and architecture patterns for modern cloud-based data platforms (e.g., Snowflake, Databricks, BigQuery).

## Exercises

*   [**Exercise 1 (OPTIONAL): Cloud Data Applications with Databricks**](./exercise-1-databricks-optional.md)
    *   Implement one of three cloud-based projects (Streaming IoT, MLlib recommendations, or DLT data quality) inside a managed Databricks workspace.

---

## Solución entregada. Opción A: telemetría IoT en tiempo real e ingesta en Delta Lake

Pipeline de Spark Structured Streaming que lee telemetría de vehículos desde un
directorio de ficheros JSON, la limpia y la escribe en una tabla Delta Lake
particionada por ciudad.

Hay dos versiones del pipeline. La lógica (esquema, seis reglas de limpieza,
cuarentena, particionado, capa oro y comprobaciones) es idéntica; solo cambia el
entorno donde corre.

| Versión | Fichero | Dónde corre |
|---|---|---|
| **Databricks** | **[Entregable-Databricks.ipynb](./Entregable-Databricks.ipynb)** | Cluster de Databricks, sobre Unity Catalog Volume o DBFS. Es la que pide el enunciado |
| Local | [Entregable.ipynb](./Entregable.ipynb) | PySpark en el portátil, sin necesidad de Databricks |

### Ficheros

| Fichero | Qué es |
|---|---|
| **[Entregable-Databricks.ipynb](./Entregable-Databricks.ipynb)** | El pipeline para Databricks, ejecutado y con los resultados incrustados. Autocontenido: el generador va embebido |
| **[Entregable.ipynb](./Entregable.ipynb)** | El mismo pipeline en local, también ejecutado |
| **[informe.md](./informe.md)** | Informe de validación de la ejecución en Databricks |
| [generador.py](./generador.py) | Simula la flota y escribe los ficheros JSON. Solo lo usa la versión local |
| `datos/semilla_ved.parquet` | Los datos reales de partida (Vehicle Energy Dataset). Lo usan las dos |
| [requirements.txt](./requirements.txt) | Dependencias de la versión local. En Databricks no hace falta instalar nada |

Todo lo que genera la ejecución (`lake/`, `rendimiento.png`) se recrea solo al
volver a ejecutar el notebook.

### Cómo ejecutarlo en Databricks

1. **Subir la semilla.** En *Catalog*, crea un volumen `telemetria` dentro de tu
   esquema y sube ahí `datos/semilla_ved.parquet` (12 MB).
2. **Importar el notebook.** *Workspace* → *Import* → `Entregable-Databricks.ipynb`.
3. **Ajustar la configuración.** Primera celda de código: `ALMACEN` a `"volumes"`
   (o `"dbfs"` si el workspace no tiene Unity Catalog) y `CATALOGO` / `ESQUEMA` /
   `VOLUMEN` a los tuyos.
4. **Conectar un cluster** con Databricks Runtime 13.3 LTS o superior y ejecutar de
   arriba abajo. No hay que instalar librerías: Delta Lake, pandas y matplotlib ya
   vienen en el runtime.

Tarda unos 4 minutos.

Las capturas de la ejecución están en [Capturas/](./Capturas/) y se comentan en la
sección 5.9 del informe.

### Cómo ejecutarlo en local

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/jupyter lab Entregable.ipynb
```

Y ejecutar las celdas de arriba abajo. El notebook lanza el generador solo, así
que no hay que arrancar nada aparte. Tarda unos 4 minutos.

### Los datos

El punto de partida es el **[Vehicle Energy Dataset](https://github.com/gsoh/VED)**
(Universidad de Michigan, Apache-2.0): 27 coches eléctricos reales medidos por
OBD-II a 1 registro por segundo. Aporta las tres señales que pide el enunciado
(GPS, velocidad y nivel de batería), medidas de verdad.

Sobre esos datos reales, el generador simula una flota repartida por 5 ciudades
españolas y estropea a propósito una parte de los registros, para que la limpieza
tenga algo que limpiar y se pueda comprobar que funciona. Vive en
[generador.py](./generador.py) en la versión local y embebido en la sección 2 del
notebook en la de Databricks, con la misma lógica en ambos.

La semilla ya viene preparada en `datos/`, así que no hay que descargar nada.

### Si algo falla en Databricks

- **`No encuentro la semilla en /Volumes/...`**: falta subir
  `datos/semilla_ved.parquet` al volumen, o `CATALOGO`/`ESQUEMA`/`VOLUMEN` no
  coinciden con los tuyos.
- **`Public DBFS root is disabled`**: el workspace no permite escribir en DBFS.
  Pon `ALMACEN = "volumes"`.
- **`java.io.FileNotFoundException: /dbfs/...`**: se están mezclando las dos
  formas de nombrar DBFS. Python usa `/dbfs/...` y Spark `dbfs:/...`; de eso se
  encarga la función `ruta()`, así que pásale siempre las constantes de la celda de
  configuración y no rutas escritas a mano.
- **`DELTA_SCHEMA_NOT_SET`**: se ha ejecutado la celda de plata antes de que
  bronce escribiera su primer lote. Vuelve a lanzar la celda de bronce y espera a
  que imprima `bronce en marcha`.

### Si algo falla en Windows (versión local)

- **`Could not locate executable winutils.exe`**: faltan los binarios de Hadoop.
  Están en `.hadoop/bin`; el notebook apunta ahí solo en su primera celda.
- **El proceso se queda colgado al arrancar Spark**: Spark está usando el `python`
  del PATH, que en Windows suele ser el atajo de la Microsoft Store. La primera
  celda del notebook lo corrige fijando `PYSPARK_PYTHON`.
- **`Failed to delete ... temp dir` al terminar**: ruido normal de Spark en
  Windows al cerrarse. No afecta a nada.

---
[⬅ Back to Home](../README.md)
