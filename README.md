# Sistema de Detección de Fraude Bancario

Sistema de detección de fraude en tiempo (cuasi) real basado en el ecosistema **Apache Big Data** y la metodología **KDD** (Knowledge Discovery in Databases). Procesa transacciones desde MongoDB hacia un cluster (1 Master + 2 Nodos) con HDFS, Hive, Spark y Airflow.

---

## Arquitectura Técnica

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   MongoDB   │────▶│ Apache NiFi │────▶│ Apache Kafka│────▶│   Spark     │
│ (Transacc.) │     │ (Ingesta    │     │ (Buffer     │     │ (Streaming  │
│             │     │  incremental)     │  Streaming)  │     │  + MLlib)   │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
        ┌──────────────────────────────────────────────────────────┼──────────────────────────────────────────┐
        │                                                          ▼                                          │
        │  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐     ┌─────────────────────┐  │
        │  │    HDFS     │     │ Apache Hive │     │   Apache Airflow         │     │   FastAPI (API)     │  │
        │  │ (Data Lake) │     │ (DW Hist.)  │     │   (Re-entrenamiento ML)  │     │   Swagger /docs     │  │
        │  └─────────────┘     └─────────────┘     └─────────────────────────┘     └─────────────────────┘  │
        └────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

| Componente        | Rol                                                                 |
|-------------------|---------------------------------------------------------------------|
| **MongoDB**       | Fuente de transacciones (simulación tiempo real).                   |
| **Apache NiFi**   | Ingesta incremental desde MongoDB hacia Kafka/HDFS.                |
| **Apache Kafka**  | Buffer de streaming para procesamiento en tiempo real.              |
| **HDFS**          | Data Lake; almacenamiento crudo y modelos.                          |
| **Apache Hive**   | Data Warehouse histórico y consultas SQL.                           |
| **Apache Spark**  | Procesamiento streaming y ML (Random Forest, detección anomalías).  |
| **Apache Airflow**| Orquestación de DAGs para re-entrenamiento del modelo.              |
| **FastAPI**       | API REST con Swagger para consultar alertas de fraude.              |

---

## Las 5 Fases del Proceso KDD

La metodología **KDD** estructura el descubrimiento de conocimiento en bases de datos en cinco fases. En este proyecto cada fase se materializa en componentes concretos.

### 1. Selección (Selection)

**Objetivo:** Definir y extraer los datos relevantes para el problema (detección de fraude).

- **Fuente:** Colección de transacciones en MongoDB.
- **Atributos seleccionados:** `transaction_id`, `amount`, `location` (lat, lon), `timestamp`, `user_id`, `merchant_id`, `is_fraud` (etiqueta).
- **Criterios:** Ventana temporal (ej. últimas 24h para streaming), filtro por tipo de transacción si aplica.
- **Implementación:** Script de ingesta (NiFi/Kafka) y consultas de selección en Spark (`spark_detection.py`).

### 2. Preprocesamiento (Preprocessing)

**Objetivo:** Limpiar y preparar los datos para minimizar ruido e inconsistencias.

- **Manejo de nulos:** Imputación o descarte según política.
- **Detección de duplicados:** Por `transaction_id`.
- **Validación de rangos:** Montos > 0, coordenadas válidas, timestamps coherentes.
- **Balanceo:** Técnicas (SMOTE, submuestreo) si hay desbalance fraude/no fraude.
- **Implementación:** Pipelines en Spark (limpieza en `spark_detection.py` y/o notebooks).

### 3. Transformación (Transformation)

**Objetivo:** Adaptar los datos al formato y escala requeridos por el modelo.

- **Normalización/estandarización:** Montos y features numéricos (StandardScaler o MinMax).
- **Ingeniería de características:** 
  - Distancia entre ubicación del usuario y del comercio.
  - Hora del día, día de la semana.
  - Ratio monto/histórico del usuario.
- **VectorAssembler:** Creación del vector de características para MLlib.
- **Implementación:** Bloque de transformación en `spark_detection.py` (VectorAssembler + escalado).

### 4. Minería de Datos (Data Mining)

**Objetivo:** Aplicar algoritmos para descubrir patrones (modelo de clasificación de fraude).

- **Algoritmo:** Random Forest (MLlib) para clasificación binaria (fraude / no fraude).
- **Entrenamiento:** Sobre datos históricos transformados; modelo persistido en HDFS.
- **Inferencia:** En streaming o batch sobre nuevas transacciones.
- **Implementación:** `pyspark.ml.classification.RandomForestClassifier` en `spark_detection.py`.

### 5. Interpretación y Evaluación (Interpretation / Evaluation)

**Objetivo:** Validar el modelo e interpretar resultados para toma de decisiones.

- **Métricas:** Precisión, recall, F1, **AUC-ROC** (área bajo la curva ROC).
- **Curva ROC:** Generada con `BinaryClassificationEvaluator` (métrica `areaUnderROC`).
- **Umbral de decisión:** Ajuste según coste de falsos positivos/negativos.
- **Salida:** Alertas de fraude expuestas vía API (FastAPI) para operadores.
- **Implementación:** Evaluación en `spark_detection.py` y consulta de alertas en `api/main.py`.

---

## Estructura del Proyecto

```
proyectoFinBigData/
├── README.md                 # Este archivo (KDD + arquitectura)
├── requirements.txt          # Dependencias Python
├── docker-compose.yml        # MongoDB, Kafka, Zookeeper (opcional)
├── .gitignore
├── api/
│   └── main.py               # FastAPI + Swagger, endpoints de alertas
├── scripts/
│   ├── spark_detection.py    # Spark: transformación, RF, AUC-ROC, guarda pipeline para Kafka
│   ├── spark_streaming_kafka.py  # Spark Streaming: Kafka → pipeline → alertas
│   ├── populate_mongodb.py   # Poblado de MongoDB con transacciones sintéticas
│   ├── export_mongodb_to_json.py  # Export MongoDB → JSON para Spark
│   ├── kafka_producer_mongodb.py  # Productor: MongoDB → Kafka (topic transactions)
│   ├── run_spark_detection.sh     # Entrenar modelo (spark-submit, detecta SPARK_HOME)
│   ├── run_spark_streaming_kafka.sh  # Arrancar consumidor Spark desde Kafka
│   ├── install_airflow.sh    # Instalar Airflow en airflow_venv
│   └── run_airflow.sh        # Arrancar Airflow (standalone, DAGs del proyecto)
├── ingesta/                  # Ver scripts/kafka_producer_mongodb.py para MongoDB→Kafka
├── notebooks/                # Jupyter para exploración y experimentos
├── airflow_dags/             # DAGs de Airflow (re-entrenamiento)
└── models/                   # Modelos persistidos (HDFS/local)
```

---

## Requisitos y Ejecución

### Requisitos

- Python 3.9+
- Spark 3.x (PySpark)
- MongoDB
- (Opcional) Kafka, NiFi, HDFS, Airflow para arquitectura completa

### Instalación

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Poblar MongoDB (datos sintéticos)

```bash
python scripts/populate_mongodb.py
```

### Entrenar modelo y evaluar (Spark)

**Opción A – Con Spark instalado (recomendado)**  
Si tienes Spark en el cluster o en el nodo (`SPARK_HOME` o `spark-submit` en el PATH):

```bash
# Exportar datos de MongoDB a JSON (opcional; si no, usa datos demo en memoria)
MONGO_URI=mongodb://127.0.0.1:27018 python scripts/export_mongodb_to_json.py

# Entrenar con spark-submit
SPARK_HOME=/ruta/a/spark ./scripts/run_spark_detection.sh
# o directamente:
spark-submit scripts/spark_detection.py
```

**Opción B – Solo PySpark en venv (modo local)**  
En algunos entornos puede aparecer `TypeError: 'JavaPackage' object is not callable`. Si ocurre, usa la Opción A (spark-submit).

```bash
pip install -r requirements.txt
MONGO_URI=mongodb://127.0.0.1:27018 python scripts/export_mongodb_to_json.py
python scripts/spark_detection.py
```

Salida: modelo en `models/fraud_rf_model`, alertas en `data/alerts`, métricas AUC-ROC en consola.

### Levantar API (FastAPI + Swagger)

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Documentación interactiva: **http://localhost:8000/docs**

### Docker (MongoDB + Kafka + Mongo Express)

```bash
docker-compose up -d
```

Para **ver los datos de MongoDB en una página web** tienes dos opciones:

1. **Mongo Express** (interfaz de administración): con los contenedores arriba, levanta también `mongo-express` y abre en el navegador **http://nodo1:8081**. Usuario: `admin`, contraseña: `admin`. Ahí puedes explorar la base `fraud_detection` y la colección `transactions`.
   ```bash
   docker-compose up -d mongodb mongo-express
   ```

2. **Página en la API FastAPI**: con la API en marcha (`uvicorn api.main:app --host 0.0.0.0 --port 8000`), abre **http://nodo1:8000/data**. Esa página muestra una tabla con las transacciones de MongoDB (usa `MONGO_URI`, por defecto `mongodb://127.0.0.1:27018`).

### Flujo con Kafka (MongoDB → Kafka → Spark Streaming)

Kafka actúa como buffer de streaming: el productor lee de MongoDB y publica en un topic; Spark Structured Streaming consume, aplica el modelo y genera alertas.

**Requisitos:** Docker con Kafka + Zookeeper, MongoDB con datos, modelo y **pipeline** ya entrenados (el entrenamiento guarda además `models/fraud_pipeline`).

1. **Levantar Kafka (y Zookeeper):**
   ```bash
   cd ~/proyectoFinBigData
   docker-compose up -d zookeeper kafka mongodb
   ```

2. **Entrenar modelo** (genera también el pipeline para streaming):
   ```bash
   ./scripts/run_spark_detection.sh
   ```
   Si usas HDFS, define `SPARK_DATA_PATH`, `SPARK_MODEL_PATH`, `SPARK_ALERTS_PATH` como antes.

3. **Productor: enviar transacciones de MongoDB a Kafka**
   ```bash
   source venv/bin/activate
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MONGO_URI=mongodb://127.0.0.1:27018 python scripts/kafka_producer_mongodb.py
   ```
   Crea/publica en el topic `transactions` (por defecto). Opcional: `PRODUCER_LIMIT=1000` para limitar mensajes.

4. **Consumidor Spark Streaming** (en otra terminal):
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ./scripts/run_spark_streaming_kafka.sh
   ```
   Lee del topic `transactions`, aplica el pipeline de fraude y escribe alertas en consola. Para escribir alertas en Parquet (y que la API las sirva): `STREAMING_SINK=parquet ./scripts/run_spark_streaming_kafka.sh`.

Variables de entorno útiles: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_TRANSACTIONS` (por defecto `transactions`), `SPARK_PIPELINE_PATH`, `STREAMING_SINK` (consola o parquet).

**Comprobar que Kafka funciona (prueba rápida)**  
Con Zookeeper y Kafka en marcha (`docker-compose up -d zookeeper kafka`):

```bash
# Crear un topic de prueba
docker exec -it fraud_kafka kafka-topics --create --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1 --topic test-topic

# Enviar un mensaje (escribir algo, Enter, luego Ctrl+C)
docker exec -it fraud_kafka kafka-console-producer --bootstrap-server localhost:29092 --topic test-topic

# En otra terminal: consumir (debe aparecer el mensaje)
docker exec -it fraud_kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic test-topic --from-beginning
```

Si ves el mensaje en el consumer, Kafka está operativo. Para el proyecto se usa el topic `transactions` (lo crea el productor o Kafka con auto-create).

**Si el productor no conecta:** desde el host usa `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Si falla, prueba `localhost:29092`.

### Airflow (re-entrenamiento periódico del modelo)

Instalación en un venv dedicado (evita conflictos con el proyecto):

```bash
cd ~/proyectoFinBigData
./scripts/install_airflow.sh
```

Primera vez: inicializar BD y crear usuario admin, luego arrancar:

```bash
source airflow_venv/bin/activate
export AIRFLOW_HOME=$PWD/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow_dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db init
airflow users create --role Admin --username admin --email admin@local --firstname Admin --lastname User --password admin
airflow standalone
```

O usar el script que hace init si no existe la BD:

```bash
./scripts/run_airflow.sh
```

Interfaz web: **http://nodo1:8080** (usuario `admin`, contraseña la que hayas puesto). El DAG `fraud_model_retrain` ejecuta el re-entrenamiento Spark con datos en HDFS cada 7 días.

---

## Componentes instalados

Resumen de lo que se usa en este proyecto y dónde está (nodo1, salvo indicación).

| Componente | Dónde / versión | Puerto(s) | Notas |
|------------|------------------|-----------|--------|
| **Python** | Sistema + venv en `venv/` | - | 3.12; venv para API y scripts |
| **MongoDB** | Docker, imagen `mongo:4.4` | 27018 (host) | Sin AVX; colección `fraud_detection.transactions` |
| **Kafka** | Docker, Confluent 7.5 | 9092, 29092 | Opcional para streaming |
| **Zookeeper** | Docker, Confluent 7.5 | 2181 | Requerido por Kafka |
| **Hadoop HDFS** | Instalación en nodo1 | 9000 (NameNode) | NameNode en nodo1; DataNode(s) según cluster |
| **Spark** | `/home/hadoop/spark` (2.10 / 3.5.x) | 7077 (master), 4040 (UI) | Master en nodo1; workers según cluster |
| **Apache Airflow** | Venv en `airflow_venv/`, datos en `airflow_home/` | 8080 (webserver) | 2.10.x; DAGs en `airflow_dags/` |
| **FastAPI** | Venv `venv/`, `api/main.py` | 8000 | API de alertas y Swagger |

Rutas HDFS usadas por defecto:

- Datos: `hdfs://nodo1:9000/user/hadoop/fraud/transactions`
- Modelo: `hdfs://nodo1:9000/user/hadoop/fraud/models/fraud_rf_model`
- Alertas: `hdfs://nodo1:9000/user/hadoop/fraud/alerts`

---

## Visualización de datos de MongoDB en una página

Los datos de prueba (transacciones) están en MongoDB, base `fraud_detection`, colección `transactions`. Se pueden ver en el navegador de dos formas:

### Opción 1: Página integrada en la API (FastAPI)

La API expone la ruta **`/data`**, que devuelve una página HTML con una tabla de transacciones leídas desde MongoDB.

**Requisitos:** API en marcha y MongoDB accesible (p. ej. contenedor en 27018).

**Pasos:**

1. Arrancar MongoDB (si no está ya):  
   `cd ~/proyectoFinBigData && docker-compose up -d mongodb`
2. Arrancar la API:  
   `source venv/bin/activate && uvicorn api.main:app --host 0.0.0.0 --port 8000`
3. Abrir en el navegador:  
   **http://nodo1:8000/data**  
   (o http://127.0.0.1:8000/data si estás en el mismo equipo).

**Parámetros:** se puede limitar el número de filas con la query `limit` (por defecto 200, máximo 2000):  
**http://nodo1:8000/data?limit=500**

**Configuración:** si MongoDB no está en el puerto por defecto, indicar la URI al arrancar la API:  
`MONGO_URI=mongodb://127.0.0.1:27018 uvicorn api.main:app --host 0.0.0.0 --port 8000`

---

### Opción 2: Mongo Express (interfaz web de administración)

Mongo Express es una interfaz web que permite explorar bases de datos, colecciones y documentos en MongoDB.

**Requisitos:** Docker; servicios `mongodb` y `mongo-express` del `docker-compose` del proyecto.

**Pasos:**

1. Levantar MongoDB y Mongo Express:  
   `cd ~/proyectoFinBigData && docker-compose up -d mongodb mongo-express`
2. Abrir en el navegador:  
   **http://nodo1:8081**
3. Iniciar sesión:  
   - Usuario: **admin**  
   - Contraseña: **admin**
4. En la interfaz, elegir la base **fraud_detection** y la colección **transactions** para ver y filtrar los documentos.

| Opción | URL | Credenciales / notas |
|--------|-----|----------------------|
| Página API | http://nodo1:8000/data | Ninguna; tabla de solo lectura. |
| Mongo Express | http://nodo1:8081 | Usuario: `admin`, contraseña: `admin`. |

---

## Arranque tras reinicio del equipo

Orden recomendado en **nodo1** (o en el nodo donde corresponda cada servicio). Ejecutar cada bloque en su propia terminal o en segundo plano según se indique.

### 1. HDFS (si usas datos en HDFS)

```bash
# En nodo1 (donde está el NameNode)
start-dfs.sh
# Opcional: salir de safe mode si aparece
hdfs dfsadmin -safemode leave
```

### 2. Spark (cluster)

Si tienes un cluster Spark en varios nodos:

```bash
# En el nodo master (p. ej. nodo1)
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh
```

Si solo usas Spark en nodo1 para entrenar con `run_spark_detection.sh`, no hace falta arrancar servicios Spark por separado (el script usa `spark-submit` y levanta el contexto que haga falta).

### 3. Docker: MongoDB (y opcionalmente Kafka + Zookeeper)

```bash
cd ~/proyectoFinBigData
docker-compose up -d mongodb
# Opcional: también Kafka y Zookeeper
# docker-compose up -d
```

Comprobar: `docker ps | grep -E 'mongo|kafka|zookeeper'`.

### 4. Airflow (webserver + scheduler)

```bash
cd ~/proyectoFinBigData
./scripts/run_airflow.sh
```

Dejar esta terminal abierta. Interfaz: **http://nodo1:8080** (o `http://<IP_nodo1>:8080`).

### 5. API FastAPI (alertas)

```bash
cd ~/proyectoFinBigData
source venv/bin/activate
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Documentación: **http://nodo1:8000/docs**.

---

### Resumen rápido (orden mínimo)

| Paso | Comando (en nodo1) |
|------|---------------------|
| 1. HDFS | `start-dfs.sh` |
| 2. Docker MongoDB | `cd ~/proyectoFinBigData && docker-compose up -d mongodb` |
| 3. Airflow | `cd ~/proyectoFinBigData && ./scripts/run_airflow.sh` |
| 4. API | `cd ~/proyectoFinBigData && source venv/bin/activate && uvicorn api.main:app --host 0.0.0.0 --port 8000` |

URLs tras el arranque:

- **Airflow:** http://nodo1:8080  
- **API (Swagger):** http://nodo1:8000/docs  
- **MongoDB:** `mongodb://127.0.0.1:27018` (desde nodo1)

---

## Licencia y Contacto

Proyecto académico / fin de grado. Ajustar licencia según necesidad.
