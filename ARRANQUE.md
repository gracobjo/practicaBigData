# Arranque del sistema tras reinicio

Referencia rápida. Detalle completo en [README.md](README.md#arranque-tras-reinicio-del-equipo).

## Orden recomendado (nodo1)

```bash
# 1. HDFS
start-dfs.sh
hdfs dfsadmin -safemode leave   # si hace falta

# 2. MongoDB (Docker)
cd ~/proyectoFinBigData && docker-compose up -d mongodb

# 3. Airflow (dejar en una terminal)
cd ~/proyectoFinBigData && ./scripts/run_airflow.sh

# 4. API (otra terminal)
cd ~/proyectoFinBigData && source venv/bin/activate && uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## URLs

| Servicio | URL |
|----------|-----|
| Airflow | http://nodo1:8080 |
| API (Swagger) | http://nodo1:8000/docs |
| **Datos MongoDB (tabla)** | http://nodo1:8000/data |
| Mongo Express (admin MongoDB) | http://nodo1:8081 (user: admin, pass: admin) |
| MongoDB (conexión) | mongodb://127.0.0.1:27018 |

## Comprobar que todo está en marcha

```bash
# HDFS
hdfs dfsadmin -report

# Docker (MongoDB, Kafka)
docker ps | grep -E 'mongo|kafka'

# Kafka (topic de prueba: crear y producer/consumer como en sección Kafka)
docker exec fraud_kafka kafka-topics --list --bootstrap-server localhost:29092

# Airflow (proceso y puerto)
ps aux | grep airflow
ss -tlnp | grep 8080

# API
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/health
```

## Entrenar el modelo (manual)

```bash
cd ~/proyectoFinBigData
export SPARK_DATA_PATH=hdfs://nodo1:9000/user/hadoop/fraud/transactions
export SPARK_MODEL_PATH=hdfs://nodo1:9000/user/hadoop/fraud/models/fraud_rf_model
export SPARK_ALERTS_PATH=hdfs://nodo1:9000/user/hadoop/fraud/alerts
./scripts/run_spark_detection.sh
```

El DAG de Airflow `fraud_model_retrain` hace esto mismo cada 7 días si Airflow está corriendo.

## Kafka

### Comprobar que Kafka funciona (prueba rápida)

```bash
cd ~/proyectoFinBigData
docker-compose up -d zookeeper kafka

# Crear topic de prueba
docker exec -it fraud_kafka kafka-topics --create --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1 --topic test-topic

# Terminal 1: enviar mensaje (escribe "hola kafka", Enter, Ctrl+C)
docker exec -it fraud_kafka kafka-console-producer --bootstrap-server localhost:29092 --topic test-topic

# Terminal 2: consumir (debe verse el mensaje)
docker exec -it fraud_kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic test-topic --from-beginning
```

### Flujo completo (MongoDB → Kafka → Spark Streaming)

```bash
# 1. Servicios
docker-compose up -d zookeeper kafka mongodb

# 2. Datos en MongoDB (si no hay)
source venv/bin/activate
MONGO_URI=mongodb://127.0.0.1:27018 python scripts/populate_mongodb.py

# 3. Entrenar modelo y pipeline (una vez)
./scripts/run_spark_detection.sh

# 4. Productor: MongoDB → Kafka (inyectar transacciones al topic 'transactions')
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MONGO_URI=mongodb://127.0.0.1:27018 python scripts/kafka_producer_mongodb.py

# 5. Consumidor Spark (otra terminal; deja corriendo)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ./scripts/run_spark_streaming_kafka.sh
```

Si el productor no conecta, prueba `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`. Alertas en Parquet para la API: `STREAMING_SINK=parquet ./scripts/run_spark_streaming_kafka.sh`.
