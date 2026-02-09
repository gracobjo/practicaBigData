# Ingesta de datos

En este proyecto la ingesta hacia Kafka se hace con un **productor Python** que lee desde MongoDB.

## MongoDB → Kafka

- **Script:** `../scripts/kafka_producer_mongodb.py`
- **Topic Kafka:** `transactions` (configurable con `KAFKA_TOPIC_TRANSACTIONS`)
- **Uso:** ver [README principal](../README.md#flujo-con-kafka-mongodb--kafka--spark-streaming).

## NiFi (opcional)

Apache NiFi podría sustituir o complementar al productor: flujo gráfico que lee de MongoDB (o JDBC/API) y publica en Kafka. La configuración de esos flujos quedaría en este directorio (templates XML o documentación). Hoy el flujo activo es el productor Python.
