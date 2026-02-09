#!/bin/bash
# Arranca el consumidor Spark Structured Streaming desde Kafka.
# Requiere: Kafka con topic 'transactions', pipeline guardado (./scripts/run_spark_detection.sh).
# Uso: ./scripts/run_spark_streaming_kafka.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Detectar Spark
if [ -z "$SPARK_HOME" ] || [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  for d in /home/hadoop/spark /opt/spark /usr/local/spark; do
    if [ -x "$d/bin/spark-submit" ]; then
      export SPARK_HOME="$d"
      break
    fi
  done
fi

if [ -z "$SPARK_HOME" ]; then
  echo "No se encontró Spark. Exporta SPARK_HOME."
  exit 1
fi

# Paquete Kafka para Spark (Scala 2.12, Spark 3.5)
KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0}"

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export SPARK_PIPELINE_PATH="${SPARK_PIPELINE_PATH:-$PROJECT_DIR/models/fraud_pipeline}"

echo "Kafka: $KAFKA_BOOTSTRAP_SERVERS"
echo "Pipeline: $SPARK_PIPELINE_PATH"
exec "$SPARK_HOME/bin/spark-submit" \
  --packages "$KAFKA_PACKAGE" \
  --master "${SPARK_MASTER:-spark://nodo1:7077}" \
  "$SCRIPT_DIR/spark_streaming_kafka.py" "$@"
