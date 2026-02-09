#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Structured Streaming: lee transacciones desde Kafka, aplica el pipeline de fraude
(assembler+scaler+RF) y escribe alertas (predicción=1) en consola y/o Parquet.
Requiere: Kafka con topic 'transactions', modelo pipeline guardado (run_spark_detection.py).
Uso: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 scripts/spark_streaming_kafka.py
"""
import os
import sys

if not os.getenv("SPARK_MASTER") and not os.getenv("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType, StructType, StructField
from pyspark.ml import PipelineModel

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
PIPELINE_PATH = os.getenv("SPARK_PIPELINE_PATH", "models/fraud_pipeline")
OUTPUT_ALERTS_PATH = os.getenv("SPARK_ALERTS_PATH", "data/alerts")
CHECKPOINT_PATH = os.getenv("SPARK_STREAMING_CHECKPOINT", "data/checkpoint_kafka")


def _to_local_uri(p: str) -> str:
    if not p or p.startswith("hdfs://") or p.startswith("file://"):
        return p
    return "file://" + os.path.abspath(p)


def main():
    spark = (
        SparkSession.builder.appName("FraudStreamingKafka")
        .config("spark.sql.streaming.checkpointLocation", _to_local_uri(CHECKPOINT_PATH))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    if not os.getenv("SPARK_MASTER"):
        spark.conf.set("spark.master", "local[*]")
    spark.sparkContext.setLogLevel("WARN")

    # Leer stream desde Kafka
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # value es binario; parsear JSON
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
    ])
    df = df_raw.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
    df = df.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))

    # Misma ingeniería de características que en batch
    loc_schema = "struct<user:struct<lat:double,lon:double>,merchant:struct<lat:double,lon:double>>"
    df = df.withColumn("loc", F.from_json(F.col("location"), loc_schema))
    df = (
        df.withColumn("user_lat", F.col("loc.user.lat"))
        .withColumn("user_lon", F.col("loc.user.lon"))
        .withColumn("merchant_lat", F.col("loc.merchant.lat"))
        .withColumn("merchant_lon", F.col("loc.merchant.lon"))
    ).drop("loc")
    df = df.withColumn(
        "distance_km",
        F.sqrt(
            F.pow(F.col("merchant_lat") - F.col("user_lat"), 2)
            + F.pow(F.col("merchant_lon") - F.col("user_lon"), 2)
        ) * 111.0,
    )
    df = df.withColumn("ts", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("hour", F.hour("ts")).withColumn("day_of_week", F.dayofweek("ts"))
    for c in ["amount", "distance_km", "hour", "day_of_week"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0.0))
    df = df.na.fill(0.0, subset=["distance_km", "hour", "day_of_week"])

    # Cargar pipeline (assembler + scaler + RF) y predecir
    pipeline_model = PipelineModel.load(_to_local_uri(PIPELINE_PATH))
    predictions = pipeline_model.transform(df)

    # Solo alertas (fraude predicho)
    alerts = predictions.filter(F.col("prediction") == 1.0).select(
        "transaction_id", "amount", "timestamp", "prediction", "probability"
    )

    # Un solo sink por query: consola (demo) o Parquet (producción)
    alerts_path = _to_local_uri(OUTPUT_ALERTS_PATH)
    sink = os.getenv("STREAMING_SINK", "console").lower()
    if sink == "parquet":
        query = (
            alerts.writeStream.outputMode("append")
            .format("parquet")
            .option("path", alerts_path)
            .option("checkpointLocation", _to_local_uri(CHECKPOINT_PATH) + "_parquet")
            .trigger(processingTime="30 seconds")
            .start()
        )
        print("Streaming activo. Leyendo de Kafka topic '%s'. Alertas en %s" % (KAFKA_TOPIC, alerts_path))
    else:
        query = alerts.writeStream.outputMode("append").format("console").option("truncate", False).start()
        print("Streaming activo. Leyendo de Kafka topic '%s'. Alertas en consola." % KAFKA_TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
