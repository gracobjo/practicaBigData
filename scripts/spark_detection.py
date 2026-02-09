#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detección de fraude con PySpark: transformación (KDD), Random Forest y evaluación AUC-ROC.
Incluye VectorAssembler, normalización y métricas. Pensado para cargar datos desde HDFS/JSON
y opcionalmente modelo desde HDFS.
Uso: spark-submit scripts/spark_detection.py
     o: python scripts/spark_detection.py (con SPARK_LOCAL_IP=127.0.0.1 si hay fallos de bind)
"""
import os
import sys
# Forzar bind a localhost al ejecutar con python (evita BindException en algunos nodos)
if not os.getenv("SPARK_MASTER") and not os.getenv("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, BooleanType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

# -----------------------------------------------------------------------------
# KDD - Fase 1 (Selección): Rutas de datos y modelo (HDFS o local)
# -----------------------------------------------------------------------------
# Rutas: si no empiezan por hdfs:// o file://, se tratan como local (file://) para no usar HDFS por defecto
def _local_path(p: str) -> str:
    if not p or p.startswith("hdfs://") or p.startswith("file://"):
        return p
    abs_p = os.path.abspath(p)
    return "file://" + abs_p if os.path.exists(abs_p) else p

def _to_local_uri(p: str) -> str:
    """Convierte ruta relativa/absoluta a file:// para no usar HDFS."""
    if not p or p.startswith("hdfs://") or p.startswith("file://"):
        return p
    return "file://" + os.path.abspath(p)

DATA_PATH = os.getenv("SPARK_DATA_PATH", "data/transactions")
MODEL_PATH = os.getenv("SPARK_MODEL_PATH", "models/fraud_rf_model")
# Pipeline completo (assembler+scaler+RF) para streaming Kafka
PIPELINE_PATH = os.getenv("SPARK_PIPELINE_PATH", os.path.join(os.path.dirname(MODEL_PATH), "fraud_pipeline"))
OUTPUT_ALERTS_PATH = os.getenv("SPARK_ALERTS_PATH", "data/alerts")
# Modo: "train" = entrenar y guardar; "inference" = cargar modelo desde HDFS y predecir
RUN_MODE = os.getenv("RUN_MODE", "train")


def get_spark_session() -> SparkSession:
    """Crea sesión Spark (local o cluster)."""
    builder = (
        SparkSession.builder.appName("FraudDetectionKDD")
        # En local: evita fallos de bind (driver y UI)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
    )
    if not os.getenv("SPARK_MASTER"):
        builder = builder.master("local[*]")
    if os.getenv("SPARK_USE_HIVE", "").lower() in ("1", "true", "yes"):
        builder = builder.config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport()
    return builder.getOrCreate()


def main() -> None:
    spark = get_spark_session()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # -------------------------------------------------------------------------
    # KDD - Fase 1 (Selección): Carga del conjunto de datos
    # -------------------------------------------------------------------------
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("location", StringType(), True),  # JSON string o struct
        StructField("timestamp", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
    ])

    # Si existe DATA_PATH como directorio, leer desde ahí (file:// evita HDFS si no está)
    path_to_read = _local_path(DATA_PATH)
    if os.path.isdir(DATA_PATH):
        df = spark.read.schema(schema).json(path_to_read)
    else:
        # Datos de ejemplo en memoria para demo (sin HDFS)
        from pyspark.sql import Row
        import random
        rows = []
        for i in range(1000):
            is_fraud = i < 50
            amount = random.uniform(500, 10000) if is_fraud else random.uniform(10, 500)
            rows.append(Row(
                transaction_id=f"tx_{i}",
                user_id=f"u_{i % 100}",
                merchant_id=f"M_{random.randint(1, 20)}",
                amount=round(amount, 2),
                location='{"user":{"lat":40.4,"lon":-3.7},"merchant":{"lat":40.5,"lon":-3.6}}',
                timestamp="2025-02-01T12:00:00",
                is_fraud=is_fraud,
            ))
        df = spark.createDataFrame(rows)

    # -------------------------------------------------------------------------
    # KDD - Fase 2 (Preprocesamiento): Limpieza
    # -------------------------------------------------------------------------
    df = df.dropDuplicates(["transaction_id"])
    df = df.filter(F.col("amount") > 0)
    df = df.na.drop(subset=["amount", "is_fraud"])

    # -------------------------------------------------------------------------
    # KDD - Fase 3 (Transformación): Ingeniería de características y normalización
    # -------------------------------------------------------------------------
    # Extraer lat/lon si location es JSON (KDD - Transformación: variables derivadas)
    loc_schema = "struct<user:struct<lat:double,lon:double>,merchant:struct<lat:double,lon:double>>"
    if "location" in df.columns and isinstance(df.schema["location"].dataType, StringType):
        df = df.withColumn("loc", F.from_json(F.col("location"), loc_schema))
        df = (
            df.withColumn("user_lat", F.col("loc.user.lat"))
            .withColumn("user_lon", F.col("loc.user.lon"))
            .withColumn("merchant_lat", F.col("loc.merchant.lat"))
            .withColumn("merchant_lon", F.col("loc.merchant.lon"))
        )
        df = df.drop("loc")
    else:
        df = df.withColumn("user_lat", F.lit(40.4)).withColumn("user_lon", F.lit(-3.7))
        df = df.withColumn("merchant_lat", F.lit(40.5)).withColumn("merchant_lon", F.lit(-3.6))

    # Feature: distancia aproximada (Haversine simplificado para demo)
    df = df.withColumn(
        "distance_km",
        F.sqrt(
            F.pow(F.col("merchant_lat") - F.col("user_lat"), 2)
            + F.pow(F.col("merchant_lon") - F.col("user_lon"), 2)
        ) * 111.0,
    )

    # Feature: hora del día (si timestamp es parseable)
    df = df.withColumn("ts", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("hour", F.hour("ts")).withColumn("day_of_week", F.dayofweek("ts"))

    # Vector de características (inputs para el modelo)
    feature_cols = ["amount", "distance_km", "hour", "day_of_week"]
    for c in feature_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0.0))

    # VectorAssembler (KDD - Transformación)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    df = assembler.transform(df)

    # Normalización (KDD - Transformación): StandardScaler
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Etiqueta numérica para MLlib
    df = df.withColumn("label", F.when(F.col("is_fraud"), 1).otherwise(0))

    # -------------------------------------------------------------------------
    # KDD - Fase 4 (Minería de Datos): Entrenamiento del modelo
    # -------------------------------------------------------------------------
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=50,
        maxDepth=10,
        seed=42,
    )
    model = rf.fit(train)

    # Persistir modelo RF (file:// si no es hdfs:// para no requerir HDFS)
    model.write().overwrite().save(_to_local_uri(MODEL_PATH))

    # Persistir pipeline completo (assembler+scaler+RF) para consumo desde Kafka streaming
    train_features = train.select("amount", "distance_km", "hour", "day_of_week", "label")
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    pipeline_model = pipeline.fit(train_features)
    pipeline_model.write().overwrite().save(_to_local_uri(PIPELINE_PATH))

    # -------------------------------------------------------------------------
    # KDD - Fase 5 (Interpretación / Evaluación): Métricas y AUC-ROC
    # -------------------------------------------------------------------------
    predictions = model.transform(test)
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = evaluator_auc.evaluate(predictions)
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    aupr = evaluator_pr.evaluate(predictions)

    print("=" * 60)
    print("KDD - Fase 5: Evaluación del modelo")
    print("=" * 60)
    print(f"AUC-ROC: {auc:.4f}")
    print(f"AUC-PR:  {aupr:.4f}")

    # Métricas por clase (precisión, recall)
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    print(f"Accuracy: {acc_eval.evaluate(predictions):.4f}")
    print(f"F1:       {f1_eval.evaluate(predictions):.4f}")
    print("=" * 60)

    # Guardar alertas (predicciones positivas) para consumo de la API
    alerts = predictions.filter(F.col("prediction") == 1.0).select(
        "transaction_id", "amount", "timestamp", "prediction", "probability"
    )
    alerts_path = _to_local_uri(OUTPUT_ALERTS_PATH)
    alerts.write.mode("overwrite").parquet(alerts_path)
    print(f"Alertas guardadas en {alerts_path}")

    spark.stop()


def run_inference(spark: SparkSession) -> None:
    """
    KDD - Fase 4 (Minería) / Fase 5 (Interpretación): Carga el modelo desde HDFS (o path local)
    y aplica predicción sobre datos nuevos (streaming o batch).
    Uso: RUN_MODE=inference spark-submit scripts/spark_detection.py
    """
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    # Cargar modelo persistido (soporta hdfs:// o file://)
    model = RandomForestClassificationModel.load(_to_local_uri(MODEL_PATH))
    # Leer datos nuevos (ej. desde DATA_PATH o streaming)
    if os.path.isdir(DATA_PATH):
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
        ])
        df = spark.read.schema(schema).json(DATA_PATH)
    else:
        print("No hay datos en DATA_PATH para inferencia. Ejecute con SPARK_DATA_PATH apuntando a JSON/Parquet.")
        return
    # Misma transformación que en entrenamiento (KDD - Transformación)
    df = df.dropDuplicates(["transaction_id"]).filter(F.col("amount") > 0)
    loc_schema = "struct<user:struct<lat:double,lon:double>,merchant:struct<lat:double,lon:double>>"
    if "location" in df.columns and isinstance(df.schema["location"].dataType, StringType):
        df = df.withColumn("loc", F.from_json(F.col("location"), loc_schema))
        df = (
            df.withColumn("user_lat", F.col("loc.user.lat"))
            .withColumn("user_lon", F.col("loc.user.lon"))
            .withColumn("merchant_lat", F.col("loc.merchant.lat"))
            .withColumn("merchant_lon", F.col("loc.merchant.lon"))
        ).drop("loc")
    else:
        df = df.withColumn("user_lat", F.lit(40.4)).withColumn("user_lon", F.lit(-3.7))
        df = df.withColumn("merchant_lat", F.lit(40.5)).withColumn("merchant_lon", F.lit(-3.6))
    df = df.withColumn(
        "distance_km",
        F.sqrt(
            F.pow(F.col("merchant_lat") - F.col("user_lat"), 2)
            + F.pow(F.col("merchant_lon") - F.col("user_lon"), 2)
        ) * 111.0,
    )
    df = df.withColumn("ts", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("hour", F.hour("ts")).withColumn("day_of_week", F.dayofweek("ts"))
    feature_cols = ["amount", "distance_km", "hour", "day_of_week"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    df = assembler.transform(df)
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)
    predictions = model.transform(df)
    alerts = predictions.filter(F.col("prediction") == 1.0).select(
        "transaction_id", "amount", "timestamp", "prediction", "probability"
    )
    alerts.write.mode("append").parquet(_to_local_uri(OUTPUT_ALERTS_PATH))
    print(f"Inferencia completada. Alertas escritas en {OUTPUT_ALERTS_PATH}")


if __name__ == "__main__":
    if RUN_MODE == "inference":
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        run_inference(spark)
        spark.stop()
    else:
        main()
