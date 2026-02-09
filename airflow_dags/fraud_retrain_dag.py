# airflow_dags/fraud_retrain_dag.py
"""
DAG de Airflow para re-entrenamiento periódico del modelo de fraude.
KDD - Fase 4 (Minería): orquestación del pipeline de entrenamiento.
Ejecuta run_spark_detection.sh con datos en HDFS.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Rutas del proyecto (ajustar si instalas en otro nodo)
PROJECT_DIR = "/home/hadoop/proyectoFinBigData"
SPARK_DATA_PATH = "hdfs://nodo1:9000/user/hadoop/fraud/transactions"
SPARK_MODEL_PATH = "hdfs://nodo1:9000/user/hadoop/fraud/models/fraud_rf_model"
SPARK_ALERTS_PATH = "hdfs://nodo1:9000/user/hadoop/fraud/alerts"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_model_retrain",
    default_args=default_args,
    description="Re-entrena el modelo Random Forest de detección de fraude (KDD - Minería)",
    schedule_interval=timedelta(days=7),  # Semanal
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "ml", "kdd"],
) as dag:

    run_spark_training = BashOperator(
        task_id="run_spark_training",
        bash_command=f"""
cd {PROJECT_DIR} && \
export SPARK_DATA_PATH={SPARK_DATA_PATH} && \
export SPARK_MODEL_PATH={SPARK_MODEL_PATH} && \
export SPARK_ALERTS_PATH={SPARK_ALERTS_PATH} && \
./scripts/run_spark_detection.sh
""",
        env={
            "SPARK_DATA_PATH": SPARK_DATA_PATH,
            "SPARK_MODEL_PATH": SPARK_MODEL_PATH,
            "SPARK_ALERTS_PATH": SPARK_ALERTS_PATH,
        },
    )

    validate_metrics = BashOperator(
        task_id="validate_metrics",
        bash_command="echo 'Revisar logs del task run_spark_training para AUC-ROC y F1'",
    )

    run_spark_training >> validate_metrics
