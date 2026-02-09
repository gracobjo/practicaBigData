#!/bin/bash
# Entrenar modelo de fraude con Spark.
# - Si SPARK_HOME está definido (o existe /home/hadoop/spark): usa spark-submit.
# - Si no: usa python (puede fallar en algunos entornos).

set -e
cd "$(dirname "$0")/.."
SCRIPT="scripts/spark_detection.py"

# Detectar Spark: usar SPARK_HOME solo si existe bin/spark-submit; si no, buscar en rutas habituales
if [ -z "$SPARK_HOME" ] || [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_HOME=""
  for d in /home/hadoop/spark /opt/spark /usr/local/spark; do
    if [ -x "$d/bin/spark-submit" ]; then
      export SPARK_HOME="$d"
      break
    fi
  done
fi

if [ -n "$SPARK_HOME" ] && [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "Usando spark-submit desde $SPARK_HOME (master: ${SPARK_MASTER:-spark://nodo1:7077})"
  # Usar Python del venv si existe (evita ModuleNotFoundError: distutils en Python 3.12)
  VENV_PYTHON="$(pwd)/venv/bin/python"
  if [ -x "$VENV_PYTHON" ]; then
    export PYSPARK_PYTHON="$VENV_PYTHON"
    export PYSPARK_DRIVER_PYTHON="$VENV_PYTHON"
  fi
  exec "$SPARK_HOME/bin/spark-submit" \
    --master "${SPARK_MASTER:-spark://nodo1:7077}" \
    "$SCRIPT" "$@"
fi

echo "SPARK_HOME no definido; ejecutando con python (PySpark en venv)."
echo "Si falla con TypeError, instala Spark y ejecuta: SPARK_HOME=/ruta/spark $0"
exec python "$SCRIPT" "$@"
