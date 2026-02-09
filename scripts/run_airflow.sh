#!/bin/bash
# Arranca Airflow (standalone: webserver + scheduler en un proceso).
# Ejecutar después de install_airflow.sh y airflow db init.
# Uso: ./scripts/run_airflow.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_DIR/airflow_venv"
AIRFLOW_HOME="${AIRFLOW_HOME:-$PROJECT_DIR/airflow_home}"

if [ ! -d "$VENV_DIR" ]; then
  echo "Primero ejecuta: ./scripts/install_airflow.sh"
  exit 1
fi

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/airflow_dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
# Escuchar en todas las interfaces para acceder desde otro equipo (ej. http://nodo1:8080)
export AIRFLOW__WEBSERVER__WEB_SERVER_HOST="0.0.0.0"
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG="False"

mkdir -p "$AIRFLOW_HOME"

# Inicializar DB si no existe
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
  echo "Inicializando base de datos Airflow en $AIRFLOW_HOME ..."
  source "$VENV_DIR/bin/activate"
  airflow db init
  echo "Crear usuario admin: airflow users create --role Admin --username admin --email admin@local --firstname Admin --lastname User --password admin"
fi

source "$VENV_DIR/bin/activate"
echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DAGs:      $AIRFLOW__CORE__DAGS_FOLDER"
echo "Web UI:    http://$(hostname -f):8080 (usuario: admin, contraseña: admin si la creaste)"
exec airflow standalone
