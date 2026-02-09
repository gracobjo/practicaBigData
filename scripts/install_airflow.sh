#!/bin/bash
# Instala Apache Airflow en un venv dedicado (evita conflictos con el proyecto).
# Uso: ./scripts/install_airflow.sh
# Después: configurar AIRFLOW_HOME y DAGs (ver README o scripts/run_airflow.sh).

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.10.2}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
# Constraints para instalación reproducible (cambiar branch si usas otra versión)
# Nombre del archivo de constraints (3.12 o 3.11 según tu Python)
CONSTRAINTS_FILE="constraints-3.12.txt"
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/${CONSTRAINTS_FILE}"
VENV_DIR="$PROJECT_DIR/airflow_venv"

echo "Proyecto: $PROJECT_DIR"
echo "Airflow:  $AIRFLOW_VERSION (Python $PYTHON_VERSION)"
echo "Venv:     $VENV_DIR"

# Crear venv solo para Airflow
if [ ! -d "$VENV_DIR" ]; then
  echo "Creando venv en $VENV_DIR ..."
  python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

# Comprobar versión Python
PY_VER=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Python en venv: $PY_VER"

# Instalar Airflow con constraints (solo extras básicos para no alargar mucho)
echo "Instalando apache-airflow (puede tardar varios minutos) ..."
pip install --upgrade pip
if ! pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINTS_URL"; then
  echo "Si falla (ej. sin constraints para 3.12), prueba: CONSTRAINTS_FILE=constraints-3.11.txt $0"
  exit 1
fi

echo ""
echo "Airflow instalado en $VENV_DIR"
echo "Siguiente: export AIRFLOW_HOME y apuntar DAGs al proyecto (ver scripts/run_airflow.sh)"
echo "  source $VENV_DIR/bin/activate"
echo "  export AIRFLOW_HOME=\${AIRFLOW_HOME:-$PROJECT_DIR/airflow_home}"
echo "  export AIRFLOW__CORE__DAGS_FOLDER=$PROJECT_DIR/airflow_dags"
echo "  airflow db init"
echo "  airflow users create --role Admin --username admin --email admin@local --firstname Admin --lastname User --password admin"
echo "  airflow standalone"
