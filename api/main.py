# api/main.py
"""
API REST para consulta de alertas de fraude.
KDD - Fase 5 (Interpretación): Exponer resultados del modelo para toma de decisiones.
Documentación Swagger: http://localhost:8000/docs
"""

import os
import subprocess
import time
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# -----------------------------------------------------------------------------
# Configuración: origen de alertas (Parquet generado por Spark o simulación)
# -----------------------------------------------------------------------------
ALERTS_PATH = os.getenv("ALERTS_PATH", "data/alerts")
USE_MOCK = os.getenv("USE_MOCK_ALERTS", "true").lower() == "true"
# Cuando ALERTS_PATH es hdfs://, se copia aquí con hdfs dfs -get (refresco cada N segundos)
ALERTS_CACHE_DIR = os.getenv("ALERTS_CACHE_DIR", "data/alerts_cache")
ALERTS_HDFS_SYNC_INTERVAL_SEC = int(os.getenv("ALERTS_HDFS_SYNC_INTERVAL_SEC", "60"))

# Última sincronización desde HDFS (timestamp)
_hdfs_sync_time: float = 0

# MongoDB para la página de visualización de transacciones
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27018")
MONGO_DB = os.getenv("MONGO_DB", "fraud_detection")
MONGO_COLLECTION = "transactions"


class AlertItem(BaseModel):
    """Modelo de una alerta de fraude (KDD - Interpretación: estructura de salida)."""
    transaction_id: str
    amount: float
    timestamp: Optional[str] = None
    prediction: float
    probability: Optional[float] = None


class HealthResponse(BaseModel):
    """Estado del servicio."""
    status: str
    alerts_path: str
    use_mock: bool
    from_hdfs: bool = False


# -----------------------------------------------------------------------------
# Aplicación FastAPI con documentación Swagger
# -----------------------------------------------------------------------------
app = FastAPI(
    title="API Detección de Fraude Bancario",
    description="Endpoints para consultar alertas generadas por el modelo Spark (Random Forest). "
                "Integrado con el flujo KDD - Fase 5: Interpretación.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


def _get_effective_alerts_path() -> str:
    """
    Si ALERTS_PATH es hdfs://, copia a carpeta local con hdfs dfs -get y devuelve esa ruta.
    Si no, devuelve ALERTS_PATH (local o file://).
    """
    global _hdfs_sync_time
    if not ALERTS_PATH.startswith("hdfs://"):
        return ALERTS_PATH
    # Refresco limitado para no llamar a hdfs en cada request
    now = time.time()
    if now - _hdfs_sync_time < ALERTS_HDFS_SYNC_INTERVAL_SEC and os.path.isdir(ALERTS_CACHE_DIR):
        tail = ALERTS_PATH.rstrip("/").split("/")[-1]
        candidate = os.path.join(ALERTS_CACHE_DIR, tail)
        if os.path.isdir(candidate):
            return candidate
        if os.path.isdir(ALERTS_CACHE_DIR):
            return ALERTS_CACHE_DIR
    try:
        os.makedirs(ALERTS_CACHE_DIR, exist_ok=True)
        subprocess.run(
            ["hdfs", "dfs", "-get", "-f", ALERTS_PATH, ALERTS_CACHE_DIR],
            check=True,
            capture_output=True,
            timeout=30,
        )
        _hdfs_sync_time = time.time()
        tail = ALERTS_PATH.rstrip("/").split("/")[-1]
        return os.path.join(ALERTS_CACHE_DIR, tail)
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo copiar alertas desde HDFS: {e.stderr.decode() if e.stderr else str(e)}",
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="Comando 'hdfs' no encontrado. ¿Hadoop en el PATH?",
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Error sincronizando HDFS: {e}")


def _load_alerts_from_parquet() -> List[AlertItem]:
    """Carga alertas desde el directorio Parquet escrito por Spark (KDD - Interpretación)."""
    try:
        import pandas as pd
        path = _get_effective_alerts_path()
        if not os.path.isdir(path):
            return []
        df = pd.read_parquet(path)
        if df.empty:
            return []
        # Ajustar columnas según salida de Spark (probability puede ser vector)
        records = []
        for _, row in df.iterrows():
            prob = row.get("probability")
            if hasattr(prob, "__iter__") and not isinstance(prob, str):
                prob = float(prob[1]) if len(prob) > 1 else float(prob[0]) if prob else None
            else:
                prob = float(prob) if prob is not None else None
            records.append(AlertItem(
                transaction_id=str(row.get("transaction_id", "")),
                amount=float(row.get("amount", 0)),
                timestamp=str(row.get("timestamp")) if pd.notna(row.get("timestamp")) else None,
                prediction=float(row.get("prediction", 0)),
                probability=prob,
            ))
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo alertas: {e}")


def _get_mock_alerts() -> List[AlertItem]:
    """Alertas de ejemplo cuando no hay Parquet (desarrollo/demo)."""
    return [
        AlertItem(
            transaction_id="tx_demo_001",
            amount=1250.50,
            timestamp=datetime.utcnow().isoformat() + "Z",
            prediction=1.0,
            probability=0.92,
        ),
        AlertItem(
            transaction_id="tx_demo_002",
            amount=8900.00,
            timestamp=datetime.utcnow().isoformat() + "Z",
            prediction=1.0,
            probability=0.87,
        ),
    ]


def _get_mongo_transactions(limit: int = 500) -> list:
    """Lee transacciones de MongoDB para la página de datos."""
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        coll = db[MONGO_COLLECTION]
        rows = list(coll.find({}, {"_id": 0}).limit(limit))
        client.close()
        return rows
    except Exception:
        return []


@app.get("/data", response_class=HTMLResponse)
def page_transactions(
    limit: int = Query(200, ge=1, le=2000, description="Máximo de filas"),
):
    """
    Página web con la tabla de transacciones de MongoDB (datos de prueba).
    """
    rows = _get_mongo_transactions(limit=limit)
    total = len(rows)
    html_rows = []
    for r in rows:
        amount = r.get("amount", "")
        ts = r.get("timestamp", "")
        is_fraud = "Sí" if r.get("is_fraud") else "No"
        tx_id = r.get("transaction_id", "")[:20] + "..." if len(str(r.get("transaction_id", ""))) > 20 else r.get("transaction_id", "")
        loc = r.get("location", {})
        loc_str = str(loc)[:50] + "..." if len(str(loc)) > 50 else str(loc)
        html_rows.append(
            f"<tr><td>{tx_id}</td><td>{amount}</td><td>{ts}</td><td>{is_fraud}</td><td>{loc_str}</td></tr>"
        )
    table_body = "\n".join(html_rows) if html_rows else "<tr><td colspan='5'>Sin datos. ¿MongoDB en marcha y poblado? (scripts/populate_mongodb.py)</td></tr>"
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Transacciones MongoDB - Fraude</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 1rem 2rem; background: #1a1a2e; color: #eee; }}
    h1 {{ color: #e94560; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
    th, td {{ border: 1px solid #444; padding: 0.5rem 0.75rem; text-align: left; }}
    th {{ background: #16213e; color: #e94560; }}
    tr:nth-child(even) {{ background: #16213e; }}
    a {{ color: #e94560; }}
    .meta {{ color: #888; margin-bottom: 1rem; }}
  </style>
</head>
<body>
  <h1>Transacciones (MongoDB)</h1>
  <p class="meta">Mostrando hasta {total} registros. Base: {MONGO_DB}.{MONGO_COLLECTION}</p>
  <p><a href="/docs">API Swagger</a> | <a href="/alerts">Alertas (JSON)</a></p>
  <table>
    <thead><tr><th>transaction_id</th><th>amount</th><th>timestamp</th><th>is_fraud</th><th>location</th></tr></thead>
    <tbody>
    {table_body}
    </tbody>
  </table>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/health", response_model=HealthResponse)
def health():
    """
    Estado del servicio y configuración.
    Útil para orquestación y comprobación de conectividad.
    """
    return HealthResponse(
        status="ok",
        alerts_path=ALERTS_PATH,
        use_mock=USE_MOCK,
        from_hdfs=ALERTS_PATH.startswith("hdfs://"),
    )


@app.get("/alerts", response_model=List[AlertItem])
def list_alerts(
    limit: int = Query(100, ge=1, le=1000, description="Máximo número de alertas a devolver"),
    min_amount: Optional[float] = Query(None, description="Filtrar por monto mínimo"),
    max_amount: Optional[float] = Query(None, description="Filtrar por monto máximo"),
):
    """
    Lista alertas de fraude generadas por el modelo Spark.
    KDD - Fase 5 (Interpretación): acceso a las predicciones positivas para análisis.
    """
    if USE_MOCK:
        items = _get_mock_alerts()
    else:
        items = _load_alerts_from_parquet()
    if min_amount is not None:
        items = [a for a in items if a.amount >= min_amount]
    if max_amount is not None:
        items = [a for a in items if a.amount <= max_amount]
    return items[:limit]


@app.get("/alerts/{transaction_id}", response_model=AlertItem)
def get_alert_by_id(transaction_id: str):
    """
    Obtiene una alerta por identificador de transacción.
    Devuelve 404 si no existe.
    """
    if USE_MOCK:
        items = _get_mock_alerts()
    else:
        items = _load_alerts_from_parquet()
    for a in items:
        if a.transaction_id == transaction_id:
            return a
    raise HTTPException(status_code=404, detail="Alerta no encontrada")


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
