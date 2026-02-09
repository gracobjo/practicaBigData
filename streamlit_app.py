#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard Streamlit: alertas de fraude, transacciones y resumen del modelo.
Uso: streamlit run streamlit_app.py
"""
import os
import json
from pathlib import Path

import pandas as pd
import streamlit as st

# Rutas por defecto (respecto a la raíz del proyecto)
PROJECT_ROOT = Path(__file__).resolve().parent


def _alerts_dir():
    p = os.getenv("SPARK_ALERTS_PATH", "data/alerts")
    return PROJECT_ROOT / p if not Path(p).is_absolute() else Path(p)


def _transactions_dir():
    return PROJECT_ROOT / "data" / "transactions"


METRICS_FILE = PROJECT_ROOT / "data" / "metrics.json"

st.set_page_config(
    page_title="Detección de fraude",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🛡️ Dashboard de detección de fraude")
st.caption("Sistema KDD – Alertas del modelo Random Forest (Spark) y datos de transacciones")


@st.cache_data(ttl=60)
def load_alerts():
    """Carga alertas desde Parquet (salida de spark_detection.py)."""
    path = _alerts_dir()
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        return df
    except Exception as e:
        st.warning(f"No se pudieron cargar alertas desde {path}: {e}")
        return None


@st.cache_data(ttl=60)
def load_transactions(max_rows: int = 5000):
    """Carga transacciones desde JSON (export MongoDB)."""
    path = _transactions_dir() / "transactions.json"
    if not path.exists():
        return None
    try:
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= max_rows:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                    rows.append({
                        "transaction_id": doc.get("transaction_id"),
                        "user_id": doc.get("user_id"),
                        "merchant_id": doc.get("merchant_id"),
                        "amount": doc.get("amount"),
                        "timestamp": doc.get("timestamp"),
                        "is_fraud": doc.get("is_fraud", False),
                    })
                except json.JSONDecodeError:
                    continue
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        st.warning(f"No se pudieron cargar transacciones desde {path}: {e}")
        return None


def load_metrics():
    """Carga métricas guardadas (si existen)."""
    path = METRICS_FILE
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# Sidebar: sección
section = st.sidebar.radio(
    "Sección",
    ["Resumen", "Alertas de fraude", "Transacciones", "Métricas del modelo"],
    label_visibility="collapsed",
)

alerts_df = load_alerts()
trans_df = load_transactions()

# --- Resumen
if section == "Resumen":
    st.header("Resumen")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        n_alerts = len(alerts_df) if alerts_df is not None else 0
        st.metric("Alertas de fraude", n_alerts)
    with c2:
        total_alerts = float(alerts_df["amount"].sum()) if alerts_df is not None and not alerts_df.empty and "amount" in alerts_df.columns else 0
        st.metric("Monto total en alertas (€)", f"{total_alerts:,.2f}")
    with c3:
        n_tx = len(trans_df) if trans_df is not None else 0
        st.metric("Transacciones cargadas", n_tx)
    with c4:
        n_fraud_tx = int(trans_df["is_fraud"].sum()) if trans_df is not None and "is_fraud" in trans_df.columns else 0
        st.metric("Fraude (etiqueta) en muestra", n_fraud_tx)

    if alerts_df is not None and not alerts_df.empty:
        st.subheader("Distribución del monto en alertas")
        st.bar_chart(alerts_df["amount"].value_counts().sort_index().head(30))
    elif alerts_df is None or alerts_df.empty:
        st.info("Ejecuta primero `./scripts/run_spark_detection.sh` o `python scripts/spark_detection.py` para generar alertas en `data/alerts`.")

# --- Alertas de fraude
if section == "Alertas de fraude":
    st.header("Alertas de fraude")
    if alerts_df is not None and not alerts_df.empty:
        # Formatear probability si existe (puede ser lista [p_no_fraud, p_fraud])
        if "probability" in alerts_df.columns:
            try:
                alerts_df = alerts_df.copy()
                alerts_df["prob_fraude"] = alerts_df["probability"].apply(
                    lambda x: float(x[1]) if isinstance(x, (list, tuple)) and len(x) > 1 else float(x) if isinstance(x, (int, float)) else None
                )
            except Exception:
                pass
        st.dataframe(alerts_df, use_container_width=True, height=400)
        st.subheader("Monto por alerta")
        st.bar_chart(alerts_df["amount"])
    else:
        st.warning("No hay alertas cargadas. Genera el modelo y las alertas con `./scripts/run_spark_detection.sh` o `python scripts/spark_detection.py`.")

# --- Transacciones
if section == "Transacciones":
    st.header("Transacciones (muestra)")
    if trans_df is not None and not trans_df.empty:
        st.caption("Origen: data/transactions/transactions.json (export desde MongoDB)")
        st.dataframe(trans_df, use_container_width=True, height=400)
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Distribución: fraude vs normal")
            st.bar_chart(trans_df["is_fraud"].value_counts())
        with col2:
            st.subheader("Distribución del monto")
            bins = pd.cut(trans_df["amount"], bins=15)
            st.bar_chart(bins.value_counts().sort_index())
    else:
        st.warning("No hay transacciones. Exporta MongoDB a JSON con `MONGO_URI=mongodb://127.0.0.1:27018 python scripts/export_mongodb_to_json.py`.")

# --- Métricas del modelo
if section == "Métricas del modelo":
    st.header("Métricas del modelo")
    metrics = load_metrics()
    if metrics:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("AUC-ROC", f"{metrics.get('auc_roc', 0):.4f}")
        m2.metric("AUC-PR", f"{metrics.get('auc_pr', 0):.4f}")
        m3.metric("Accuracy", f"{metrics.get('accuracy', 0):.4f}")
        m4.metric("F1", f"{metrics.get('f1', 0):.4f}")
        st.json(metrics)
    else:
        st.info(
            "Las métricas se muestran en consola al ejecutar `run_spark_detection.sh` o `spark_detection.py`. "
            "Para verlas aquí, guarda un archivo `data/metrics.json` con claves: auc_roc, auc_pr, accuracy, f1."
        )

st.sidebar.divider()
st.sidebar.caption("Datos: data/alerts (Parquet), data/transactions/transactions.json")
