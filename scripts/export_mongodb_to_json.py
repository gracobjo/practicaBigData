#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exporta transacciones de MongoDB a JSON para que Spark las lea (SPARK_DATA_PATH).
KDD - Fase 1 (Selección): preparar el conjunto de datos para el pipeline Spark.
Uso: python scripts/export_mongodb_to_json.py
"""
import json
import os
from datetime import datetime, timezone

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "fraud_detection")
COLLECTION = "transactions"
OUTPUT_DIR = os.getenv("SPARK_DATA_PATH", "data/transactions")

def main():
    try:
        from pymongo import MongoClient
    except ImportError:
        raise SystemExit("Instala pymongo: pip install pymongo")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[COLLECTION]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_file = os.path.join(OUTPUT_DIR, "transactions.json")

    # Convertir a documentos serializables; location como string JSON para Spark
    docs = []
    for d in coll.find():
        doc = {}
        for k, v in d.items():
            if k == "_id":
                doc[k] = str(v)
            elif k == "location" and isinstance(v, dict):
                doc[k] = json.dumps(v)
            elif isinstance(v, datetime):
                doc[k] = v.isoformat()
            else:
                doc[k] = v
        docs.append(doc)

    with open(out_file, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"Exportadas {len(docs)} transacciones a {out_file}")
    client.close()

if __name__ == "__main__":
    main()
