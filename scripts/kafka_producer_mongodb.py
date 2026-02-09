#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Productor Kafka: lee transacciones de MongoDB y las publica en un topic.
Integración ingesta: MongoDB → Kafka (buffer de streaming para Spark).
Uso: python scripts/kafka_producer_mongodb.py
     KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MONGO_URI=mongodb://127.0.0.1:27018 python scripts/kafka_producer_mongodb.py
"""
import json
import os
import sys
from datetime import datetime, timezone

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27018")
MONGO_DB = os.getenv("MONGO_DB", "fraud_detection")
MONGO_COLLECTION = "transactions"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
# Límite de documentos a enviar (0 = todos); para modo continuo usar --tail o similar
LIMIT = int(os.getenv("PRODUCER_LIMIT", "0"))


def serialize_doc(doc):
    """Convierte documento MongoDB a JSON serializable."""
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return json.dumps(out, default=str)


def main():
    try:
        from pymongo import MongoClient
    except ImportError:
        print("Instala pymongo: pip install pymongo", file=sys.stderr)
        sys.exit(1)
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Instala kafka-python: pip install kafka-python", file=sys.stderr)
        sys.exit(1)

    print(f"Conectando a MongoDB: {MONGO_URI}")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    print(f"Conectando a Kafka: {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
    )

    cursor = coll.find({}).limit(LIMIT) if LIMIT else coll.find({})
    sent = 0
    for doc in cursor:
        msg = serialize_doc(doc)
        producer.send(KAFKA_TOPIC, value=msg, key=doc.get("transaction_id", "").encode("utf-8"))
        sent += 1
        if sent % 500 == 0:
            print(f"Enviadas {sent} transacciones...")
    producer.flush()
    producer.close()
    client.close()
    print(f"Listo. Enviadas {sent} transacciones al topic '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    main()
