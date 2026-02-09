#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Poblado de MongoDB con transacciones sintéticas para simulación de fraude.
KDD - Fase 1 (Selección): Define el dominio de datos (transacciones con monto, ubicación, timestamp).
Uso: python scripts/populate_mongodb.py
"""

import os
import random
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

# pymongo se instala vía requirements.txt
try:
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
except ImportError:
    raise ImportError("Instala pymongo: pip install pymongo")

# -----------------------------------------------------------------------------
# Configuración (KDD - Selección: criterios de la fuente de datos)
# -----------------------------------------------------------------------------
# Permite MONGO_URI completo o MONGO_HOST + MONGO_PORT (útil en clusters: host del nodo donde corre Docker)
# Por defecto 127.0.0.1 evita problemas con localhost resolviendo a IPv6 (::1) si el puerto solo escucha en IPv4
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
    # 27018 = puerto del host cuando MongoDB corre en Docker (docker-compose: 27018:27017)
    MONGO_PORT = os.getenv("MONGO_PORT", "27018")
    MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
DB_NAME = os.getenv("MONGO_DB", "fraud_detection")
COLLECTION_NAME = "transactions"
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", "10000"))
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.05"))  # ~5% fraude
# Timeout suficiente para conexión y para insert_many (5s a veces poco en Docker/red)
CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "30000"))

# Ubicaciones de ejemplo (lat, lon) para simular geolocalización
LOCATIONS = [
    (40.4168, -3.7038),   # Madrid
    (41.3851, 2.1734),   # Barcelona
    (37.3891, -5.9845),  # Sevilla
    (39.4699, -0.3763),  # Valencia
    (43.2630, -2.9350),  # Bilbao
    (36.7213, -4.4214),  # Málaga
    (28.1248, -15.4300), # Las Palmas
    (38.3452, -0.4810),  # Alicante
    (42.8782, -8.5448),  # Santiago
    (41.6488, -0.8891),  # Zaragoza
]


def generate_transaction(transaction_id: str, is_fraud: bool, base_time: datetime) -> Dict[str, Any]:
    """
    Genera una transacción sintética.
    KDD - Selección: atributos elegidos (monto, ubicación, timestamp, etiqueta).
    """
    # Monto: fraudes suelen tener montos atípicos (muy altos o muy bajos)
    if is_fraud:
        amount = round(random.uniform(500, 15000) if random.random() > 0.5 else random.uniform(5, 50), 2)
    else:
        amount = round(random.uniform(10, 500), 2)

    # Ubicación: usuario y comercio (en fraude a veces muy lejanos)
    user_location = random.choice(LOCATIONS)
    if is_fraud and random.random() > 0.6:
        merchant_location = random.choice([loc for loc in LOCATIONS if loc != user_location])
    else:
        merchant_location = user_location if random.random() > 0.3 else random.choice(LOCATIONS)

    # Timestamp dentro de una ventana
    delta = timedelta(seconds=random.randint(0, 86400 * 7))  # 1 semana
    ts = base_time + delta

    return {
        "transaction_id": transaction_id,
        "user_id": str(uuid.uuid4()),
        "merchant_id": f"M_{random.randint(1000, 9999)}",
        "amount": amount,
        "location": {
            "user": {"lat": user_location[0], "lon": user_location[1]},
            "merchant": {"lat": merchant_location[0], "lon": merchant_location[1]},
        },
        "timestamp": ts.isoformat(),
        "is_fraud": is_fraud,
    }


def main() -> None:
    # KDD - Fase 1 (Selección): Conexión a la fuente y definición del conjunto
    print(f"Conectando a MongoDB: {MONGO_URI} (timeout {CONNECT_TIMEOUT_MS}ms)...")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=CONNECT_TIMEOUT_MS,
        connectTimeoutMS=CONNECT_TIMEOUT_MS,
    )
    # Reintentos: el contenedor tarda unos segundos en aceptar conexiones
    max_intentos = int(os.getenv("MONGO_RETRY_ATTEMPTS", "6"))
    espera_segundos = int(os.getenv("MONGO_RETRY_WAIT", "5"))
    ultimo_error = None
    for intento in range(1, max_intentos + 1):
        try:
            client.admin.command("ping")
            ultimo_error = None
            break
        except Exception as e:
            ultimo_error = e
            if intento < max_intentos:
                print(f"  Intento {intento}/{max_intentos} fallido, reintento en {espera_segundos}s...")
                time.sleep(espera_segundos)
            else:
                host_actual = socket.gethostname()
                print(
                    f"Error: No se pudo conectar a MongoDB (estás en el host: {host_actual}).\n"
                    "  - Si acabas de levantar el contenedor, MongoDB tarda ~10-20s en estar listo.\n"
                    "  - Vuelve a ejecutar el script o espera y usa: MONGO_RETRY_ATTEMPTS=10 MONGO_RETRY_WAIT=5\n"
                    "  - Comprueba: docker ps | grep mongo ; docker logs fraud_mongodb"
                )
                raise SystemExit(1) from ultimo_error

    print("Conectado correctamente a MongoDB.")

    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    base_time = datetime.now(timezone.utc) - timedelta(days=7)
    num_fraud = int(NUM_TRANSACTIONS * FRAUD_RATIO)
    num_normal = NUM_TRANSACTIONS - num_fraud

    print(f"Generando {NUM_TRANSACTIONS} transacciones ({num_fraud} fraude, {num_normal} normales)...")

    docs: List[Dict[str, Any]] = []
    for i in range(NUM_TRANSACTIONS):
        is_fraud = i < num_fraud
        tx_id = str(uuid.uuid4())
        docs.append(generate_transaction(tx_id, is_fraud, base_time))

    # Mezclar para no tener todos los fraudes al inicio
    random.shuffle(docs)

    try:
        coll.insert_many(docs)
        print(f"Insertadas {len(docs)} transacciones en {DB_NAME}.{COLLECTION_NAME}")
    except (ServerSelectionTimeoutError, ConnectionFailure) as e:
        print(
            "Error: La conexión falló durante la escritura (insert_many).\n"
            "  El ping inicial funcionó pero MongoDB no respondió al insertar.\n"
            "  Prueba con más tiempo: MONGO_CONNECT_TIMEOUT_MS=60000"
        )
        raise SystemExit(1) from e

    # Índice para consultas por tiempo (útil para ingesta incremental)
    try:
        coll.create_index("timestamp")
        coll.create_index("transaction_id", unique=True)
        print("Índices creados (timestamp, transaction_id).")
    except Exception as e:
        # Si la colección ya tenía índices, no es crítico
        print(f"Aviso al crear índices: {e}")

    client.close()
    print("Listo.")


if __name__ == "__main__":
    main()
