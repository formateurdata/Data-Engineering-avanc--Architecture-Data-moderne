"""Écrit dans bronze.customers_cdc les événements Debezium fournis par le provider Kafka Airflow, lot par lot.

Ce module est appelé par ``ConsumeFromTopicOperator`` dans le processus de la tâche Airflow. Une exception empêche le
commit Kafka du lot : il sera donc relu lors de la prochaine tentative, sans perte d'événement.
"""

import json
import os
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog

OPERATIONS = {"c", "u", "d", "r"}

SCHEMA_BRONZE = pa.schema(
    [
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("op", pa.string(), nullable=False),
        pa.field("ts_ms", pa.int64()),
        pa.field("source_lsn", pa.int64()),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("before_payload", pa.string()),
        pa.field("after_payload", pa.string()),
        pa.field("full_name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("city", pa.string()),
        pa.field("loyalty_points", pa.int32()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def vers_ligne(message, ingested_at):
    """Transforme un message Kafka en ligne Bronze, sans jamais ignorer un message invalide."""
    if message.error():
        raise RuntimeError(f"Erreur Kafka : {message.error()}")
    evenement = json.loads(message.value())
    if evenement["op"] not in OPERATIONS:
        raise ValueError(f"Opération inconnue à l'offset {message.offset()} : {evenement['op']}")

    avant, apres = evenement["before"], evenement["after"]
    client = apres or avant
    if client is None or "id" not in client:
        raise ValueError(f"Client absent à l'offset {message.offset()}")

    colonnes = apres or {}
    return {
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
        "op": evenement["op"],
        "ts_ms": evenement.get("ts_ms"),
        "source_lsn": evenement.get("source", {}).get("lsn"),
        "customer_id": client["id"],
        "before_payload": json.dumps(avant, ensure_ascii=False) if avant else None,
        "after_payload": json.dumps(apres, ensure_ascii=False) if apres else None,
        "full_name": colonnes.get("full_name"),
        "email": colonnes.get("email"),
        "city": colonnes.get("city"),
        "loyalty_points": colonnes.get("loyalty_points"),
        "ingested_at": ingested_at,
    }


def ecrire_lot(messages):
    """Écrit atomiquement un lot Bronze avant que l'opérateur ne confirme ses offsets Kafka."""
    if not messages:
        return

    ingested_at = datetime.now(UTC)
    lignes = [vers_ligne(message, ingested_at) for message in messages]
    catalogue = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ["AWS_REGION"],
        },
    )
    catalogue.create_namespace_if_not_exists("bronze")
    table = catalogue.create_table_if_not_exists("bronze.customers_cdc", schema=SCHEMA_BRONZE)
    table.append(pa.Table.from_pylist(lignes, schema=SCHEMA_BRONZE))
    print(f"Bronze : {len(lignes)} événements écrits, offsets {lignes[0]['kafka_offset']} à {lignes[-1]['kafka_offset']}")
