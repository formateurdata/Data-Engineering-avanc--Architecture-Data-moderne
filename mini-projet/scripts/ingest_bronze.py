# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "confluent-kafka==2.15.0",
#     "pyarrow==25.0.1",
#     "pyiceberg[pyarrow]==0.12.0",
# ]
# ///
"""Lit des événements Debezium dans Kafka et les écrit dans bronze.customers_cdc.

Usage : ``uv run scripts/ingest_bronze.py``.

Les offsets Kafka ne sont validés qu'après l'écriture Iceberg. Une panne peut donc
rejouer un lot et créer un doublon Bronze, mais elle ne peut pas perdre un événement.
Silver dédupliquera ensuite avec (topic, partition, offset).
"""

import json
import os
import time
from datetime import UTC, datetime
from typing import Any, TypeAlias

import pyarrow as pa
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from pyiceberg.catalog import load_catalog

TOPIC = "pg_src.public.customers"
OPERATIONS = {"c", "u", "d", "r"}
LigneBronze: TypeAlias = dict[str, Any]

SCHEMA_BRONZE = pa.schema([
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
])


def vers_ligne(message: Any, ingested_at: datetime) -> dict[str, Any]:
    """Transforme un message Kafka en ligne Bronze, sans jamais ignorer un message invalide."""
    if message.error():
        raise RuntimeError(f"Erreur Kafka : {message.error()}")
    evenement = json.loads(message.value())
    if evenement["op"] not in OPERATIONS:
        raise ValueError(
            f"Opération inconnue à l'offset {message.offset()} : {evenement['op']}"
        )

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


def ecrire_lot(messages: list[Any]) -> None:
    """Écrit atomiquement un lot Bronze avant que l'opérateur ne confirme ses offsets Kafka."""
    if not messages:
        return

    ingested_at = datetime.now(UTC)
    lignes = [vers_ligne(message, ingested_at) for message in messages]
    catalogue = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "garageadmin"),
            "s3.secret-access-key": os.environ.get(
                "AWS_SECRET_ACCESS_KEY", "garage-demo-secret-2026"
            ),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )
    catalogue.create_namespace_if_not_exists("bronze")
    table = catalogue.create_table_if_not_exists(
        "bronze.customers_cdc", schema=SCHEMA_BRONZE
    )
    table.append(pa.Table.from_pylist(lignes, schema=SCHEMA_BRONZE))
    print(
        f"Bronze : {len(lignes)} événements écrits, offsets {lignes[0]['kafka_offset']} à {lignes[-1]['kafka_offset']}"
    )


def consommer() -> None:
    """Lit jusqu'aux offsets de fin capturés au démarrage, par lots de 500.

    L'attribution explicite évite l'attente d'un rééquilibrage. Le groupe manuel
    conserve ses offsets, indépendamment de celui du DAG.
    """
    consumer = Consumer({
        "bootstrap.servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092"
        ),
        "group.id": "bronze-manuel",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "enable.partition.eof": True,
        "max.poll.interval.ms": 600000,
    })
    messages: list[Message] = []
    total = 0

    def ecrire_et_confirmer() -> None:
        nonlocal total
        if not messages:
            return
        ecrire_lot(messages)
        # Confirmer uniquement les messages effectivement écrits dans Iceberg.
        offsets = {}
        for message in messages:
            offsets[message.partition()] = message.offset() + 1
        consumer.commit(
            offsets=[
                TopicPartition(TOPIC, partition, offset)
                for partition, offset in offsets.items()
            ],
            asynchronous=False,
        )
        total += len(messages)
        messages.clear()

    try:
        metadata = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        if metadata.error is not None:
            raise KafkaException(metadata.error)
        partitions = [TopicPartition(TOPIC, numero) for numero in metadata.partitions]
        if not partitions:
            raise RuntimeError(f"Aucune partition disponible pour {TOPIC}")
        commits = consumer.committed(partitions, timeout=10)
        bornes = {}
        attributions = []
        for partition in commits:
            if partition.error is not None:
                raise KafkaException(partition.error)
            debut, fin = consumer.get_watermark_offsets(partition, timeout=10)
            offset = partition.offset if partition.offset >= 0 else debut
            if not debut <= offset <= fin:
                raise RuntimeError(
                    f"Offset validé {offset} hors de [{debut}, {fin}] "
                    f"pour la partition {partition.partition}"
                )
            if offset < fin:
                bornes[partition.partition] = fin
                attributions.append(TopicPartition(TOPIC, partition.partition, offset))
        consumer.assign(attributions)
        derniere_progression = time.monotonic()
        while bornes:
            message = consumer.poll(timeout=2.0)
            if message is not None:
                erreur = message.error()
                if erreur and erreur.code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(erreur)
                partition = message.partition()
                if partition in bornes:
                    fin = bornes[partition]
                    if erreur is None and message.offset() < fin:
                        messages.append(message)
                        derniere_progression = time.monotonic()
                    # EOF permet aussi de franchir des offsets sans message visible.
                    position = message.offset() if erreur else message.offset() + 1
                    if position >= fin:
                        del bornes[partition]
                        consumer.pause([TopicPartition(TOPIC, partition)])
                    if len(messages) >= 500:
                        ecrire_et_confirmer()
                        derniere_progression = time.monotonic()
            if bornes and time.monotonic() - derniere_progression >= 30:
                raise TimeoutError(
                    f"Lecture Kafka sans progression depuis 30 secondes ; bornes restantes : {bornes}"
                )
        ecrire_et_confirmer()
        print(f"Ingestion terminée : {total} événement(s) lu(s) depuis {TOPIC}.")
    finally:
        consumer.close()


def main() -> None:
    consommer()


if __name__ == "__main__":
    main()
