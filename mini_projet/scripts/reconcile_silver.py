# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb==2.0.0",
#     "pyarrow==25.0.1",
#     "pyiceberg[pyarrow]==0.12.0",
#     "rich==15.0.0",
# ]
# ///
"""Reconstruit silver.customers_current à partir de Bronze.

Bronze est lu entièrement en mémoire : c'est acceptable pour cette démonstration, pas pour un historique volumineux.
"""

import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from rich.console import Console

CATALOGUE = {
    "type": "rest",
    "uri": os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "garageadmin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "garage-demo-secret-2026"),
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
}

SCHEMA_SILVER = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("full_name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("city", pa.string()),
        pa.field("loyalty_points", pa.int32()),
        pa.field("last_event_ts", pa.int64()),
    ]
)

EVENEMENTS_UNIQUES = "SELECT count(*) FROM (SELECT DISTINCT kafka_topic, kafka_partition, kafka_offset FROM bronze_arrow_table)"

ETAT_COURANT = """
WITH unique_events AS (
    SELECT * EXCLUDE (duplicate_rank)
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY kafka_topic, kafka_partition, kafka_offset
            ORDER BY ingested_at DESC
        ) AS duplicate_rank
        FROM bronze_arrow_table
    )
    WHERE duplicate_rank = 1
), latest_events AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY kafka_offset DESC
    ) AS event_rank
    FROM unique_events
)
SELECT customer_id AS id, full_name, email, city, loyalty_points,
       ts_ms AS last_event_ts
FROM latest_events
WHERE event_rank = 1 AND op <> 'd'
ORDER BY id
"""


def main():
    console = Console(highlight=False)
    catalogue = load_catalog("default", **CATALOGUE)
    if not catalogue.table_exists("bronze.customers_cdc"):
        console.print("Bronze n'existe pas encore : aucune ingestion n'a eu lieu.", soft_wrap=True)
        return

    bronze = catalogue.load_table("bronze.customers_cdc").scan().to_arrow()
    connexion = duckdb.connect()
    try:
        connexion.register("bronze_arrow_table", bronze)
        uniques = connexion.execute(EVENEMENTS_UNIQUES).fetchone()[0]
        clients = connexion.execute(ETAT_COURANT).to_arrow_table().cast(SCHEMA_SILVER)
    finally:
        connexion.close()

    catalogue.create_namespace_if_not_exists("silver")
    silver = catalogue.create_table_if_not_exists("silver.customers_current", schema=SCHEMA_SILVER)
    silver.overwrite(clients)
    console.print(f"Bronze : {bronze.num_rows} événements, dont {uniques} uniques", soft_wrap=True)
    console.print(f"Silver : {clients.num_rows} clients actifs", soft_wrap=True)


if __name__ == "__main__":
    main()
