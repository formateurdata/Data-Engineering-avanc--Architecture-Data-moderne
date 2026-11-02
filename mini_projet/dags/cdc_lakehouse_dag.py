"""Le pipeline CDC en micro-batch : toutes les 2 minutes, de Kafka vers Bronze, puis de Bronze vers Silver.

La capture reste continue : Debezium publie dans Kafka sans attendre ce DAG. La première tâche vérifie qu'elle tourne,
sinon l'exécution s'arrête là. Ce que chaque script affiche se lit dans l'onglet Logs de sa tâche.

max_active_runs=1 protège les exécutions de ce DAG entre elles, pas d'une ingestion lancée à la main : pendant la démo,
n'en lancez pas en même temps que lui.
"""

from datetime import timedelta

import pendulum
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import DAG, task


@task(execution_timeout=timedelta(minutes=1))
def check_debezium_status():
    hook = HttpHook(method="GET", http_conn_id="debezium_connect")
    statut = hook.run("/connectors/pg-source-connector/status", extra_options={"timeout": 10}).json()
    connecteur = statut["connector"]["state"]
    taches = [tache["state"] for tache in statut["tasks"]]
    print(f"Connecteur : {connecteur}, tâches : {taches}")
    # Une liste de tâches vide échoue aussi : il faut au moins une tâche, et toutes à RUNNING
    if connecteur != "RUNNING" or set(taches) != {"RUNNING"}:
        raise RuntimeError("Le connecteur et au moins une tâche doivent être RUNNING, et aucune tâche dans un autre état")


@task.bash(execution_timeout=timedelta(minutes=5))
def reconcile_bronze_to_silver():
    return "uv run /opt/airflow/scripts/reconcile_silver.py"


with DAG(
    dag_id="cdc_postgres_to_iceberg_batch",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    doc_md=__doc__,
):
    ingest_kafka_to_bronze = ConsumeFromTopicOperator(
        task_id="ingest_kafka_to_bronze",
        kafka_config_id="kafka_cdc",
        topics=["pg_src.public.customers"],
        apply_function_batch="ingest_bronze.ecrire_lot",
        # L'offset Kafka n'est confirmé qu'après l'append Iceberg du lot.
        commit_cadence="end_of_batch",
        max_messages=500,
        max_batch_size=500,
        poll_timeout=10,
        execution_timeout=timedelta(minutes=10),
    )

    check_debezium_status() >> ingest_kafka_to_bronze >> reconcile_bronze_to_silver()
