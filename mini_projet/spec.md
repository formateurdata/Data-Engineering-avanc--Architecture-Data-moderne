# Spécification technique — Démonstration de pipeline CDC hybride

## 1. Objectif et périmètre

Construire une démonstration pédagogique locale, reproductible et légère du pipeline suivant :

**PostgreSQL → Debezium → Kafka → Bronze Iceberg → Silver Iceberg**

La capture CDC est continue. L’ingestion dans Iceberg et la reconstruction de l’état courant sont réalisées en micro-batch, avec Python, PyArrow, PyIceberg et DuckDB, sous le contrôle d’Apache Airflow. Les scripts sont exécutés avec `uv` et des métadonnées PEP 723.

Aucun traitement Spark ni stockage Hadoop/HDFS n’est utilisé. L’expression « sans JVM » concerne uniquement les traitements Python : Kafka, Kafka Connect et le catalogue REST peuvent utiliser une JVM.

Cette stack est destinée à une démonstration locale, avec des identifiants de développement.

## 2. Livrables

```text
cdc-iceberg-demo/
├── spec.md
├── Makefile
├── infra/
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow
│   ├── INFRASTRUCTURE.md
│   ├── sql/init.sql
│   └── connectors/pg-source-connector.json
├── scripts/
│   ├── generator.py
│   ├── ingest_bronze.py
│   ├── reconcile_silver.py
│   └── verify.py
├── dags/cdc_lakehouse_dag.py
└── README.md
```

Tous les fichiers doivent être complets, sans placeholder. Les versions des images, de Python, de `uv` et des dépendances doivent être fixées et leur compatibilité validée lors de l’implémentation. Ne pas utiliser de tag `latest`. Livrer les verrous de dépendances générés par `uv` pour les scripts.

## 3. Infrastructure locale

Tous les services partagent le réseau Docker Compose. Utiliser des volumes persistants pour PostgreSQL, Kafka, SeaweedFS, les métadonnées Airflow et, si nécessaire, le catalogue REST.

| Service | Exigences | Adresse interne | Accès depuis l’hôte |
|---|---|---|---|
| `postgres-source` | PostgreSQL 16, `wal_level=logical`, slots et WAL senders suffisants | `postgres-source:5432` | `localhost:15432` |
| `kafka` | Mode KRaft, un broker, un contrôleur local | `kafka:9092` | `127.0.0.1:29092` |
| `debezium-connect` | Connecteur PostgreSQL Debezium | `debezium-connect:8083` | `localhost:8083` |
| `seaweedfs` | Stockage objet S3 | `http://seaweedfs:8333` | API `localhost:8333`, interface web `localhost:18888/buckets/warehouse/` |
| `seaweedfs-init` | Initialisation éphémère et idempotente du bucket | — | — |
| `iceberg-rest` | Catalogue REST compatible avec PyIceberg et SeaweedFS | `http://iceberg-rest:8181` | `http://localhost:8181` |
| `airflow` | Mode standalone, image personnalisée contenant `uv` et `curl` | `airflow:8080` | `localhost:8080` |

Configurer deux listeners Kafka avec des adresses annoncées adaptées : un pour les conteneurs et un pour l’hôte. Les topics de configuration, offsets et statuts de Connect ont un facteur de réplication de 1. Le topic `pg_src.public.customers` comporte une seule partition pour simplifier l’ordre des événements de cette démonstration.

L’image du catalogue REST doit être choisie et testée : ne pas supposer que les images candidates ont les mêmes variables de configuration. Le catalogue doit conserver ses métadonnées après redémarrage.

### Configuration S3 commune

```text
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
warehouse=s3://warehouse/
```

Configurer explicitement l’endpoint S3 et l’accès par chemin pour SeaweedFS dans chaque client concerné. Les conteneurs utilisent `http://seaweedfs:8333` ; les scripts lancés sur l’hôte utilisent `http://localhost:8333`. Appliquer la même distinction au catalogue REST. Si le catalogue fournit une configuration S3 au client, vérifier qu’elle ne remplace pas l’endpoint hôte par un nom Docker inaccessible.

Le service `seaweedfs-init` attend SeaweedFS, crée le bucket `warehouse` s’il n’existe pas et sort avec un code nul. Le catalogue démarre après la réussite de cette initialisation. Utiliser des contrôles de disponibilité et des tentatives bornées, sans temporisation fixe comme seul mécanisme de synchronisation.

## 4. Source PostgreSQL et connecteur CDC

### Table source

```sql
CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100),
    loyalty_points INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.customers REPLICA IDENTITY FULL;
```

Définir des identifiants locaux cohérents dans Compose, le connecteur et les scripts. Préparer les droits de réplication, la publication et le slot nécessaires à `pgoutput`. Documenter leur cycle de vie et leur remise à zéro.

### Configuration du connecteur

- Nom : `pg-source-connector`.
- Classe : `io.debezium.connector.postgresql.PostgresConnector`.
- Plugin : `pgoutput`.
- Table incluse : `public.customers`.
- Préfixe : `pg_src` ; topic : `pg_src.public.customers`.
- Snapshot initial activé ; traiter les événements `r` comme des créations d’état.
- Convertisseurs de clé et de valeur JSON, avec `schemas.enable=false` pour chacun.
- `tombstones.on.delete=false`.
- Noms explicites et stables pour le slot et la publication.

Conserver l’enveloppe Debezium `before` / `after` / `source` / `op`. Aucun transformateur ne doit supprimer les informations nécessaires aux suppressions ou à l’inspection pédagogique.

## 5. Générateur de trafic

`scripts/generator.py` utilise PEP 723 avec `psycopg[binary]`, `faker` et `rich`.

- Arguments : `--batch-size`, `--interval` et une option permettant un nombre fini de salves ; exécution continue par défaut.
- Répartition probabiliste : 60 % INSERT, 30 % UPDATE et 10 % DELETE.
- UPDATE et DELETE ciblent un identifiant existant. Si la table est vide, effectuer un INSERT.
- UPDATE modifie `city` ou `loyalty_points` et actualise explicitement `updated_at` : une valeur DEFAULT ne le fait pas automatiquement.
- Valider les transactions et afficher les opérations réussies avec `rich`.
- Gérer l’arrêt propre et les erreurs sans masquer une transaction échouée.

Le service `data-generator` est activable avec un profil Compose dédié. `make generate` lance le même script localement via `uv`, avec les adresses hôte. Ne pas démarrer deux générateurs implicitement.

## 6. Ingestion Kafka vers Bronze

`scripts/ingest_bronze.py` utilise PEP 723 avec `confluent-kafka`, `pyarrow`, les extras PyIceberg nécessaires au catalogue REST et à S3, et éventuellement `rich`. Ne pas ajouter de dépendance à un catalogue SQL PostgreSQL si elle n’est pas utilisée.

### Consommation bornée

- Groupe : `airflow-iceberg-bronze-ingestor`.
- `enable.auto.commit=false`, `enable.auto.offset.store=false`, `auto.offset.reset=earliest`.
- Après attribution des partitions, capturer les offsets de fin exclusifs de chaque partition. Une attente d’attribution expirée doit échouer explicitement.
- Consommer par lots de 500 jusqu’à atteindre ces bornes. Les événements arrivés après les bornes attendent l’exécution suivante.
- Une réponse vide après un timeout n’est pas une preuve de fin de partition. Réessayer dans une limite bornée puis échouer si les bornes ne sont pas atteintes.
- Borner la mémoire en écrivant des lots successifs. Configurer le délai maximal entre les appels au consommateur pour tenir compte de l’écriture Iceberg.
- Si toutes les partitions sont déjà à leur borne, terminer avec succès sans écrire de snapshot vide.

### Schéma Bronze

Table : `bronze.customers_cdc`.

| Colonne | Type logique | Origine |
|---|---|---|
| `kafka_topic` | string | Topic Kafka |
| `kafka_partition` | int32 | Partition Kafka |
| `kafka_offset` | int64 | Offset Kafka |
| `op` | string | `c`, `u`, `d`, `r` |
| `ts_ms` | int64 nullable | Timestamp d’enveloppe Debezium |
| `source_lsn` | int64 nullable | `source.lsn`, à titre d’inspection |
| `customer_id` | int64 | `after.id`, ou `before.id` pour DELETE |
| `before_payload` | string nullable | Objet `before` sérialisé en JSON |
| `after_payload` | string nullable | Objet `after` sérialisé en JSON |
| `full_name` | string nullable | `after.full_name` |
| `email` | string nullable | `after.email` |
| `city` | string nullable | `after.city` |
| `loyalty_points` | int32 nullable | `after.loyalty_points` |
| `ingested_at` | timestamp UTC | Horodatage d’ingestion |

Pour DELETE, les colonnes métier issues de `after` sont nulles. Définir explicitement le schéma Arrow et Iceberg, y compris pour un lot contenant uniquement des suppressions.

Créer le namespace et la table si nécessaire. Pour chaque lot :

1. Décoder et valider tous les événements concernés.
2. Construire une table Arrow conforme au schéma.
3. Appeler `table.append(arrow_table)`.
4. Après confirmation de l’écriture, committer explicitement et synchronement, pour chaque partition, le dernier offset écrit plus un.

Ne pas committer la position globale du consommateur si elle peut inclure des messages non écrits. Toute erreur de décodage, d’écriture ou de commit fait échouer la tâche ; ne pas ignorer silencieusement un message invalide. Fermer le consommateur dans un bloc de nettoyage.

### Garantie de livraison

Bronze est **at-least-once** : un échec entre l’append Iceberg et le commit Kafka peut produire des doublons. Kafka et Iceberg ne partagent pas de transaction atomique dans cette architecture. La clé `(kafka_topic, kafka_partition, kafka_offset)` permet de reconnaître ces doublons lors de la réconciliation.

## 7. Réconciliation Bronze vers Silver

`scripts/reconcile_silver.py` utilise PEP 723 avec `duckdb`, `pyarrow`, les extras PyIceberg nécessaires et `rich`.

Lire un snapshot stable de `bronze.customers_cdc` via `table.scan().to_arrow()`, puis enregistrer la table Arrow dans DuckDB. La lecture complète en mémoire est acceptable pour cette démonstration ; documenter cette limite.

La reconstruction suit cet ordre :

1. Dédupliquer par `(kafka_topic, kafka_partition, kafka_offset)`.
2. Pour chaque `customer_id`, conserver l’événement dont l’offset est le plus élevé.
3. Exclure les clients dont le dernier événement est un DELETE.

Ne pas utiliser `ts_ms` seul pour ordonner les événements : plusieurs changements peuvent partager le même timestamp. L’ordre par offset suppose ici un topic à une seule partition ; toute évolution du partitionnement impose de revoir cette règle.

```sql
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
ORDER BY id;
```

Écrire le résultat par overwrite atomique dans `silver.customers_current`, avec un schéma explicite, y compris lorsque le résultat est vide. Une nouvelle exécution sur les mêmes données doit produire le même état métier.

Journaliser le nombre d’événements Bronze, le nombre d’événements uniques et le nombre de clients actifs Silver. Si Bronze n’existe pas encore, terminer proprement en indiquant qu’aucune ingestion n’a eu lieu.

## 8. Orchestration Airflow

- DAG : `cdc_postgres_to_iceberg_batch`.
- Planification : `*/2 * * * *`, avec déclenchement manuel possible.
- `catchup=False`, `max_active_runs=1`.
- Retries bornés, timeouts explicites et logs consultables.
- Scripts et DAG montés dans `/opt/airflow/scripts` et `/opt/airflow/dags`.
- Cache `uv` persistant et accessible à l’utilisateur Airflow.

Chaîne de tâches :

1. `check_debezium_status` : `@task`, interroger `/connectors/pg-source-connector/status` avec `requests`, vérifier le code HTTP puis analyser le JSON. Le connecteur et toutes ses tâches doivent être `RUNNING` ; au moins une tâche doit exister.
2. `ingest_kafka_to_bronze` : `@task.bash`, `uv run /opt/airflow/scripts/ingest_bronze.py`.
3. `reconcile_bronze_to_silver` : `@task.bash`, `uv run /opt/airflow/scripts/reconcile_silver.py`.

`max_active_runs=1` protège les exécutions de ce DAG. Documenter qu’une ingestion manuelle concurrente est interdite pendant la démo.

## 9. Commandes Make

| Commande | Comportement |
|---|---|
| `make up` | Construire et démarrer la stack ; attendre sa disponibilité avec un délai maximal et signaler les services en échec |
| `make down` | Arrêter la stack en conservant les volumes |
| `make reset` | Supprimer les volumes uniquement après une confirmation explicite de destruction des données |
| `make register-cdc` | Créer le connecteur par POST s’il est absent ; mettre à jour sa configuration par PUT s’il existe ; vérifier son état |
| `make register-connector` | Alias de `register-cdc` |
| `make logs` | Suivre les logs des services |
| `make generate` | Lancer le générateur local avec `uv run` |
| `make verify` | Comparer PostgreSQL et Silver via `scripts/verify.py` |

## 10. Vérification de convergence

La réplication est asynchrone. Pour une comparaison probante :

1. Arrêter le générateur et toute autre écriture source.
2. Attendre que Debezium ait publié les changements, puis qu’une ingestion et une réconciliation les aient traités.
3. Exécuter `make verify`, avec répétitions bornées jusqu’à convergence ou expiration du délai.

`verify.py` compare les lignes triées et les valeurs de `id`, `full_name`, `email`, `city`, `loyalty_points`, ainsi que les comptes. `last_event_ts` est une métadonnée Silver ; `updated_at` reste accessible dans les payloads Bronze mais n’entre pas dans cette comparaison.

Afficher les deux comptes et les différences : lignes manquantes, supplémentaires ou modifiées. Retourner 0 uniquement si les données comparées correspondent, et un code non nul en cas de divergence persistante ou d’erreur technique. Une égalité des comptes seule ne suffit pas.

## 11. Critères d’acceptation

- Une stack vierge démarre selon les instructions du README et initialise automatiquement `warehouse`.
- Le connecteur atteint l’état `RUNNING` et capture le snapshot initial puis les INSERT, UPDATE et DELETE.
- Une ingestion sans nouveaux événements se termine avec succès.
- L’ingestion reste bornée malgré un générateur actif.
- Après arrêt des écritures et convergence, `make verify` confirme l’égalité ligne par ligne.
- Deux changements du même client partageant un `ts_ms` sont réconciliés selon leur offset.
- Une reprise après append réussi et commit Kafka échoué peut dupliquer Bronze, mais conserve le résultat Silver correct après réconciliation.
- La suppression du dernier client produit une table Silver vide.
- Un redémarrage conserve les données et les métadonnées nécessaires.
- Le README décrit les prérequis, les versions et ressources mesurées, les accès, le déroulé de démonstration, les garanties et limites, le dépannage et la remise à zéro.

## 12. Ordre d’implémentation

1. Infrastructure, initialisation SQL et catalogue, puis contrôles de disponibilité.
2. Configuration et enregistrement Debezium.
3. Générateur et scripts Bronze/Silver, avec dépendances verrouillées.
4. DAG Airflow et commandes Make.
5. Vérification des scénarios d’acceptation et rédaction du README.
