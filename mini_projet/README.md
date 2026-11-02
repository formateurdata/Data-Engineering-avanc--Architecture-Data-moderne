# Mini-projet — Construire un pipeline CDC vers un lakehouse

Ce mini-projet est une **mission de data engineering** que nous allons construire et expliquer ensemble en classe. Il sert de support évolutif pendant le module : la première version est développée dans cette séance, puis nous pourrons l'améliorer, la tester et la faire évoluer lors des séances suivantes.

## 1. La mission : répondre à un besoin concret

Une enseigne fictive, **Boutique**, possède une application transactionnelle qui gère ses clients dans PostgreSQL. Les équipes métier souhaitent disposer d'une vue analytique fiable des clients : ville, email et nombre de points de fidélité.

Le besoin paraît simple, mais il cache plusieurs contraintes :

- les clients sont créés, modifiés et supprimés tout au long de la journée ;
- la base transactionnelle ne doit pas être surchargée par des extractions analytiques répétées ;
- les changements doivent pouvoir être rejoués en cas de panne ;
- l'équipe doit garder un historique technique des événements reçus ;
- la table analytique finale doit refléter l'état courant de PostgreSQL ;
- nous voulons détecter rapidement une divergence entre la source et la cible.

Notre mission est donc de construire une première chaîne locale, reproductible et observable qui répond à ce besoin.

### Résultat attendu

Après une insertion, une mise à jour ou une suppression dans PostgreSQL :

1. le changement est capturé sans relire toute la base ;
2. une trace technique rejouable est conservée ;
3. une vue analytique de l'état courant est mise à jour ;
4. un contrôle permet de comparer cette vue à la source.

## 2. Avant de coder : quelles solutions pourrait-on choisir ?

L'objectif n'est pas de retenir des outils par habitude. Pour chaque besoin, nous comparons des options puis justifions notre choix pour ce prototype pédagogique.

| Besoin | Options envisagées | Choix retenu et justification |
| --- | --- | --- |
| Récupérer les changements source | Export complet quotidien, colonne `updated_at`, triggers applicatifs, CDC sur le journal PostgreSQL | **Debezium + CDC** : capture les changements validés dans le WAL sans ajouter de code à l'application ni relire toutes les lignes. |
| Transporter les événements | Appel direct vers le lakehouse, fichiers intermédiaires, RabbitMQ, Kafka | **Kafka** : découple producteurs et consommateurs, conserve les messages, permet le rejeu et expose une position de lecture (offset). |
| Observer les messages | Scripts CLI Kafka, logs des conteneurs, interface web | **Kafbat** : rend le topic, les offsets et le connecteur visibles pendant le cours. |
| Stocker les événements analytiques | CSV, Parquet seul, Delta Lake, Hudi, Iceberg | **Iceberg** : tables versionnées avec catalogue, schéma explicite et stockage objet ; bon support pour distinguer Bronze et Silver. |
| Stockage objet local | Dossier local, MinIO, SeaweedFS, Garage, service cloud | **Garage avec API S3** : léger, auto-hébergeable, compatible avec Iceberg et introduit les notions de stockage objet distribué. |
| Consulter le stockage objet | Commandes S3, scripts, interface web | **S3 Manager** : rend les fichiers Parquet et les métadonnées Iceberg visibles pendant le cours, à travers l'API S3 de Garage et avec une clé en lecture seule. |
| Orchestrer les traitements | Lancement manuel, cron, script monolithique, Airflow | **Airflow** : rend les dépendances, les logs, les relances et les états des tâches visibles. |
| Vérifier la qualité | Confiance visuelle, quelques requêtes SQL, comparaison automatisée | **Script de convergence** : compare PostgreSQL et Silver après une période d'attente contrôlée. |

Ces choix sont adaptés à une démonstration locale. En entreprise, le même raisonnement s'applique, mais les réponses dépendent du volume, des coûts, de la sécurité, de la gouvernance et des compétences disponibles.

### Architecture retenue

Après cette comparaison, nous retenons l'architecture suivante :

```text
PostgreSQL ──> Debezium ──> Kafka ──> Bronze ──> Silver
     ↑               │             │          tables Iceberg sur Garage
générateur      capture CDC     Kafbat                │
                                              Airflow + contrôle qualité
```

Les zones Bronze et Silver sont des tables **Iceberg**. Iceberg définit les tables, leurs schémas, snapshots et métadonnées ; **Garage** conserve physiquement les fichiers de données et de métadonnées grâce à son API S3. Le catalogue Iceberg REST relie les deux.

## 3. Ce que nous allons construire dans cette séance

Cette séance couvre une première version complète du pipeline : **ingestion, Bronze, Silver et qualité**.

L'infrastructure est déjà fournie : PostgreSQL, Debezium, Kafka, Kafbat, Garage, S3 Manager, le catalogue Iceberg et Airflow sont prêts à démarrer. Nous ne codons ni Docker Compose ni la configuration des services pendant cette séance ; nous les utilisons comme un socle pour nous concentrer sur le **code Python**.

Les fichiers à développer ou expliquer sont :

- `scripts/ingest_bronze.py` ;
- `scripts/reconcile_silver.py` ;
- `scripts/verify.py` ;
- `dags/cdc_lakehouse_dag.py`.

### Étape A — Prendre en main l'infrastructure fournie

Nous présentons brièvement le rôle des services déjà fournis :

- PostgreSQL : source transactionnelle ;
- Debezium Connect : capture les changements PostgreSQL ;
- Kafka : journal d'événements ;
- Kafbat : interface d'observation de Kafka et du connecteur ;
- Garage : stockage objet compatible S3 ;
- S3 Manager : interface de consultation des fichiers stockés dans Garage ;
- Iceberg REST : catalogue des tables Iceberg ;
- Airflow : orchestration du traitement.

Le fichier [`infra/docker-compose.yml`](infra/docker-compose.yml) est fourni : il sert à comprendre l'environnement, pas à être codé dans cette séance.

```bash
make up
```

Interfaces à ouvrir :

- Kafbat : <http://localhost:8090>
- Airflow : <http://localhost:8080>
- Debezium Connect : <http://localhost:8083>
- S3 Manager : <http://localhost:8334>

### Étape B — Observer les changements : PostgreSQL vers Kafka

Nous utilisons la table source et le connecteur Debezium déjà configurés. Le générateur Python fourni permet de produire quelques changements :

Nous produisons quelques changements :

```bash
uv run scripts/generator.py --batches 2 --batch-size 5 --interval 0.5
```

Dans Kafbat, nous lisons le topic `pg_src.public.customers` et repérons :

- `op` : création, mise à jour, suppression ou événement de snapshot ;
- `before` et `after` ;
- la partition et l'offset Kafka ;
- les métadonnées de provenance PostgreSQL.

### Étape C — Écrire la zone Bronze

Nous codons ou commentons [`scripts/ingest_bronze.py`](scripts/ingest_bronze.py).

Bronze conserve les événements tels qu'ils sont reçus, avec leurs informations techniques : topic, partition, offset, type d'opération et contenu Debezium. C'est notre trace rejouable.

Point clé : l'ingestion est *at-least-once*. Si une panne intervient après l'écriture dans la table Bronze mais avant la validation de l'offset Kafka, un événement peut être écrit deux fois. Nous ne cachons pas ce risque : nous le traitons dans Silver.

### Étape D — Construire la zone Silver

Nous codons ou commentons [`scripts/reconcile_silver.py`](scripts/reconcile_silver.py).

Silver n'est plus un journal d'événements : c'est l'état actuel des clients. Le traitement :

1. déduplique Bronze par `(topic, partition, offset)` ;
2. conserve le dernier événement de chaque client ;
3. retire les clients supprimés ;
4. écrit `silver.customers_current` : une table Iceberg dont les fichiers sont stockés dans Garage.

```bash
make reconcile
```

Dans S3 Manager, nous ouvrons le bucket `warehouse`, puis `silver/customers_current/` : `data/` contient les fichiers Parquet et `metadata/` les métadonnées Iceberg. L'interface les télécharge, le navigateur ne les affiche pas directement.

### Étape E — Orchestrer et contrôler la qualité

Le DAG [`dags/cdc_lakehouse_dag.py`](dags/cdc_lakehouse_dag.py) enchaîne :

1. le contrôle de l'état du connecteur Debezium ;
2. l'ingestion Kafka vers Bronze ;
3. la réconciliation Bronze vers Silver.

Dans Airflow, nous suivons le DAG `cdc_postgres_to_iceberg_batch`, programmé toutes les deux minutes, ou déclenché manuellement.

Enfin, nous vérifions que la table Silver converge vers PostgreSQL :

```bash
make verify
```

Le contrôle compare les colonnes métier des deux systèmes et attend la convergence jusqu'à 3 minutes : le DAG ne passe que toutes les deux minutes, un écart affiché pendant cette attente est donc normal. Une différence doit être expliquée : retard normal du pipeline, problème de connecteur, erreur de transformation ou défaut de qualité source.

## 4. Critères de réussite de la séance

À la fin, chaque groupe doit être capable de démontrer que :

- une opération PostgreSQL apparaît dans Kafka ;
- les messages sont visibles et interprétables dans Kafbat ;
- Bronze conserve les événements avec leurs offsets ;
- Silver correspond à l'état courant après déduplication et suppressions ;
- `make verify` confirme la parité PostgreSQL ↔ Silver ;
- le groupe peut expliquer pourquoi un doublon Bronze ne doit pas créer de doublon dans Silver.

## 5. Commandes utiles

| Commande | Rôle |
| --- | --- |
| `make up` | Construit l'image Airflow et démarre l'infrastructure. |
| `make down` | Arrête les services en conservant les données. |
| `make logs` | Suit les logs de l'infrastructure. |
| `make generate` | Lance le générateur en continu ; arrêter avec `Ctrl+C`. |
| `make register-cdc` | Réapplique la configuration du connecteur Debezium. |
| `make reconcile` | Reconstruit Silver à partir de Bronze. |
| `make verify` | Compare PostgreSQL et Silver. |
| `make reset` | Supprime les données locales après confirmation. |

## 6. Après cette séance

Cette première version restera volontairement simple : une seule partition Kafka, une réconciliation qui relit Bronze, des identifiants locaux et pas de gestion avancée des schémas.

Les séances suivantes pourront partir de ce socle pour étudier, par exemple, le partitionnement, l'incrémentalité, l'évolution de schéma, les tests de panne, l'observabilité, la sécurité et le passage à l'échelle.

## Préparer son poste

- Docker Desktop avec Compose ;
- `uv` ;
- `make` ;
- au moins 8 Go disponibles pour Docker.

Sous Windows, le parcours recommandé est **WSL2** avec Docker Desktop. Dans le terminal WSL, les mêmes commandes `make` sont utilisables.

La documentation d'exploitation destinée à l'encadrant est dans [infra/INFRASTRUCTURE.md](infra/INFRASTRUCTURE.md).
