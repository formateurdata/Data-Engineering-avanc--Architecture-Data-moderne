# Mini-projet — du CDC vers un lakehouse

Ce dossier est le point de départ de notre mini-projet de data engineering. Nous allons le construire ensemble, séance après séance, à partir d'un besoin concret. L'infrastructure est prête ; le code métier du pipeline est volontairement laissé à écrire en classe.

## La mission

L'enseigne fictive **Boutique** gère ses clients dans PostgreSQL. Les équipes métier souhaitent une vue analytique fiable des clients, avec leur ville, leur email et leurs points de fidélité.

Cette vue doit suivre les créations, mises à jour et suppressions sans surcharger la base transactionnelle. Les changements doivent être rejouables en cas de panne et nous devons pouvoir vérifier que la cible analytique finit bien par refléter la source.

Notre objectif est de construire progressivement la chaîne suivante :

```text
PostgreSQL -> Debezium -> Kafka -> Bronze -> Silver
     ^                         tables Iceberg sur Garage
générateur                         ^
                         Airflow + contrôle qualité
```

À ce stade, l'infrastructure nous permet déjà d'observer le parcours **PostgreSQL -> Debezium -> Kafka**. Nous coderons ensuite Bronze, Silver, le contrôle de qualité et le DAG Airflow.

## Les choix techniques fournis

| Besoin | Solution | Pourquoi ici ? |
| --- | --- | --- |
| Base source | PostgreSQL | Base transactionnelle de la démonstration. |
| Capture des changements | Debezium | Lit le journal PostgreSQL (WAL) sans réextraire toute la table. |
| Transport des événements | Kafka | Conserve les événements et permet leur rejeu. |
| Observation Kafka | Kafbat | Rend les topics, messages et offsets visibles pendant le cours. |
| Tables analytiques | Apache Iceberg | Gère les tables, schémas et snapshots. |
| Stockage objet | Garage | Fournit une API S3 locale pour les fichiers Iceberg. |
| Consultation des fichiers | S3 Manager | Permet de parcourir le bucket Garage en lecture seule. |
| Orchestration | Airflow | Exécutera et rendra visibles les étapes du pipeline. |

L'infrastructure est expliquée dans [`infra/INFRASTRUCTURE.md`](infra/INFRASTRUCTURE.md). Elle est fournie : nous ne la modifions pas pendant cette première séance.

## Démarrer le projet

Prérequis : Docker Desktop, `uv` et GNU Make.

Sous Windows, utilisez de préférence **WSL2** avec Docker Desktop : ouvrez ce dossier depuis un terminal Ubuntu/WSL, puis utilisez les mêmes commandes que ci-dessous.

```bash
make up
```

Au premier lancement, Docker construit l'image Airflow puis démarre les services. Ouvrez ensuite :

- Kafbat : <http://localhost:8090>
- Airflow : <http://localhost:8080>
- Debezium Connect : <http://localhost:8083>
- S3 Manager : <http://localhost:8334>

Pour arrêter la pile en conservant les données :

```bash
make down
```

Pour supprimer toutes les données locales et repartir de zéro :

```bash
make reset
```

## Première observation : le CDC jusqu'à Kafka

Le générateur est fourni. Il crée, modifie ou supprime des clients dans PostgreSQL.

```bash
uv run scripts/generator.py --batches 2 --batch-size 5 --interval 0.5
```

Dans Kafbat, ouvrez le topic `pg_src.public.customers`. Pour chaque message Debezium, observez :

- `op` : création, mise à jour ou suppression ;
- `before` et `after` ;
- la partition et l'offset Kafka ;
- les métadonnées provenant de PostgreSQL.

Le connecteur CDC est créé automatiquement au démarrage. En cas de besoin, vous pouvez le réappliquer :

```bash
make register-cdc
```

## Ce que nous coderons en classe

Les fichiers suivants sont intentionnellement vides. Ils seront construits progressivement pendant les séances :

| Fichier | Rôle à construire |
| --- | --- |
| `scripts/ingest_bronze.py` | Lire Kafka et écrire les événements dans Bronze. |
| `scripts/reconcile_silver.py` | Dédupliquer Bronze et construire l'état courant Silver. |
| `scripts/verify.py` | Comparer PostgreSQL et Silver pour vérifier la convergence. |
| `dags/cdc_lakehouse_dag.py` | Orchestrer les étapes avec Airflow. |

Quand les tables Iceberg existeront, S3 Manager permettra de télécharger les fichiers Parquet et les métadonnées depuis le bucket `warehouse`. Nous les lirons ensuite avec Pandas ; le navigateur ne prévisualise pas directement les fichiers Parquet.

## Commandes utiles

| Commande | Rôle |
| --- | --- |
| `make up` | Construit les images nécessaires et démarre l'infrastructure. |
| `make down` | Arrête les services en conservant les données. |
| `make logs` | Suit les logs des services. |
| `make generate` | Lance le générateur en continu ; arrêter avec `Ctrl+C`. |
| `make register-cdc` | Réapplique la configuration Debezium. |
| `make reset` | Supprime les volumes locaux après confirmation. |

## Arborescence

```text
mini-projet/
├── infra/                 # services Docker et configuration fournis
├── scripts/
│   ├── generator.py        # fourni : génère le trafic PostgreSQL
│   ├── ingest_bronze.py    # à coder
│   ├── reconcile_silver.py # à coder
│   └── verify.py           # à coder
├── dags/
│   └── cdc_lakehouse_dag.py # à coder
├── Makefile
└── README.md
```
