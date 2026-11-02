# Exploitation de l infrastructure locale

Cette infrastructure est destinée à une démonstration locale. Les services exposés sur l hôte sont limités à `127.0.0.1` et les identifiants fournis sont uniquement des identifiants de développement.

Interfaces locales : Airflow `http://localhost:8080`, Kafbat UI `http://localhost:8090`, SeaweedFS `http://localhost:18888` et Iceberg REST `http://localhost:8181`.

## Démarrage et configuration Debezium

```sh
docker compose -f infra/docker-compose.yml up --build -d
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml logs debezium-init
```

`debezium-init` applique `connectors/pg-source-connector.json` par `PUT` sur l API Kafka Connect. L opération est idempotente : elle crée le connecteur absent ou met à jour celui qui existe. Après une modification de ce fichier, rejouer explicitement son initialisation :

```sh
docker compose -f infra/docker-compose.yml up --force-recreate debezium-init
docker compose -f infra/docker-compose.yml logs debezium-init
```

Le conteneur `debezium-init` doit se terminer avec le code `0`. Airflow attend cette réussite avant de démarrer.

## Contrôles d infrastructure

```sh
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml exec postgres-source psql -U admin -d boutique -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retained FROM pg_replication_slots;"
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
docker compose -f infra/docker-compose.yml exec seaweedfs df -h /data
docker compose -f infra/docker-compose.yml exec iceberg-rest curl -fsS http://localhost:8181/v1/config
```

Surveiller en priorité :

- un slot PostgreSQL inactif ou un WAL retenu qui augmente continuellement ;
- les groupes Kafka en retard et l espace disponible pour les journaux Kafka ;
- l espace libre de SeaweedFS ;
- la disponibilité du catalogue Iceberg REST.

Pour cette démonstration locale, déclencher une investigation si le WAL retenu par un slot atteint `1 GiB`, si l espace Kafka ou SeaweedFS dépasse `80 %` d occupation, ou si le retard Kafka augmente sur trois contrôles successifs. Ces seuils servent à détecter une dérive avant saturation ; ils ne remplacent pas une métrologie de production.

Un slot inactif impose de vérifier en premier les journaux de Debezium et l état du connecteur. Ne jamais supprimer un slot de réplication à l aveugle : cela peut empêcher la reprise de la capture. En cas de saturation proche, arrêter les écritures de démonstration, libérer ou étendre l espace de stockage, puis relancer le service concerné et contrôler de nouveau le connecteur.

## Reprise

Pour un incident local, consulter d abord les journaux du service concerné, puis le redémarrer sans supprimer les volumes :

```sh
docker compose -f infra/docker-compose.yml restart <service>
docker compose -f infra/docker-compose.yml ps
```

Ne pas utiliser `docker compose down -v` pendant une reprise : cette commande détruit PostgreSQL, Kafka, SeaweedFS, le catalogue Iceberg et les métadonnées Airflow. La remise à zéro des volumes doit rester une action volontaire et distincte.

SQLite est conservé uniquement pour les métadonnées Airflow de cette démonstration. Airflow 3 est configuré explicitement avec `LocalExecutor`, qui utilise SQLite en mode WAL. Pour un déploiement multi-utilisateur ou plusieurs ordonnanceurs, remplacer cette base par PostgreSQL avant d augmenter la concurrence.
