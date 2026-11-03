# Exploitation de l infrastructure locale

Cette infrastructure est destinée à une démonstration locale. Les services exposés sur l hôte sont limités à `127.0.0.1` et les identifiants fournis sont uniquement des identifiants de développement.

Interfaces locales : Airflow `http://localhost:8080`, Kafbat UI `http://localhost:8090`, S3 Manager `http://localhost:8334`, endpoint S3 Garage `http://localhost:8333` et Iceberg REST `http://localhost:8181`.

## Démarrage et configuration Debezium

```sh
make up
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml logs debezium-init
```

Démarrer avec `make up` plutôt qu'avec `docker compose up` : le chemin du projet contient un `é`, que le terminal peut écrire sous deux formes Unicode (NFC ou NFD). Compose intègre ce chemin au hash des services qui montent des fichiers du projet, et recréerait PostgreSQL, Garage et Airflow à chaque changement d'écriture. Le `Makefile` retire `PWD` de l'environnement : Compose lit alors toujours la forme du disque. Une commande `docker compose up` lancée à la main reste exposée, y compris avec `--profile generator`. Les données ne sont pas perdues, mais ces services redémarrent.

Les images spécifiques au projet (`Airflow` et `garage-init`) sont publiées sur GitHub Container Registry. `make up` les télécharge automatiquement ; les étudiants n'ont donc pas besoin de construire des images Docker. Elles sont publiées pour `linux/amd64` (Docker Desktop avec WSL2/Windows) et `linux/arm64` (Mac Apple Silicon). La cible `make build-images` est réservée à la maintenance de ces images.

`debezium-init` applique `connectors/pg-source-connector.json` par `PUT` sur l API Kafka Connect. L opération est idempotente : elle crée le connecteur absent ou met à jour celui qui existe. Après une modification de ce fichier, rejouer explicitement son initialisation :

```sh
make register-cdc
docker compose -f infra/docker-compose.yml logs debezium-init
```

Le conteneur `debezium-init` doit se terminer avec le code `0`. Airflow attend cette réussite avant de démarrer.

## Contrôles d infrastructure

```sh
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml exec postgres-source psql -U admin -d boutique -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retained FROM pg_replication_slots;"
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
docker compose -f infra/docker-compose.yml exec garage /garage status
docker compose -f infra/docker-compose.yml exec iceberg-rest curl -fsS http://localhost:8181/v1/config
```

Surveiller en priorité :

- un slot PostgreSQL inactif ou un WAL retenu qui augmente continuellement ;
- les groupes Kafka en retard et l espace disponible pour les journaux Kafka ;
- la santé de Garage et l'espace disque Docker qui héberge son volume ;
- la disponibilité du catalogue Iceberg REST.

Pour cette démonstration locale, déclencher une investigation si le WAL retenu par un slot atteint `1 GiB`, si l espace Kafka ou le disque Docker qui porte Garage dépasse `80 %` d occupation, ou si le retard Kafka augmente sur trois contrôles successifs. Ces seuils servent à détecter une dérive avant saturation ; ils ne remplacent pas une métrologie de production.

Un slot inactif impose de vérifier en premier les journaux de Debezium et l état du connecteur. Ne jamais supprimer un slot de réplication à l aveugle : cela peut empêcher la reprise de la capture. En cas de saturation proche, arrêter les écritures de démonstration, libérer ou étendre l espace de stockage, puis relancer le service concerné et contrôler de nouveau le connecteur.

## Reprise

Pour un incident local, consulter d abord les journaux du service concerné, puis le redémarrer sans supprimer les volumes :

```sh
docker compose -f infra/docker-compose.yml restart <service>
docker compose -f infra/docker-compose.yml ps
```

Ne pas utiliser `docker compose down -v` pendant une reprise : cette commande détruit PostgreSQL, Kafka, Garage, le catalogue Iceberg et les métadonnées Airflow. La remise à zéro des volumes doit rester une action volontaire et distincte.

SQLite est conservé uniquement pour les métadonnées Airflow de cette démonstration. Airflow 3 est configuré explicitement avec `LocalExecutor`, qui utilise SQLite en mode WAL. Pour un déploiement multi-utilisateur ou plusieurs ordonnanceurs, remplacer cette base par PostgreSQL avant d augmenter la concurrence.

## Particularité Garage et Iceberg REST

Iceberg REST embarque le SDK AWS Java 2.33, qui calcule par défaut un CRC32 envoyé en trailer. Cette variante d'écriture échoue avec Garage v2 dans notre combinaison de versions. Le catalogue utilise donc le client S3 standard (`s3.crt.enabled=false`) et fixe `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` ; seules les sommes de contrôle imposées par l'API sont alors ajoutées. Les écritures restent signées avec la clé S3 locale de démonstration.

## Consulter le stockage avec S3 Manager

Garage n'expose qu'une API S3. S3 Manager (`http://localhost:8334`, port modifiable avec `S3_MANAGER_PORT`) en donne une vue web, avec sa propre clé en lecture seule (`garagelecture`). Le bucket `warehouse` contient `bronze/customers_cdc/` et `silver/customers_current/` ; dans chaque table, `data/` regroupe les fichiers Parquet et `metadata/` les métadonnées Iceberg. La liste tronque les noms longs : l'icône d'information affiche le nom complet.

Limites à connaître :

- S3 Manager n'a pas de mode lecture seule : ses boutons d'envoi de fichiers et de création de bucket restent affichés. Garage les refuse avec une erreur `Forbidden`, car la clé n'a que le droit de lecture : rien n'est écrit. `ALLOW_DELETE=false` retire en plus les boutons de suppression.
- Les fichiers Iceberg sont stockés en `application/octet-stream` : l'action d'ouverture les télécharge au lieu de les afficher dans le navigateur.
- Le lien temporaire pointe vers `http://garage:3900`, un nom que seuls les conteneurs résolvent : il ne s'ouvre pas depuis la machine. Utiliser le téléchargement.

Le healthcheck attend que `warehouse` apparaisse dans la liste des buckets. Un état `unhealthy` signale donc que Garage ne répond pas, qu'il refuse la clé ou que le bucket manque. Avec une clé refusée, la page répond quand même 200 et affiche `Unable to connect to S3 instance 'garage'`.

La clé est créée par `garage-init`, qui doit se terminer avec le code `0` : S3 Manager attend cette réussite. Ce service tourne à chaque démarrage et ne réimporte pas une clé existante, car Garage le refuserait. Il a sa propre image (`Dockerfile.garage-init`), parce que celle de Garage ne contient pas de shell. La clé `garageadmin` reste celle du pipeline, avec tous ses droits.
