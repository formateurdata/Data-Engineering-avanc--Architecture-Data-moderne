# Démo séance 1 : un besoin, deux architectures

Les données : chaque transaction BTC/USDT exécutée sur Binance du lundi 5 au dimanche 11 octobre 2026.
Le besoin : pour chaque minute de la semaine, le nombre de transactions, le volume échangé en BTC et
le montant échangé en USDT, exacts à la huitième décimale. La même requête SQL tourne sur deux
architectures :

- **A, entrepôt relationnel** : PostgreSQL 18 dans Docker. Schéma à l'écriture : la table
  `transactions` a des types fixés (décimaux exacts, horodatage) ; chaque journée est copiée depuis
  son zip, sans extraction sur le disque, puis convertie.
- **B, lac de fichiers** : chaque journée est convertie en Parquet par DuckDB et déposée dans
  `lac/transactions/jour=AAAA-MM-JJ/`. Aucune table : DuckDB lit les fichiers à la requête.
  Schéma à la lecture.

**Critère de réussite** : les deux tableaux sont identiques, minute par minute, à la huitième
décimale. Chaque architecture est ensuite confrontée à deux exigences fixées avant la première
mesure, dans `besoin.py` : une journée chargée en moins de 60 s, et le tableau de la semaine produit
en moins de 2 s.

## Données

Binance publie gratuitement ses données de marché, sans compte : <https://data.binance.vision>.
Le format des fichiers `trades` est décrit dans <https://github.com/binance/binance-public-data> :
`trade Id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch`, sans en-tête ; depuis le
1er janvier 2025, `time` est en microsecondes (heure UTC).

- `data/BTCUSDT-trades-2026-10-05.zip` à `...-2026-10-11.zip` : 7 fichiers, 141 Mo,
  18 140 779 transactions. Non versionnés : `python run.py telecharger`, qui vérifie aussi
  l'empreinte SHA-256 publiée à côté de chaque fichier.

Binance précise que ses archives peuvent être mises à jour après coup, quand un problème est
découvert : relancer `telecharger` récupère la version publiée du moment.

## Fichiers

- `besoin.py` : la semaine, les exigences chiffrées et la requête commune.
- `entrepot.py` : architecture A (table de transit, `COPY`, conversion, `ANALYZE`, requête).
- `lac.py` : architecture B (extraction temporaire, conversion en Parquet, requête DuckDB).
- `mesure.py` : chronométrage (médiane de 5 exécutions de la requête) et écriture des résultats.
- `run.py` : point d'entrée.
- `docker-compose.yml` : le conteneur PostgreSQL.

## Lancer la démo

Testé avec Python 3.13 et 3.15, Docker 29, PostgreSQL 18.0, DuckDB 1.5.6, pyarrow 26.0.0 et
psycopg 3.3.6, sur Mac (Apple Silicon). Prévoir environ 2 Go d'espace disque libre (zips, table
PostgreSQL, fichiers Parquet).

```bash
cd seance1/demo_architectures
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d          # architecture A : PostgreSQL sur le port 5442
python run.py telecharger     # 141 Mo depuis Binance, à faire avant le cours
python run.py tout            # controler, entrepot, lac, comparer
docker compose down           # à la fin
```

Les étapes peuvent aussi se lancer une par une : `controler`, `entrepot`, `lac`, `comparer`.

## Ce qu'on regarde

`python run.py controler` vérifie chaque journée et arrête la démo si les identifiants de
transaction ne se suivent pas, si une transaction sort de sa journée, ou si un prix ou une quantité
est nul :

```
2026-10-05 : 2992211 transactions, identifiants continus, prix de 84972.01000000 à 86999.11000000 USDT
...
2026-10-11 : 996255 transactions, identifiants continus, prix de 82804.65000000 à 84199.00000000 USDT
Semaine : 18140779 transactions, prix de 80393.56000000 à 86999.11000000 USDT
```

`python run.py comparer` affiche le critère de réussite, puis les mesures. Résultat obtenu lors de
la préparation (quatre exécutions de l'entrepôt, deux du lac, chiffres arrondis) :

| Mesure | A entrepôt | B lac | Exigence |
|---|---|---|---|
| Chargement, pire journée (mercredi 7) | ≈ 8 s | ≈ 1 s | < 60 s |
| Chargement, semaine | ≈ 31 s | ≈ 4 s | - |
| Stockage de la semaine | 1 418,1 Mo | 213,9 Mo | - |
| Requête, médiane de 5 | 1,4 à 1,7 s | ≈ 0,12 s | < 2 s |

Tableaux identiques : 10 080 minutes, 18 140 779 transactions, 98 019,14258 BTC et
8 212 617 613,2983576 USDT échangés ; prix moyen pondéré de la semaine : 83 785,85 USDT ; minute la
plus active : le 7 octobre à 02:01 UTC, 34 731 transactions. Les deux architectures tiennent les
exigences ; la requête de l'entrepôt passe sous les 2 s avec peu de marge.

Les durées dépendent de la machine et de la mémoire allouée à Docker. Les tailles, le nombre de
lignes et les montants ne changent pas, tant que Binance ne republie pas un fichier.

Pour voir la table dans PostgreSQL : `\d+` affiche les colonnes et leurs types, `\dt+` la taille.

```bash
docker compose exec entrepot psql -U demo -d marche -c '\d+ transactions'
docker compose exec entrepot psql -U demo -d marche -c '\dt+ transactions'
```

`\dt+` affiche la taille en Mio (1 024 x 1 024 octets), la démo en millions d'octets : c'est la
même taille.

## Dépannage

`docker compose up -d` échoue avec `Bind for 0.0.0.0:5442 failed: port is already allocated` :
un autre conteneur ou un PostgreSQL local utilise déjà ce port. Pendant la préparation, le port
5433 était pris par un autre conteneur, d'où le choix de 5442. Changer le port dans
`docker-compose.yml` et dans `DSN` (`entrepot.py`), ou arrêter l'autre service
(`docker ps` pour le trouver).

`python run.py entrepot` échoue avec `connection refused` : le conteneur n'est pas démarré, ou
PostgreSQL démarre encore. Lancer `docker compose up -d`, attendre quelques secondes, relancer.

`python run.py comparer` signalait, dans une première version de la démo, des tableaux différents
alors que leurs contenus étaient les mêmes : PostgreSQL et DuckDB ne trient pas toujours les textes
de la même façon (collation). `comparer` trie désormais les deux tableaux avant de les comparer.

`python run.py telecharger` s'arrête sur `empreinte différente de celle publiée` : le fichier est
incomplet ou Binance l'a republié pendant le téléchargement. Relancer la commande.

`python run.py controler` échoue avec `No such file` : les fichiers ne sont pas téléchargés.
Lancer `python run.py telecharger`.
