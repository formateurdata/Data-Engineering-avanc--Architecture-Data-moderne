# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "psycopg[binary]==3.3.6",
#     "pyarrow==25.0.1",
#     "pyiceberg[pyarrow]==0.12.0",
#     "rich==15.0.0",
# ]
# ///
"""Compare l'état métier PostgreSQL et la table Iceberg Silver jusqu'à convergence ou expiration.

Usage : ``uv run scripts/verify.py --attempts 20 --interval 3``.
``last_event_ts`` est une métadonnée Silver et n'entre volontairement pas dans la comparaison.
"""

import argparse
import os
import sys
import time

import psycopg
from pyiceberg.catalog import load_catalog
from rich.console import Console

COLONNES = ("id", "full_name", "email", "city", "loyalty_points")


def arguments():
    parser = argparse.ArgumentParser(description="Vérifie la convergence entre PostgreSQL et Silver.")
    parser.add_argument("--attempts", type=int, default=20, help="nombre maximal de comparaisons (défaut : 20)")
    parser.add_argument("--interval", type=float, default=3.0, help="secondes entre deux comparaisons (défaut : 3)")
    args = parser.parse_args()
    if args.attempts < 1 or args.interval < 0:
        parser.error("--attempts doit être positif et --interval ne peut pas être négatif")
    return args


def source_postgres():
    connexion = {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "15432"),
        "dbname": os.environ.get("POSTGRES_DB", "boutique"),
        "user": os.environ.get("POSTGRES_USER", "admin"),
        "password": os.environ.get("POSTGRES_PASSWORD", "password"),
    }
    with psycopg.connect(**connexion) as db, db.cursor() as curseur:
        curseur.execute("SELECT id, full_name, email, city, loyalty_points FROM public.customers ORDER BY id")
        return [dict(zip(COLONNES, ligne, strict=True)) for ligne in curseur.fetchall()]


def cible_silver():
    catalogue = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )
    if not catalogue.table_exists("silver.customers_current"):
        return None
    return [{colonne: ligne[colonne] for colonne in COLONNES} for ligne in catalogue.load_table("silver.customers_current").scan().to_arrow().to_pylist()]


def lignes(jeu):
    return {tuple(ligne[colonne] for colonne in COLONNES) for ligne in jeu}


def afficher_differences(console, source, silver):
    manquantes = lignes(source) - lignes(silver)
    supplementaires = lignes(silver) - lignes(source)
    console.print(f"PostgreSQL : {len(source)} ligne(s) ; Silver : {len(silver)} ligne(s)", soft_wrap=True)
    for etiquette, valeurs in (("manquante(s) dans Silver", manquantes), ("supplémentaire(s) dans Silver", supplementaires)):
        if valeurs:
            console.print(f"{len(valeurs)} ligne(s) {etiquette} :", soft_wrap=True)
            for valeur in sorted(valeurs)[:10]:
                console.print(dict(zip(COLONNES, valeur, strict=True)), soft_wrap=True)


def main():
    args = arguments()
    console = Console(highlight=False)
    for tentative in range(1, args.attempts + 1):
        source = source_postgres()
        silver = cible_silver()
        if silver is not None and lignes(source) == lignes(silver):
            console.print(f"Convergence confirmée à la tentative {tentative} : {len(source)} ligne(s).", soft_wrap=True)
            return 0
        console.print(f"Pas encore convergé (tentative {tentative}/{args.attempts}).", soft_wrap=True)
        afficher_differences(console, source, silver or [])
        if tentative < args.attempts:
            time.sleep(args.interval)

    console.print("Échec de convergence avant expiration du délai.", soft_wrap=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
