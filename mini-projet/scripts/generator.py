# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "faker==40.44.0",
#     "psycopg[binary]==3.3.6",
#     "rich==15.0.0",
# ]
# ///
"""Générateur de trafic sur public.customers : des INSERT, UPDATE et DELETE que Debezium va capturer.

Usage :
  uv run scripts/generator.py [--batch-size 5] [--interval 2] [--batches N]

Sans --batches, il tourne jusqu'à Ctrl+C. Chaque salve est une transaction : elle est validée en entier
ou pas du tout, et seules les opérations validées s'affichent.
"""

import argparse
import os
import random
import signal
import sys
import threading
from collections import Counter
from typing import Any

import psycopg
from faker import Faker
from rich.console import Console
from rich.markup import escape

# Adresses de la machine par défaut ; le service data-generator passe par le réseau Docker
CONNEXION: dict[str, str] = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": os.environ.get("POSTGRES_PORT", "15432"),
    "dbname": "boutique",
    "user": "admin",
    "password": "password",
}

OPERATIONS = ["INSERT", "UPDATE", "DELETE"]
POIDS = [60, 30, 10]
COULEURS = {"INSERT": "green", "UPDATE": "yellow", "DELETE": "red"}
POINTS_MAX = 5000

# Une seule requête choisit la ligne et la modifie : elle vise toujours un client qui existe encore
CLIENT_AU_HASARD = "id = (SELECT id FROM customers ORDER BY random() LIMIT 1)"


def inserer(cur: psycopg.Cursor[Any], faux: Faker) -> str:
    nom, email, ville, points = faux.name(), faux.ascii_safe_email(), faux.city(), random.randint(0, POINTS_MAX)
    cur.execute(
        "INSERT INTO customers (full_name, email, city, loyalty_points) VALUES (%s, %s, %s, %s) RETURNING id",
        [nom, email, ville, points],
    )
    return f"id={cur.fetchone()[0]} {nom}, {ville}, {points} points"


def modifier(cur: psycopg.Cursor[Any], faux: Faker) -> str | None:
    if random.random() < 0.5:
        colonne, valeur = "city", faux.city()
    else:
        colonne, valeur = "loyalty_points", random.randint(0, POINTS_MAX)
    # La valeur par défaut de updated_at ne joue qu'à l'insertion : un UPDATE doit l'actualiser lui-même
    cur.execute(
        f"UPDATE customers SET {colonne} = %s, updated_at = clock_timestamp() WHERE {CLIENT_AU_HASARD} RETURNING id",
        [valeur],
    )
    ligne = cur.fetchone()
    return ligne and f"id={ligne[0]} {colonne} = {valeur}"


def supprimer(cur: psycopg.Cursor[Any]) -> str | None:
    cur.execute(f"DELETE FROM customers WHERE {CLIENT_AU_HASARD} RETURNING id, full_name")
    ligne = cur.fetchone()
    return ligne and f"id={ligne[0]} {ligne[1]}"


def appliquer(cur: psycopg.Cursor[Any], faux: Faker, operation: str) -> tuple[str, str]:
    """Applique l'opération et renvoie ce qui a réellement été fait, pour l'affichage."""
    if operation == "UPDATE" and (detail := modifier(cur, faux)):
        return "UPDATE", detail
    if operation == "DELETE" and (detail := supprimer(cur)):
        return "DELETE", detail
    # INSERT demandé, ou table vide : il n'y avait rien à modifier ni à supprimer
    return "INSERT", inserer(cur, faux)


def au_moins_un(texte: str) -> int:
    valeur = int(texte)
    if valeur < 1:
        raise argparse.ArgumentTypeError("doit valoir au moins 1")
    return valeur


def lire_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Génère du trafic sur public.customers.")
    parser.add_argument("--batch-size", type=au_moins_un, default=5, help="opérations par salve (défaut : 5)")
    parser.add_argument("--interval", type=float, default=2.0, help="secondes entre deux salves (défaut : 2)")
    parser.add_argument("--batches", type=au_moins_un, help="nombre de salves (défaut : sans fin)")
    return parser.parse_args()


def main() -> int:
    args = lire_arguments()
    console = Console(highlight=False)
    faux = Faker("fr_FR")
    comptes: Counter[str] = Counter()
    salves = 0

    # Un signal ne coupe jamais une salve : elle se termine, puis la boucle s'arrête
    arret = threading.Event()
    for nom_signal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(nom_signal, lambda *_: arret.set())

    code = 0
    try:
        with psycopg.connect(**CONNEXION, autocommit=True) as connexion:
            while not arret.is_set() and (args.batches is None or salves < args.batches):
                # Validée à la sortie du bloc, ou annulée si une opération échoue
                with connexion.transaction(), connexion.cursor() as cur:
                    faites = [appliquer(cur, faux, op) for op in random.choices(OPERATIONS, POIDS, k=args.batch_size)]
                salves += 1
                for operation, detail in faites:
                    comptes[operation] += 1
                    console.print(f"[{COULEURS[operation]}]{operation}[/] {escape(detail)}", soft_wrap=True)
                console.print(f"[bold]Salve {salves} validée[/] : {len(faites)} opérations", soft_wrap=True)
                if args.batches is None or salves < args.batches:
                    arret.wait(args.interval)
    except psycopg.Error as erreur:
        console.print(f"[bold red]Erreur PostgreSQL, rien n'est validé pour la salve en cours :[/] {escape(str(erreur))}", soft_wrap=True)
        code = 1

    bilan = ", ".join(f"{comptes[operation]} {operation}" for operation in OPERATIONS)
    console.print(f"[bold]Bilan[/] : {salves} salve(s) validée(s), {bilan}", soft_wrap=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
