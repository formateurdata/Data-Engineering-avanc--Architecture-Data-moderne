"""Démo de la séance 1 : un besoin, deux architectures.

Usage :
  python run.py telecharger   télécharge les 7 journées BTCUSDT de Binance et vérifie leur empreinte
  python run.py controler     contrôle chaque journée avant de la charger
  python run.py entrepot      architecture A : PostgreSQL (docker compose up -d avant)
  python run.py lac           architecture B : fichiers Parquet interrogés avec DuckDB
  python run.py comparer      critère de réussite, puis confrontation aux exigences
  python run.py tout          controler, entrepot, lac, comparer
"""

import csv
import hashlib
import json
import sys
import tempfile
import urllib.request
import zipfile
from decimal import Decimal
from pathlib import Path

import duckdb

from besoin import (BASE_URL, CHARGEMENT_JOUR_MAX_S, COLONNES, DATA, JOURS, REQUETE_MAX_S, RESULTATS, archive,
                    nom_fichier)


def telecharger():
    """Lit la source externe à chaque appel, puis vérifie l'empreinte SHA-256 publiée par Binance."""
    DATA.mkdir(exist_ok=True)
    for jour in JOURS:
        url = f"{BASE_URL}/{nom_fichier(jour)}"
        urllib.request.urlretrieve(url, archive(jour))
        attendue = urllib.request.urlopen(f"{url}.CHECKSUM").read().decode().split()[0]
        obtenue = hashlib.sha256(archive(jour).read_bytes()).hexdigest()
        if obtenue != attendue:
            sys.exit(f"{nom_fichier(jour)} : empreinte différente de celle publiée, fichier rejeté")
        print(f"{nom_fichier(jour)} : {archive(jour).stat().st_size / 1e6:.1f} Mo, empreinte vérifiée")


def controler():
    """Arrête la démo si une journée est incomplète ou incohérente ; affiche ce qu'elle contient."""
    con = duckdb.connect()
    total, prix_min, prix_max = 0, None, None
    for jour in JOURS:
        with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(archive(jour)) as z:
            fichier = Path(z.extract(z.namelist()[0], tmp))
            types = dict(zip(COLONNES, ["BIGINT", "DECIMAL(18, 8)", "DECIMAL(18, 8)", "DECIMAL(24, 8)",
                                        "BIGINT", "BOOLEAN", "BOOLEAN"]))
            n, ids, id_min, id_max, hors_jour, non_positifs, pmin, pmax = con.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT trade_id), MIN(trade_id), MAX(trade_id),
                       COUNT(*) FILTER (WHERE CAST(make_timestamp(horodatage_us) AS DATE) <> DATE '{jour}'),
                       COUNT(*) FILTER (WHERE prix <= 0 OR quantite <= 0),
                       MIN(prix), MAX(prix)
                FROM read_csv('{fichier}', header = false, columns = {types})""").fetchone()
        problemes = []
        if ids != n or id_max - id_min + 1 != n:
            problemes.append("identifiants manquants ou en double")
        if hors_jour:
            problemes.append(f"{hors_jour} transactions hors du jour")
        if non_positifs:
            problemes.append(f"{non_positifs} prix ou quantités nuls ou négatifs")
        if problemes:
            sys.exit(f"{jour} rejeté : {', '.join(problemes)}")
        print(f"{jour} : {n} transactions, identifiants continus, prix de {pmin} à {pmax} USDT")
        total += n
        prix_min = pmin if prix_min is None else min(prix_min, pmin)
        prix_max = pmax if prix_max is None else max(prix_max, pmax)
    print(f"Semaine : {total} transactions, prix de {prix_min} à {prix_max} USDT")


def lire(nom):
    with open(RESULTATS / f"{nom}.csv", encoding="utf-8") as f:
        lignes = list(csv.reader(f))[1:]
    mesures = json.loads((RESULTATS / f"{nom}.json").read_text(encoding="utf-8"))
    return lignes, mesures


def comparer():
    a, mesures_a = lire("entrepot")
    b, mesures_b = lire("lac")
    # On compare le contenu, trié de la même façon des deux côtés
    a, b = sorted(a), sorted(b)
    identiques = a == b
    print(f"Critère de réussite : tableaux identiques à la huitième décimale -> {'OUI' if identiques else 'NON'}"
          f" ({len(a)} minutes)")
    if not identiques:
        ecarts = [(x, y) for x, y in zip(a, b) if x != y][:5]
        sys.exit(f"Tableaux différents, premiers écarts : {ecarts}")

    transactions = sum(int(row[1]) for row in a)
    volume = sum(Decimal(row[2]) for row in a)
    montant = sum(Decimal(row[3]) for row in a)
    pointe = max(a, key=lambda row: int(row[1]))
    print(f"Transactions : {transactions} ; volume : {volume} BTC ; montant : {montant} USDT")
    print(f"Prix moyen pondéré de la semaine : {montant / volume:.2f} USDT")
    print(f"Minute la plus active : {pointe[0]}, {pointe[1]} transactions")

    print(f"\n{'Mesure':<34}{'A entrepôt':>12}{'B lac':>12}{'Exigence':>12}")
    lignes = [("Chargement, pire journée (s)", "chargement_jour_max_s", f"< {CHARGEMENT_JOUR_MAX_S}"),
              ("Chargement, semaine (s)", "chargement_total_s", "-"),
              ("Stockage (Mo)", "stockage_mo", "-"),
              ("Requête, médiane de 5 (s)", "requete_s", f"< {REQUETE_MAX_S}")]
    for libelle, cle, exigence in lignes:
        print(f"{libelle:<34}{mesures_a[cle]:>12}{mesures_b[cle]:>12}{exigence:>12}")
    for nom, m in [("A entrepôt", mesures_a), ("B lac", mesures_b)]:
        ok = m["chargement_jour_max_s"] < CHARGEMENT_JOUR_MAX_S and m["requete_s"] < REQUETE_MAX_S
        print(f"{nom} : exigences {'respectées' if ok else 'NON respectées'}")


def main(commande):
    if commande == "telecharger":
        telecharger()
    elif commande == "controler":
        controler()
    elif commande == "entrepot":
        import entrepot
        entrepot.main()
    elif commande == "lac":
        import lac
        lac.main()
    elif commande == "comparer":
        comparer()
    elif commande == "tout":
        for etape in ["controler", "entrepot", "lac", "comparer"]:
            print(f"\n== {etape}")
            main(etape)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
