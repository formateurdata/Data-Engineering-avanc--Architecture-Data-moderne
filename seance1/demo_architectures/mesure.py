"""Chronométrage et écriture des résultats, communs aux deux architectures."""

import csv
import json
import statistics
import time

from besoin import RESULTATS

REPETITIONS = 5


def chronometrer(fonction):
    """Exécute la fonction REPETITIONS fois ; renvoie son résultat et la durée médiane en secondes."""
    durees = []
    for _ in range(REPETITIONS):
        debut = time.perf_counter()
        resultat = fonction()
        durees.append(time.perf_counter() - debut)
    return resultat, round(statistics.median(durees), 3)


def ecrire_resultat(nom, mesures, lignes):
    """Écrit resultats/<nom>.json (mesures) et resultats/<nom>.csv (le tableau produit)."""
    RESULTATS.mkdir(exist_ok=True)
    mesures["lignes_resultat"] = len(lignes)
    (RESULTATS / f"{nom}.json").write_text(json.dumps(mesures, indent=2), encoding="utf-8")
    with open(RESULTATS / f"{nom}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["jour", "borough", "zone", "courses", "recettes"])
        writer.writerows(lignes)
    print(f"{nom} : {json.dumps(mesures, ensure_ascii=False)}")
