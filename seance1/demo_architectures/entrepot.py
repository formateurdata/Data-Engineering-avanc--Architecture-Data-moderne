"""Architecture A : un entrepôt relationnel (PostgreSQL).

Schéma à l'écriture : la table transactions a des types fixés (décimaux exacts, horodatage).
Chaque journée est copiée depuis son zip dans une table de transit, puis convertie et insérée
dans transactions. Le chargement mesuré comprend la copie, la conversion et ANALYZE.
"""

import time
import zipfile

import psycopg

from besoin import JOURS, REQUETE, archive
from mesure import chronometrer, ecrire_resultat

DSN = "postgresql://demo:demo@localhost:5442/marche"

TRANSIT = """CREATE UNLOGGED TABLE transit (trade_id bigint, prix numeric, quantite numeric,
    montant numeric, horodatage_us bigint, acheteur_maker boolean, meilleur_prix boolean)"""

TRANSACTIONS = """CREATE TABLE transactions (trade_id bigint, prix numeric(18, 8),
    quantite numeric(18, 8), montant numeric(24, 8), horodatage timestamp, acheteur_maker boolean)"""

CONVERTIR = """INSERT INTO transactions
SELECT trade_id, prix, quantite, montant,
       timestamp '1970-01-01' + horodatage_us * interval '1 microsecond', acheteur_maker
FROM transit"""


def charger_jour(cur, jour):
    """Copie le CSV du zip dans transit, sans l'extraire sur le disque, puis le convertit."""
    with zipfile.ZipFile(archive(jour)) as z, z.open(z.namelist()[0]) as f, \
            cur.copy("COPY transit FROM STDIN (FORMAT csv)") as copy:
        while bloc := f.read(1 << 20):
            copy.write(bloc)
    cur.execute(CONVERTIR)
    cur.execute("TRUNCATE transit")


def main():
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        for table in ("transactions", "transit"):
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(TRANSIT)
        cur.execute(TRANSACTIONS)
        conn.commit()
        durees = []
        for jour in JOURS:
            debut = time.perf_counter()
            charger_jour(cur, jour)
            cur.execute("ANALYZE transactions")
            conn.commit()
            durees.append(time.perf_counter() - debut)
        cur.execute("DROP TABLE transit")
        cur.execute("SELECT COUNT(*), pg_total_relation_size('transactions') FROM transactions")
        lignes_table, taille = cur.fetchone()
        mesures = {"lignes": lignes_table,
                   "chargement_jour_max_s": round(max(durees), 2),
                   "chargement_total_s": round(sum(durees), 2),
                   "stockage_mo": round(taille / 1e6, 1)}

        def requete():
            cur.execute(REQUETE)
            return cur.fetchall()

        lignes, mesures["requete_s"] = chronometrer(requete)
    ecrire_resultat("entrepot", mesures, lignes)
