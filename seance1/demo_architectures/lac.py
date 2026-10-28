"""Architecture B : un lac de fichiers Parquet, interrogé avec DuckDB.

Chaque journée est extraite de son zip, convertie en Parquet par DuckDB et déposée dans le lac,
dans un dossier par jour. Aucune table n'est créée : DuckDB lit les fichiers au moment de la
requête. Le chargement mesuré comprend l'extraction et la conversion.
"""

import tempfile
import time
import zipfile
from pathlib import Path

import duckdb

from besoin import COLONNES, ICI, JOURS, REQUETE, archive
from mesure import chronometrer, ecrire_resultat

LAC = ICI / "lac"
TYPES = ["BIGINT", "DECIMAL(18, 8)", "DECIMAL(18, 8)", "DECIMAL(24, 8)", "BIGINT", "BOOLEAN", "BOOLEAN"]

CONVERTIR = """COPY (
    SELECT trade_id, prix, quantite, montant,
           make_timestamp(horodatage_us) AS horodatage, acheteur_maker
    FROM read_csv('{csv}', header = false, columns = {colonnes})
) TO '{parquet}' (FORMAT parquet)"""


def deposer_jour(con, jour):
    """Extrait le CSV du jour dans un dossier temporaire, puis l'écrit en Parquet dans le lac."""
    cible = LAC / "transactions" / f"jour={jour.isoformat()}" / "transactions.parquet"
    cible.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(archive(jour)) as z:
        csv = Path(z.extract(z.namelist()[0], tmp))
        colonnes = dict(zip(COLONNES, TYPES))
        con.execute(CONVERTIR.format(csv=csv, colonnes=colonnes, parquet=cible))
    return cible


def main():
    con = duckdb.connect()
    durees, fichiers = [], []
    for jour in JOURS:
        debut = time.perf_counter()
        fichiers.append(deposer_jour(con, jour))
        durees.append(time.perf_counter() - debut)
    con.execute(f"CREATE VIEW transactions AS SELECT * FROM read_parquet('{LAC}/transactions/*/*.parquet')")
    mesures = {"lignes": con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0],
               "chargement_jour_max_s": round(max(durees), 2),
               "chargement_total_s": round(sum(durees), 2),
               "stockage_mo": round(sum(f.stat().st_size for f in fichiers) / 1e6, 1)}
    lignes, mesures["requete_s"] = chronometrer(lambda: con.execute(REQUETE).fetchall())
    ecrire_resultat("lac", mesures, lignes)
