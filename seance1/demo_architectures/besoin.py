"""Le besoin commun aux deux architectures : données, exigences chiffrées, requête.

Les données : chaque transaction BTC/USDT exécutée sur Binance pendant une semaine, publiée
gratuitement sur https://data.binance.vision (un fichier zip par jour, avec son empreinte SHA-256).
Le besoin fonctionnel : pour chaque minute de la semaine, le nombre de transactions, le volume
échangé en BTC et le montant échangé en USDT, exacts à la huitième décimale.
Les exigences non fonctionnelles sont fixées ici, avant toute mesure.
"""

from datetime import date, timedelta
from pathlib import Path

SYMBOLE = "BTCUSDT"
PREMIER_JOUR = date(2026, 10, 5)   # lundi
JOURS = [PREMIER_JOUR + timedelta(days=i) for i in range(7)]
DEBUT, FIN = JOURS[0].isoformat(), (JOURS[-1] + timedelta(days=1)).isoformat()

BASE_URL = f"https://data.binance.vision/data/spot/daily/trades/{SYMBOLE}"

ICI = Path(__file__).parent
DATA = ICI / "data"
RESULTATS = ICI / "resultats"


def nom_fichier(jour):
    return f"{SYMBOLE}-trades-{jour.isoformat()}.zip"


def archive(jour):
    return DATA / nom_fichier(jour)


# Colonnes des fichiers trades de Binance (README de binance/binance-public-data) ;
# depuis le 1er janvier 2025, l'horodatage est en microsecondes.
COLONNES = ["trade_id", "prix", "quantite", "montant", "horodatage_us", "acheteur_maker", "meilleur_prix"]

# Exigences non fonctionnelles, fixées avant la première mesure
CHARGEMENT_JOUR_MAX_S = 60   # une journée publiée est chargée en moins d'une minute
REQUETE_MAX_S = 2.0          # le tableau minute par minute de la semaine s'affiche en moins de 2 s

# La même requête SQL tourne sur PostgreSQL et sur DuckDB
REQUETE = f"""
SELECT date_trunc('minute', horodatage) AS minute,
       COUNT(*) AS transactions,
       SUM(quantite) AS volume_btc,
       SUM(montant) AS montant_usdt
FROM transactions
WHERE horodatage >= '{DEBUT}' AND horodatage < '{FIN}'
GROUP BY 1
ORDER BY 1
"""
