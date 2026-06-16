# =============================================================================
# etl/extract.py - Extraction des données depuis les fichiers CSV Olist
# =============================================================================

import os
import logging
from typing import Dict
import pandas as pd

# Import de la configuration centralisée
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_PATH, CSV_FILES

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL.Extract")


def charger_csv(nom_fichier: str, sep: str = ",", encoding: str = "utf-8") -> pd.DataFrame:
    """
    Charge un fichier CSV depuis le dossier data/.

    Args:
        nom_fichier: Nom du fichier CSV (sans chemin).
        sep: Séparateur de colonnes (défaut: virgule).
        encoding: Encodage du fichier (défaut: utf-8).

    Returns:
        DataFrame pandas contenant les données du CSV.

    Raises:
        FileNotFoundError: Si le fichier n'existe pas.
    """
    chemin = os.path.join(DATA_PATH, nom_fichier)
    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Fichier introuvable : {chemin}")

    df = pd.read_csv(chemin, sep=sep, encoding=encoding, low_memory=False)
    logger.info(f"Chargé : {nom_fichier} ({len(df):,} lignes, {len(df.columns)} colonnes)")
    return df


def afficher_resume(nom: str, df: pd.DataFrame) -> None:
    """
    Affiche un résumé statistique d'un DataFrame.

    Args:
        nom: Nom descriptif du dataset.
        df: DataFrame à analyser.
    """
    manquants = df.isnull().sum()
    manquants_total = manquants[manquants > 0]

    logger.info(f"\n{'='*60}")
    logger.info(f"  RÉSUMÉ : {nom}")
    logger.info(f"{'='*60}")
    logger.info(f"  Lignes         : {len(df):,}")
    logger.info(f"  Colonnes       : {len(df.columns)}")
    logger.info(f"  Colonnes       : {list(df.columns)}")

    if len(manquants_total) > 0:
        logger.info(f"  Valeurs manquantes :")
        for col, nb in manquants_total.items():
            pct = nb / len(df) * 100
            logger.info(f"    - {col}: {nb:,} ({pct:.1f}%)")
    else:
        logger.info("  Valeurs manquantes : aucune")


def extraire_toutes_les_donnees() -> Dict[str, pd.DataFrame]:
    """
    Charge les 9 fichiers CSV Olist et retourne un dictionnaire de DataFrames.

    Returns:
        Dictionnaire {clé: DataFrame} avec les 9 jeux de données Olist.
    """
    logger.info("Démarrage de l'extraction des données...")
    dataframes: Dict[str, pd.DataFrame] = {}

    for cle, nom_fichier in CSV_FILES.items():
        try:
            df = charger_csv(nom_fichier)
            afficher_resume(cle.upper(), df)
            dataframes[cle] = df
        except FileNotFoundError as e:
            logger.error(f"ERREUR - {e}")
            raise
        except Exception as e:
            logger.error(f"Erreur inattendue lors du chargement de {nom_fichier} : {e}")
            raise

    logger.info(f"\nExtraction terminée : {len(dataframes)} fichiers chargés.")
    return dataframes


if __name__ == "__main__":
    dfs = extraire_toutes_les_donnees()
    logger.info("Test d'extraction réussi.")
