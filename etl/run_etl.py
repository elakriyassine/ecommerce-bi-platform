# =============================================================================
# etl/run_etl.py - Pipeline ETL principal : Extract → Transform → Load
# =============================================================================

import os
import sys
import logging
import time

# Ajout du répertoire racine au path Python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import extraire_toutes_les_donnees
from etl.transform import transformer
from etl.load import charger_star_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL.Pipeline")


def executer_pipeline() -> None:
    """
    Exécute le pipeline ETL complet dans l'ordre Extract → Transform → Load.
    Affiche le temps d'exécution de chaque étape et le total.
    """
    logger.info("=" * 60)
    logger.info("  DÉMARRAGE DU PIPELINE ETL - E-Commerce BI Platform")
    logger.info("=" * 60)

    debut_total = time.time()

    # ─── ÉTAPE 1 : EXTRACT ────────────────────────────────────────────────
    logger.info("\n[ÉTAPE 1/3] EXTRACTION DES DONNÉES")
    debut = time.time()
    try:
        dfs_bruts = extraire_toutes_les_donnees()
    except Exception as e:
        logger.error(f"ÉCHEC de l'extraction : {e}")
        sys.exit(1)
    logger.info(f"Extraction terminée en {time.time() - debut:.2f}s")

    # ─── ÉTAPE 2 : TRANSFORM ──────────────────────────────────────────────
    logger.info("\n[ÉTAPE 2/3] TRANSFORMATION DES DONNÉES")
    debut = time.time()
    try:
        star_schema = transformer(dfs_bruts)
    except Exception as e:
        logger.error(f"ÉCHEC de la transformation : {e}")
        sys.exit(1)
    logger.info(f"Transformation terminée en {time.time() - debut:.2f}s")

    # ─── ÉTAPE 3 : LOAD ───────────────────────────────────────────────────
    logger.info("\n[ÉTAPE 3/3] CHARGEMENT DANS POSTGRESQL")
    debut = time.time()
    try:
        resultats = charger_star_schema(star_schema)
    except Exception as e:
        logger.error(f"ÉCHEC du chargement : {e}")
        sys.exit(1)
    logger.info(f"Chargement terminé en {time.time() - debut:.2f}s")

    # ─── RÉSUMÉ FINAL ─────────────────────────────────────────────────────
    duree_totale = time.time() - debut_total
    logger.info("\n" + "=" * 60)
    logger.info(f"  PIPELINE ETL TERMINÉ AVEC SUCCÈS en {duree_totale:.1f}s")
    logger.info("=" * 60)
    logger.info(f"  Tables chargées : {len(resultats)}")
    logger.info(f"  Total lignes    : {sum(resultats.values()):,}")
    logger.info("\nProchaines étapes :")
    logger.info("  → Démarrer Kafka : kafka/start_kafka.bat")
    logger.info("  → Lancer les producteurs et le consommateur")
    logger.info("  → Ouvrir le notebook ML : jupyter notebook ml/churn_prediction.ipynb")


if __name__ == "__main__":
    executer_pipeline()
