# =============================================================================
# etl/load.py - Chargement du Star Schema dans PostgreSQL
# =============================================================================

import os
import logging
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL.Load")

# Ordre de chargement respectant les contraintes FK
ORDRE_CHARGEMENT = [
    "dim_client",
    "dim_produit",
    "dim_vendeur",
    "dim_temps",
    "dim_localisation",
    "fait_commandes",
]


def creer_moteur_bdd() -> object:
    """
    Crée et retourne un moteur SQLAlchemy connecté à PostgreSQL.

    Returns:
        Moteur SQLAlchemy actif.

    Raises:
        SQLAlchemyError: En cas d'échec de connexion.
    """
    try:
        moteur = create_engine(DB_URL, echo=False)
        with moteur.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connexion PostgreSQL établie : {DB_URL.split('@')[1]}")
        return moteur
    except SQLAlchemyError as e:
        logger.error(f"Impossible de se connecter à PostgreSQL : {e}")
        raise


def charger_table(
    df: pd.DataFrame,
    nom_table: str,
    moteur: object,
    si_existant: str = "append",
) -> int:
    """
    Charge un DataFrame dans une table PostgreSQL via to_sql().

    Args:
        df: DataFrame à insérer.
        nom_table: Nom de la table cible dans PostgreSQL.
        moteur: Moteur SQLAlchemy.
        si_existant: Comportement si la table existe ('append' ou 'replace').

    Returns:
        Nombre de lignes insérées.
    """
    try:
        # Vider la table avant insertion pour éviter les doublons en rejeu
        with moteur.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {nom_table} RESTART IDENTITY CASCADE"))
        logger.info(f"Table '{nom_table}' vidée avant rechargement.")
    except Exception as e:
        logger.warning(f"TRUNCATE impossible sur '{nom_table}' (peut-être vide) : {e}")

    try:
        df_a_inserer = df.copy()

        # dim_temps : PostgreSQL génère date_id en SERIAL, on l'exclut
        if nom_table == "dim_temps" and "date_id" in df_a_inserer.columns:
            df_a_inserer = df_a_inserer.drop(columns=["date_id"])

        # dim_localisation : PostgreSQL génère localisation_id en SERIAL
        if nom_table == "dim_localisation" and "localisation_id" in df_a_inserer.columns:
            df_a_inserer = df_a_inserer.drop(columns=["localisation_id"])

        df_a_inserer.to_sql(
            name=nom_table,
            con=moteur,
            if_exists=si_existant,
            index=False,
            method="multi",
            chunksize=1000,
        )

        nb_lignes = len(df_a_inserer)
        logger.info(f"  ✓ {nom_table:<25} : {nb_lignes:>8,} lignes insérées")
        return nb_lignes

    except SQLAlchemyError as e:
        logger.error(f"Erreur lors du chargement de '{nom_table}' : {e}")
        raise


def charger_star_schema(star_schema: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    """
    Charge l'ensemble du Star Schema dans PostgreSQL dans le bon ordre.

    Args:
        star_schema: Dictionnaire {nom_table: DataFrame} du Star Schema.

    Returns:
        Dictionnaire {nom_table: nb_lignes_insérées}.
    """
    logger.info("Démarrage du chargement dans PostgreSQL...")
    moteur = creer_moteur_bdd()
    resultats: Dict[str, int] = {}

    for nom_table in ORDRE_CHARGEMENT:
        if nom_table not in star_schema:
            logger.warning(f"Table '{nom_table}' absente du Star Schema, ignorée.")
            continue

        df = star_schema[nom_table]
        nb = charger_table(df, nom_table, moteur)
        resultats[nom_table] = nb

    # Résumé final
    total = sum(resultats.values())
    logger.info("\n" + "="*50)
    logger.info("RÉSUMÉ DU CHARGEMENT")
    logger.info("="*50)
    for table, nb in resultats.items():
        logger.info(f"  {table:<25} : {nb:>8,} lignes")
    logger.info(f"  {'TOTAL':<25} : {total:>8,} lignes")
    logger.info("="*50)

    moteur.dispose()
    return resultats


if __name__ == "__main__":
    from extract import extraire_toutes_les_donnees
    from transform import transformer

    dfs_bruts = extraire_toutes_les_donnees()
    star_schema = transformer(dfs_bruts)
    charger_star_schema(star_schema)
