# =============================================================================
# etl/transform.py - Transformation et construction du Star Schema
# =============================================================================

import os
import logging
import requests
from typing import Dict, Tuple
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL.Transform")


# =============================================================================
# RÉCUPÉRATION DES TAUX DE CHANGE
# =============================================================================

def recuperer_taux_change() -> Tuple[float, float]:
    """
    Appelle l'API Open Exchange Rates pour obtenir les taux BRL→EUR et BRL→MAD.
    L'API retourne des taux par rapport à USD, donc on calcule le croisement.

    Returns:
        Tuple (taux_eur, taux_mad) - taux de conversion depuis BRL.

    Raises:
        requests.RequestException: En cas d'échec de l'appel API.
    """
    logger.info(f"Appel API taux de change : {API_URL}")
    try:
        reponse = requests.get(API_URL, timeout=10)
        reponse.raise_for_status()
        donnees = reponse.json()

        taux_usd = donnees.get("rates", {})
        brl_par_usd = taux_usd.get("BRL", 5.10)   # BRL pour 1 USD
        eur_par_usd = taux_usd.get("EUR", 0.92)    # EUR pour 1 USD
        mad_par_usd = taux_usd.get("MAD", 10.05)   # MAD pour 1 USD

        # Conversion BRL → EUR : (EUR/USD) / (BRL/USD)
        taux_brl_eur = eur_par_usd / brl_par_usd
        # Conversion BRL → MAD : (MAD/USD) / (BRL/USD)
        taux_brl_mad = mad_par_usd / brl_par_usd

        logger.info(f"Taux BRL→EUR : {taux_brl_eur:.6f}")
        logger.info(f"Taux BRL→MAD : {taux_brl_mad:.6f}")
        return taux_brl_eur, taux_brl_mad

    except requests.RequestException as e:
        logger.warning(f"Échec API ({e}). Utilisation des taux de repli.")
        return 0.1803, 1.9677  # Taux de repli statiques


# =============================================================================
# NETTOYAGE GÉNÉRAL
# =============================================================================

def nettoyer_dataframes(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Supprime les doublons, les lignes sans order_id, et convertit les dates.

    Args:
        dfs: Dictionnaire de DataFrames bruts.

    Returns:
        Dictionnaire de DataFrames nettoyés.
    """
    logger.info("Nettoyage des données...")
    dfs_nettoyes = {}

    for cle, df in dfs.items():
        df = df.copy()
        nb_avant = len(df)

        # Suppression des doublons
        df = df.drop_duplicates()

        # Suppression des lignes sans order_id (si la colonne existe)
        if "order_id" in df.columns:
            df = df.dropna(subset=["order_id"])

        # Conversion des colonnes de dates
        colonnes_dates = [c for c in df.columns if "date" in c or "timestamp" in c]
        for col in colonnes_dates:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

        nb_apres = len(df)
        if nb_avant != nb_apres:
            logger.info(f"  {cle}: {nb_avant:,} → {nb_apres:,} lignes ({nb_avant - nb_apres:,} supprimées)")

        dfs_nettoyes[cle] = df

    return dfs_nettoyes


# =============================================================================
# CONSTRUCTION DES DIMENSIONS
# =============================================================================

def construire_dim_client(customers: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension client depuis le DataFrame customers.

    Args:
        customers: DataFrame olist_customers_dataset.

    Returns:
        DataFrame dim_client prêt à charger.
    """
    dim = customers[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
    ]].copy()

    dim = dim.rename(columns={
        "customer_id": "client_id",
        "customer_city": "ville",
        "customer_state": "etat",
        "customer_zip_code_prefix": "code_postal",
    })

    dim["code_postal"] = dim["code_postal"].astype(str)
    dim = dim.drop_duplicates(subset=["client_id"])
    logger.info(f"dim_client : {len(dim):,} lignes")
    return dim


def construire_dim_produit(
    products: pd.DataFrame,
    translations: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la dimension produit avec traduction des catégories.

    Args:
        products: DataFrame olist_products_dataset.
        translations: DataFrame product_category_name_translation.

    Returns:
        DataFrame dim_produit prêt à charger.
    """
    dim = products[[
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]].copy()

    # Jointure avec la traduction des catégories
    dim = dim.merge(translations, on="product_category_name", how="left")

    dim = dim.rename(columns={
        "product_id": "produit_id",
        "product_category_name": "categorie_pt",
        "product_category_name_english": "categorie_en",
        "product_weight_g": "poids_g",
        "product_length_cm": "longueur_cm",
        "product_height_cm": "hauteur_cm",
        "product_width_cm": "largeur_cm",
    })

    dim = dim.drop_duplicates(subset=["produit_id"])
    logger.info(f"dim_produit : {len(dim):,} lignes")
    return dim


def construire_dim_vendeur(sellers: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension vendeur.

    Args:
        sellers: DataFrame olist_sellers_dataset.

    Returns:
        DataFrame dim_vendeur prêt à charger.
    """
    dim = sellers[[
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix",
    ]].copy()

    dim = dim.rename(columns={
        "seller_id": "vendeur_id",
        "seller_city": "ville",
        "seller_state": "etat",
        "seller_zip_code_prefix": "code_postal",
    })

    dim["code_postal"] = dim["code_postal"].astype(str)
    dim = dim.drop_duplicates(subset=["vendeur_id"])
    logger.info(f"dim_vendeur : {len(dim):,} lignes")
    return dim


def construire_dim_temps(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la dimension temps à partir des dates de commandes.

    Args:
        orders: DataFrame olist_orders_dataset avec colonnes datetime.

    Returns:
        DataFrame dim_temps prêt à charger.
    """
    col_date = "order_purchase_timestamp"
    dates_uniques = orders[col_date].dropna().dt.date
    dates_uniques = pd.Series(pd.to_datetime(dates_uniques.unique())).sort_values().reset_index(drop=True)

    jours_fr = {
        "Monday": "Lundi", "Tuesday": "Mardi", "Wednesday": "Mercredi",
        "Thursday": "Jeudi", "Friday": "Vendredi", "Saturday": "Samedi",
        "Sunday": "Dimanche",
    }

    dim = pd.DataFrame()
    dim["date_complete"] = dates_uniques
    dim["jour"]          = dates_uniques.dt.day.astype("int16")
    dim["mois"]          = dates_uniques.dt.month.astype("int16")
    dim["trimestre"]     = dates_uniques.dt.quarter.astype("int16")
    dim["annee"]         = dates_uniques.dt.year.astype("int16")
    dim["jour_semaine"]  = dates_uniques.dt.day_name().map(jours_fr)
    dim["is_weekend"]    = dates_uniques.dt.dayofweek >= 5

    # La colonne date_id sera générée en SERIAL par PostgreSQL
    logger.info(f"dim_temps : {len(dim):,} dates uniques")
    return dim


def construire_dim_localisation(
    geolocation: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la dimension localisation en joignant geolocation avec customers.

    Args:
        geolocation: DataFrame olist_geolocation_dataset.
        customers: DataFrame olist_customers_dataset.

    Returns:
        DataFrame dim_localisation prêt à charger.
    """
    # Moyenne des coordonnées par code postal (évite les doublons)
    geo_agg = geolocation.groupby("geolocation_zip_code_prefix").agg(
        ville=("geolocation_city", "first"),
        etat=("geolocation_state", "first"),
        lat=("geolocation_lat", "mean"),
        lng=("geolocation_lng", "mean"),
    ).reset_index()

    geo_agg = geo_agg.rename(columns={
        "geolocation_zip_code_prefix": "code_postal",
    })

    geo_agg["code_postal"] = geo_agg["code_postal"].astype(str)

    # Garder uniquement les codes postaux présents chez les clients
    codes_clients = customers["customer_zip_code_prefix"].astype(str).unique()
    geo_agg = geo_agg[geo_agg["code_postal"].isin(codes_clients)].copy()

    geo_agg = geo_agg.reset_index(drop=True)
    # localisation_id sera généré en SERIAL par PostgreSQL
    logger.info(f"dim_localisation : {len(geo_agg):,} lignes")
    return geo_agg


# =============================================================================
# CONSTRUCTION DE LA TABLE DE FAITS
# =============================================================================

def construire_fait_commandes(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    order_payments: pd.DataFrame,
    order_reviews: pd.DataFrame,
    dim_client: pd.DataFrame,
    dim_produit: pd.DataFrame,
    dim_vendeur: pd.DataFrame,
    dim_temps: pd.DataFrame,
    dim_localisation: pd.DataFrame,
    taux_eur: float,
    taux_mad: float,
) -> pd.DataFrame:
    """
    Construit la table de faits fait_commandes.

    Args:
        orders, order_items, order_payments, order_reviews: DataFrames sources.
        dim_*: Tables de dimension déjà construites.
        taux_eur, taux_mad: Taux de conversion depuis BRL.

    Returns:
        DataFrame fait_commandes prêt à charger.
    """
    logger.info("Construction de fait_commandes...")

    # Agrégat des paiements par commande
    paiements_agg = order_payments.groupby("order_id").agg(
        montant_brl=("payment_value", "sum"),
        nb_versements=("payment_installments", "max"),
        type_paiement=("payment_type", "first"),
    ).reset_index()

    # Agrégat des articles par commande (prend le premier produit/vendeur)
    items_agg = order_items.groupby("order_id").agg(
        produit_id=("product_id", "first"),
        vendeur_id=("seller_id", "first"),
        frais_livraison=("freight_value", "sum"),
    ).reset_index()

    # Score de satisfaction moyen par commande
    reviews_agg = order_reviews.groupby("order_id").agg(
        score_satisfaction=("review_score", "mean"),
    ).reset_index()
    reviews_agg["score_satisfaction"] = reviews_agg["score_satisfaction"].round().astype("Int64")

    # Jointures progressives
    fait = orders[["order_id", "customer_id", "order_status", "order_purchase_timestamp"]].copy()
    fait = fait.merge(paiements_agg, on="order_id", how="left")
    fait = fait.merge(items_agg, on="order_id", how="left")
    fait = fait.merge(reviews_agg, on="order_id", how="left")

    # Conversion des montants
    fait["montant_eur"] = (fait["montant_brl"] * taux_eur).round(4)
    fait["montant_mad"] = (fait["montant_brl"] * taux_mad).round(4)

    # Résolution des clés FK : date_id
    dim_temps_indexed = dim_temps.copy().reset_index()
    dim_temps_indexed["date_id"] = dim_temps_indexed.index + 1  # simule SERIAL
    dim_temps_indexed["date_complete"] = pd.to_datetime(dim_temps_indexed["date_complete"])

    fait["date_achat"] = pd.to_datetime(fait["order_purchase_timestamp"]).dt.normalize()
    fait = fait.merge(
        dim_temps_indexed[["date_complete", "date_id"]],
        left_on="date_achat",
        right_on="date_complete",
        how="left",
    )

    # Résolution FK : localisation_id depuis le client
    clients_cp = dim_client[["client_id", "code_postal"]].copy()
    dim_loc_indexed = dim_localisation.copy().reset_index()
    dim_loc_indexed["localisation_id"] = dim_loc_indexed.index + 1

    fait = fait.merge(clients_cp, left_on="customer_id", right_on="client_id", how="left")
    fait = fait.merge(
        dim_loc_indexed[["code_postal", "localisation_id"]],
        on="code_postal",
        how="left",
    )

    # Renommage avant déduplication
    fait = fait.rename(columns={
        "customer_id": "client_id",
        "order_status": "statut",
    })

    # Suppression des colonnes dupliquées issues des jointures (ex: client_id apparaît
    # deux fois : depuis customer_id renommé ET depuis clients_cp mergé sur right_on="client_id")
    fait = fait.loc[:, ~fait.columns.duplicated()]
    logger.info(f"Colonnes après déduplication : {list(fait.columns)}")

    # Colonnes finales — correspondent exactement aux colonnes de fait_commandes dans create_tables.sql
    colonnes_finales = [
        "order_id", "client_id", "produit_id", "vendeur_id",
        "date_id", "localisation_id",
        "montant_brl", "montant_eur", "montant_mad",
        "frais_livraison", "score_satisfaction", "statut",
        "nb_versements", "type_paiement",
    ]

    fait = fait[colonnes_finales].copy()
    fait = fait.drop_duplicates(subset=["order_id"])
    logger.info(f"Colonnes de fait_commandes (vérification SQL) : {list(fait.columns)}")

    # Conversion des types
    fait["date_id"] = fait["date_id"].astype("Int64")
    fait["localisation_id"] = fait["localisation_id"].astype("Int64")
    fait["nb_versements"] = fait["nb_versements"].astype("Int64")

    logger.info(f"fait_commandes : {len(fait):,} lignes")
    return fait


# =============================================================================
# POINT D'ENTRÉE PRINCIPAL
# =============================================================================

def transformer(dfs_bruts: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Orchestre toutes les transformations et retourne les 6 DataFrames du Star Schema.

    Args:
        dfs_bruts: Dictionnaire des DataFrames bruts issus de l'extraction.

    Returns:
        Dictionnaire contenant les 6 tables du Star Schema.
    """
    logger.info("Démarrage des transformations...")

    # Étape 1 : Nettoyage
    dfs = nettoyer_dataframes(dfs_bruts)

    # Étape 2 : Taux de change
    taux_eur, taux_mad = recuperer_taux_change()

    # Étape 3 : Dimensions
    dim_client      = construire_dim_client(dfs["customers"])
    dim_produit     = construire_dim_produit(dfs["products"], dfs["category_translation"])
    dim_vendeur     = construire_dim_vendeur(dfs["sellers"])
    dim_temps       = construire_dim_temps(dfs["orders"])
    dim_localisation = construire_dim_localisation(dfs["geolocation"], dfs["customers"])

    # Étape 4 : Table de faits
    fait_commandes = construire_fait_commandes(
        orders=dfs["orders"],
        order_items=dfs["order_items"],
        order_payments=dfs["order_payments"],
        order_reviews=dfs["order_reviews"],
        dim_client=dim_client,
        dim_produit=dim_produit,
        dim_vendeur=dim_vendeur,
        dim_temps=dim_temps,
        dim_localisation=dim_localisation,
        taux_eur=taux_eur,
        taux_mad=taux_mad,
    )

    star_schema = {
        "dim_client":       dim_client,
        "dim_produit":      dim_produit,
        "dim_vendeur":      dim_vendeur,
        "dim_temps":        dim_temps,
        "dim_localisation": dim_localisation,
        "fait_commandes":   fait_commandes,
    }

    logger.info("Transformations terminées. Star Schema prêt.")
    return star_schema


if __name__ == "__main__":
    from extract import extraire_toutes_les_donnees
    dfs_bruts = extraire_toutes_les_donnees()
    star_schema = transformer(dfs_bruts)
    for nom, df in star_schema.items():
        logger.info(f"  {nom}: {len(df):,} lignes")
