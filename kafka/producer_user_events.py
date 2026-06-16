# =============================================================================
# kafka/producer_user_events.py - Producteur Kafka d'événements utilisateurs
# =============================================================================

import os
import sys
import json
import time
import uuid
import random
import logging
from datetime import datetime
import pandas as pd
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka import KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_PATH, CSV_FILES, KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Kafka.ProducerUserEvents")

TOPIC = KAFKA_CONFIG["topics"]["user_events"]
EVENTS_PAR_SEC = 100
DELAI_SEC = 1.0 / EVENTS_PAR_SEC
LOG_INTERVAL = 500

ACTIONS = ["view_product", "add_to_cart", "remove_from_cart", "search", "checkout"]
POIDS_ACTIONS = [0.45, 0.25, 0.10, 0.15, 0.05]

DEVICES = ["mobile", "desktop", "tablet"]
POIDS_DEVICES = [0.55, 0.35, 0.10]

fake = Faker("pt_BR")


def rapport_livraison(err, msg) -> None:
    """Callback appelé par confluent-kafka après chaque tentative de livraison."""
    if err is not None:
        logger.warning(f"Erreur de livraison pour {msg.topic()} : {err}")


def charger_ids_reels() -> tuple:
    """
    Charge les vrais customer_id et product_id depuis les CSV Olist.

    Returns:
        Tuple (liste_customer_ids, liste_product_ids).
    """
    customers = pd.read_csv(
        os.path.join(DATA_PATH, CSV_FILES["customers"]),
        usecols=["customer_id"],
        low_memory=False,
    )
    products = pd.read_csv(
        os.path.join(DATA_PATH, CSV_FILES["products"]),
        usecols=["product_id"],
        low_memory=False,
    )

    customer_ids = customers["customer_id"].dropna().unique().tolist()
    product_ids = products["product_id"].dropna().unique().tolist()

    logger.info(f"IDs chargés : {len(customer_ids):,} clients, {len(product_ids):,} produits")
    return customer_ids, product_ids


def creer_producteur() -> Producer:
    """
    Crée et retourne un producteur confluent-kafka.

    Returns:
        Instance Producer configurée pour haut débit.

    Raises:
        KafkaException: Si la configuration est invalide.
    """
    conf = {
        "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
        "acks":              1,
        "retries":           2,
        "linger.ms":         2,
        "batch.size":        32768,
    }
    try:
        producteur = Producer(conf)
        logger.info(f"Producteur user-events connecté : {KAFKA_CONFIG['bootstrap_servers']}")
        return producteur
    except KafkaException as e:
        logger.error(f"Connexion Kafka impossible : {e}")
        raise


def generer_evenement(
    customer_ids: list,
    product_ids: list,
    sessions_actives: dict,
) -> dict:
    """
    Génère un événement utilisateur réaliste avec de vrais IDs.

    Args:
        customer_ids: Liste des vrais customer_id Olist.
        product_ids: Liste des vrais product_id Olist.
        sessions_actives: Dictionnaire {user_id: session_id} pour cohérence.

    Returns:
        Dictionnaire représentant l'événement.
    """
    user_id = random.choice(customer_ids)
    action = random.choices(ACTIONS, weights=POIDS_ACTIONS, k=1)[0]
    device = random.choices(DEVICES, weights=POIDS_DEVICES, k=1)[0]

    if user_id in sessions_actives and random.random() < 0.70:
        session_id = sessions_actives[user_id]
    else:
        session_id = str(uuid.uuid4())
        sessions_actives[user_id] = session_id

    product_id = random.choice(product_ids) if action != "search" else None

    return {
        "user_id":    user_id,
        "action":     action,
        "product_id": product_id,
        "session_id": session_id,
        "device":     device,
        "timestamp":  datetime.now().isoformat(),
    }


def produire_evenements_utilisateurs() -> None:
    """
    Génère et envoie en continu des événements utilisateurs dans le topic 'user-events'
    à raison de 100 événements par seconde.
    Tourne indéfiniment jusqu'à interruption clavier (Ctrl+C).
    """
    logger.info("Chargement des IDs réels depuis les CSV Olist...")
    customer_ids, product_ids = charger_ids_reels()

    producteur = creer_producteur()
    sessions_actives: dict = {}
    nb_envoyes = 0
    nb_erreurs = 0

    logger.info(f"Envoi de {EVENTS_PAR_SEC} événements/sec dans '{TOPIC}'. Ctrl+C pour arrêter.")

    try:
        while True:
            evenement = generer_evenement(customer_ids, product_ids, sessions_actives)

            try:
                producteur.produce(
                    TOPIC,
                    value=json.dumps(evenement, default=str, ensure_ascii=False).encode("utf-8"),
                    on_delivery=rapport_livraison,
                )
                producteur.poll(0)
                nb_envoyes += 1

                if nb_envoyes % LOG_INTERVAL == 0:
                    logger.info(
                        f"[user-events] {nb_envoyes:,} événements envoyés | "
                        f"Sessions actives : {len(sessions_actives)}"
                    )

                time.sleep(DELAI_SEC)

            except KafkaException as e:
                logger.warning(f"Erreur envoi événement : {e}")
                nb_erreurs += 1

    except KeyboardInterrupt:
        logger.info(f"\nArrêt manuel. {nb_envoyes:,} événements envoyés, {nb_erreurs} erreurs.")
    finally:
        producteur.flush()
        logger.info("Producteur user-events fermé.")


if __name__ == "__main__":
    produire_evenements_utilisateurs()
