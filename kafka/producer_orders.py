# =============================================================================
# kafka/producer_orders.py - Producteur Kafka pour le stream de commandes
# =============================================================================

import os
import sys
import json
import time
import logging
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka import KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_PATH, CSV_FILES, KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Kafka.ProducerOrders")

TOPIC = KAFKA_CONFIG["topics"]["orders"]
DELAI_SEC = 0.05   # 20 messages/seconde
LOG_INTERVAL = 100


def rapport_livraison(err, msg) -> None:
    """Callback appelé par confluent-kafka après chaque tentative de livraison."""
    if err is not None:
        logger.warning(f"Erreur de livraison pour {msg.topic()} : {err}")


def creer_producteur() -> Producer:
    """
    Crée et retourne un producteur confluent-kafka configuré.

    Returns:
        Instance Producer prête à l'emploi.

    Raises:
        KafkaException: Si la configuration est invalide.
    """
    conf = {
        "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
        "acks":              "all",
        "retries":           3,
        "linger.ms":         10,
    }
    try:
        producteur = Producer(conf)
        logger.info(f"Producteur connecté au broker : {KAFKA_CONFIG['bootstrap_servers']}")
        return producteur
    except KafkaException as e:
        logger.error(f"Impossible de créer le producteur Kafka : {e}")
        raise


def produire_commandes() -> None:
    """
    Lit olist_orders_dataset.csv et envoie chaque commande dans le topic 'orders-stream'.
    Affiche un log toutes les LOG_INTERVAL commandes.
    """
    chemin_csv = os.path.join(DATA_PATH, CSV_FILES["orders"])
    logger.info(f"Lecture du fichier : {chemin_csv}")

    df = pd.read_csv(chemin_csv, low_memory=False)
    logger.info(f"{len(df):,} commandes à envoyer dans le topic '{TOPIC}'")

    producteur = creer_producteur()
    nb_envoyes = 0
    nb_erreurs = 0

    try:
        for _, ligne in df.iterrows():
            message = {
                "order_id":         str(ligne.get("order_id", "")),
                "customer_id":      str(ligne.get("customer_id", "")),
                "statut":           str(ligne.get("order_status", "")),
                "timestamp_actuel": datetime.now().isoformat(),
            }

            try:
                producteur.produce(
                    TOPIC,
                    value=json.dumps(message, default=str, ensure_ascii=False).encode("utf-8"),
                    on_delivery=rapport_livraison,
                )
                # Déclenche les callbacks de livraison sans bloquer
                producteur.poll(0)
                nb_envoyes += 1

                if nb_envoyes % LOG_INTERVAL == 0:
                    logger.info(f"[orders-stream] {nb_envoyes:,} commandes envoyées...")

                time.sleep(DELAI_SEC)

            except KafkaException as e:
                logger.warning(f"Erreur d'envoi pour order_id={message['order_id']} : {e}")
                nb_erreurs += 1

        producteur.flush()
        logger.info(f"Production terminée : {nb_envoyes:,} envoyés, {nb_erreurs} erreurs.")

    except KeyboardInterrupt:
        logger.info(f"Arrêt manuel. {nb_envoyes:,} messages envoyés.")
    finally:
        producteur.flush()
        logger.info("Producteur fermé.")


if __name__ == "__main__":
    produire_commandes()
