# =============================================================================
# kafka/producer_payments.py - Producteur Kafka pour le stream de paiements
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
logger = logging.getLogger("Kafka.ProducerPayments")

TOPIC = KAFKA_CONFIG["topics"]["payments"]
MESSAGES_PAR_SEC = 50
DELAI_SEC = 1.0 / MESSAGES_PAR_SEC   # 0.02 seconde entre chaque message
LOG_INTERVAL = 200


def rapport_livraison(err, msg) -> None:
    """Callback appelé par confluent-kafka après chaque tentative de livraison."""
    if err is not None:
        logger.warning(f"Erreur de livraison pour {msg.topic()} : {err}")


def creer_producteur() -> Producer:
    """
    Crée et retourne un producteur confluent-kafka.

    Returns:
        Instance Producer configurée.

    Raises:
        KafkaException: Si la configuration est invalide.
    """
    conf = {
        "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
        "acks":              "all",
        "retries":           3,
        "linger.ms":         5,
    }
    try:
        producteur = Producer(conf)
        logger.info(f"Producteur paiements connecté : {KAFKA_CONFIG['bootstrap_servers']}")
        return producteur
    except KafkaException as e:
        logger.error(f"Connexion Kafka impossible : {e}")
        raise


def produire_paiements() -> None:
    """
    Lit olist_order_payments_dataset.csv et envoie chaque paiement
    dans le topic 'payments-stream' à 50 messages par seconde.
    """
    chemin_csv = os.path.join(DATA_PATH, CSV_FILES["order_payments"])
    logger.info(f"Lecture : {chemin_csv}")

    df = pd.read_csv(chemin_csv, low_memory=False)
    logger.info(f"{len(df):,} paiements à envoyer dans '{TOPIC}' à {MESSAGES_PAR_SEC} msg/s")

    producteur = creer_producteur()
    nb_envoyes = 0
    nb_erreurs = 0

    try:
        for _, ligne in df.iterrows():
            message = {
                "order_id":           str(ligne.get("order_id", "")),
                "payment_sequential": int(ligne.get("payment_sequential", 1)),
                "payment_type":       str(ligne.get("payment_type", "")),
                "payment_value":      float(ligne.get("payment_value", 0.0)),
                "installments":       int(ligne.get("payment_installments", 1)),
                "timestamp_actuel":   datetime.now().isoformat(),
            }

            try:
                producteur.produce(
                    TOPIC,
                    value=json.dumps(message, default=str, ensure_ascii=False).encode("utf-8"),
                    on_delivery=rapport_livraison,
                )
                producteur.poll(0)
                nb_envoyes += 1

                if nb_envoyes % LOG_INTERVAL == 0:
                    logger.info(f"[payments-stream] {nb_envoyes:,} paiements envoyés...")

                time.sleep(DELAI_SEC)

            except KafkaException as e:
                logger.warning(f"Erreur envoi paiement order_id={message['order_id']} : {e}")
                nb_erreurs += 1

        producteur.flush()
        logger.info(f"Production terminée : {nb_envoyes:,} envoyés, {nb_erreurs} erreurs.")

    except KeyboardInterrupt:
        logger.info(f"Arrêt manuel. {nb_envoyes:,} paiements envoyés.")
    finally:
        producteur.flush()
        logger.info("Producteur paiements fermé.")


if __name__ == "__main__":
    produire_paiements()
