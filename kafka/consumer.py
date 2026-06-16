# =============================================================================
# kafka/consumer.py - Consommateur Kafka multi-topics avec persistance PostgreSQL
# =============================================================================

import os
import sys
import json
import logging
from datetime import datetime
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Kafka.Consumer")

TOPICS = list(KAFKA_CONFIG["topics"].values())
LOG_INTERVAL = 100


# =============================================================================
# CONNEXION POSTGRESQL
# =============================================================================

def connecter_postgresql() -> psycopg2.extensions.connection:
    """
    Crée une connexion psycopg2 à PostgreSQL.

    Returns:
        Connexion psycopg2 active.

    Raises:
        psycopg2.OperationalError: Si la connexion échoue.
    """
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            dbname=DB_CONFIG["database"],
        )
        conn.autocommit = False
        logger.info(f"Connexion PostgreSQL établie ({DB_CONFIG['host']}:{DB_CONFIG['port']})")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Connexion PostgreSQL impossible : {e}")
        raise


def creer_tables_stream(conn: psycopg2.extensions.connection) -> None:
    """
    Crée les 3 tables de stream si elles n'existent pas encore.

    Args:
        conn: Connexion psycopg2 active.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS fait_commandes_stream (
        id          SERIAL PRIMARY KEY,
        order_id    VARCHAR(50),
        customer_id VARCHAR(50),
        statut      VARCHAR(50),
        received_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS fait_paiements_stream (
        id             SERIAL PRIMARY KEY,
        order_id       VARCHAR(50),
        payment_type   VARCHAR(30),
        payment_value  NUMERIC(12, 2),
        installments   SMALLINT,
        received_at    TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS fait_user_events (
        id          SERIAL PRIMARY KEY,
        user_id     VARCHAR(50),
        action      VARCHAR(50),
        product_id  VARCHAR(50),
        session_id  VARCHAR(50),
        device      VARCHAR(30),
        event_time  TIMESTAMP,
        received_at TIMESTAMP DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Tables de stream vérifiées/créées.")


# =============================================================================
# INSERTIONS PAR TOPIC
# =============================================================================

def inserer_commande(cur, message: dict) -> None:
    """Insère un message du topic orders-stream dans fait_commandes_stream."""
    cur.execute(
        """
        INSERT INTO fait_commandes_stream (order_id, customer_id, statut, received_at)
        VALUES (%s, %s, %s, %s)
        """,
        (
            message.get("order_id"),
            message.get("customer_id"),
            message.get("statut"),
            datetime.now(),
        ),
    )


def inserer_paiement(cur, message: dict) -> None:
    """Insère un message du topic payments-stream dans fait_paiements_stream."""
    cur.execute(
        """
        INSERT INTO fait_paiements_stream (order_id, payment_type, payment_value, installments, received_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            message.get("order_id"),
            message.get("payment_type"),
            message.get("payment_value"),
            message.get("installments"),
            datetime.now(),
        ),
    )


def inserer_user_event(cur, message: dict) -> None:
    """Insère un message du topic user-events dans fait_user_events."""
    ts = message.get("timestamp")
    event_time = None
    if ts:
        try:
            event_time = datetime.fromisoformat(ts)
        except ValueError:
            event_time = None

    cur.execute(
        """
        INSERT INTO fait_user_events (user_id, action, product_id, session_id, device, event_time, received_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            message.get("user_id"),
            message.get("action"),
            message.get("product_id"),
            message.get("session_id"),
            message.get("device"),
            event_time,
            datetime.now(),
        ),
    )


# =============================================================================
# ROUTEUR DE MESSAGES
# =============================================================================

HANDLERS = {
    KAFKA_CONFIG["topics"]["orders"]:      inserer_commande,
    KAFKA_CONFIG["topics"]["payments"]:    inserer_paiement,
    KAFKA_CONFIG["topics"]["user_events"]: inserer_user_event,
}


# =============================================================================
# BOUCLE PRINCIPALE DU CONSOMMATEUR
# =============================================================================

def consommer() -> None:
    """
    S'abonne aux 3 topics Kafka et insère chaque message dans PostgreSQL.
    Tourne indéfiniment jusqu'à Ctrl+C.
    """
    logger.info(f"Abonnement aux topics : {TOPICS}")

    conn = connecter_postgresql()
    creer_tables_stream(conn)

    conf = {
        "bootstrap.servers":  KAFKA_CONFIG["bootstrap_servers"],
        "group.id":           "ecommerce-consumer-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)
    compteurs = {topic: 0 for topic in TOPICS}

    try:
        consumer.subscribe(TOPICS)
        logger.info("Consommateur Kafka démarré. En attente de messages... (Ctrl+C pour arrêter)")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                # _PARTITION_EOF est informatif, pas une vraie erreur
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Erreur Kafka : {msg.error()}")
                raise KafkaException(msg.error())

            topic = msg.topic()
            try:
                donnees = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Message invalide sur '{topic}' : {e}")
                continue

            handler = HANDLERS.get(topic)
            if handler is None:
                logger.warning(f"Aucun handler pour le topic '{topic}'")
                continue

            try:
                with conn.cursor() as cur:
                    handler(cur, donnees)
                conn.commit()
                compteurs[topic] = compteurs.get(topic, 0) + 1

                total = sum(compteurs.values())
                if total % LOG_INTERVAL == 0:
                    logger.info(
                        f"Messages insérés → "
                        f"orders: {compteurs.get(KAFKA_CONFIG['topics']['orders'], 0):,} | "
                        f"payments: {compteurs.get(KAFKA_CONFIG['topics']['payments'], 0):,} | "
                        f"user-events: {compteurs.get(KAFKA_CONFIG['topics']['user_events'], 0):,}"
                    )

            except Exception as e:
                logger.error(f"Erreur insertion ({topic}) : {e}")
                conn.rollback()

    except KeyboardInterrupt:
        logger.info("\nArrêt demandé par l'utilisateur.")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consommateur et connexion PostgreSQL fermés.")


if __name__ == "__main__":
    consommer()
