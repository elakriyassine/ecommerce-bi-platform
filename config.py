# =============================================================================
# config.py - Configuration centralisée du projet E-Commerce BI Platform
# =============================================================================

import os

# Chemin racine du projet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data")

# Configuration PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres123",
    "database": "ecommerce_dw",
}

# URL SQLAlchemy pour la connexion PostgreSQL
DB_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Configuration Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topics": {
        "orders": "orders-stream",
        "payments": "payments-stream",
        "user_events": "user-events",
    },
}

# API externe - Taux de change (Open Exchange Rates, sans clé API)
API_URL = "https://open.er-api.com/v6/latest/USD"

# Noms des fichiers CSV Olist
CSV_FILES = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
