# E-Commerce BI Platform

Projet de Master Big Data & Intelligence Artificielle.
Pipeline complet de traitement de données e-commerce : ingestion CSV/API → streaming Kafka → ETL → Data Warehouse PostgreSQL → Machine Learning → Power BI.

---

## Architecture du projet

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE DE DONNÉES                           │
│   9 CSV Olist (Brazil E-Commerce)  +  API Open Exchange Rates   │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │         ETL PIPELINE        │
          │  extract.py → transform.py  │
          │       → load.py             │
          └──────────────┬──────────────┘
                         │
          ┌──────────────▼──────────────┐
          │    DATA WAREHOUSE           │
          │    PostgreSQL (ecommerce_dw)│
          │                             │
          │  ┌─────────────────────┐   │
          │  │  STAR SCHEMA        │   │
          │  │                     │   │
          │  │  dim_client         │   │
          │  │  dim_produit        │   │
          │  │  dim_vendeur   ──► fait_commandes │
          │  │  dim_temps          │   │
          │  │  dim_localisation   │   │
          │  └─────────────────────┘   │
          └──────────────┬──────────────┘
                         │
     ┌───────────────────┼──────────────────────┐
     │                   │                      │
┌────▼────┐    ┌─────────▼──────────┐   ┌──────▼──────┐
│  KAFKA  │    │   MACHINE LEARNING │   │  POWER BI   │
│ Stream  │    │  churn_prediction  │   │  Dashboard  │
│         │    │  Random Forest     │   │             │
│ topics: │    │  → churn_pred.csv  │   │  Connexion  │
│ orders  │    └────────────────────┘   │  PostgreSQL │
│ payments│                             └─────────────┘
│ events  │
└─────────┘
```

---

## Prérequis

| Outil | Version | Usage |
|-------|---------|-------|
| Python | 3.11+ | Scripts ETL, ML, Kafka |
| PostgreSQL | 14+ | Data Warehouse |
| Apache Kafka | 3.x | Streaming temps réel |
| Power BI Desktop | Dernière | Visualisation |
| Java JDK | 11+ | Requis par Kafka |

---

## Installation pas à pas

### 1. Cloner le projet et installer les dépendances Python

```bash
git clone <url-du-repo>
cd ecommerce-bi-platform

# Créer un environnement virtuel (recommandé)
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -r requirements.txt
```

### 2. Configurer PostgreSQL

```sql
-- Dans psql ou pgAdmin
CREATE DATABASE ecommerce_dw;
```

Vérifier que `config.py` contient vos paramètres de connexion :
```python
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres123",
    "database": "ecommerce_dw",
}
```

### 3. Créer le schéma en étoile

```bash
psql -U postgres -d ecommerce_dw -f warehouse/create_tables.sql
```

### 4. Placer les CSV Olist dans le dossier `data/`

Les fichiers suivants doivent être présents dans `data/` :
- `olist_orders_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

> Télécharger depuis Kaggle : https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## Lancement des composants

### A. Pipeline ETL (batch)

```bash
python etl/run_etl.py
```

Cela exécute en séquence :
1. **Extract** : lecture des 9 CSV + résumé par fichier
2. **Transform** : nettoyage, construction du Star Schema, conversion de devises
3. **Load** : insertion dans PostgreSQL (6 tables)

### B. Streaming Kafka

**Étape 1 — Démarrer Kafka (une seule fois)**
```bat
kafka\start_kafka.bat
```
Ce script démarre Zookeeper, puis Kafka, et crée automatiquement les 3 topics.

**Étape 2 — Lancer le consommateur** (terminal 1)
```bash
python kafka/consumer.py
```

**Étape 3 — Lancer les producteurs** (3 terminaux séparés)
```bash
# Terminal 2 : commandes (20 msg/sec)
python kafka/producer_orders.py

# Terminal 3 : paiements (50 msg/sec)
python kafka/producer_payments.py

# Terminal 4 : événements utilisateurs (100 events/sec, continu)
python kafka/producer_user_events.py
```

### C. Machine Learning

```bash
jupyter notebook ml/churn_prediction.ipynb
```

Le notebook exécute toutes les cellules dans l'ordre et génère :
- `ml/churn_predictions.csv` : prédictions pour tous les clients
- `ml/churn_distribution.png`
- `ml/confusion_matrix_roc.png`
- `ml/feature_importance.png`

### D. Power BI

1. Ouvrir Power BI Desktop
2. `Obtenir les données` → `Base de données PostgreSQL`
3. Serveur : `localhost`, Base : `ecommerce_dw`
4. Importer les tables : `fait_commandes`, `dim_*`
5. Créer les relations selon le Star Schema
6. Construire les visuels analytiques

---

## Structure des dossiers

```
ecommerce-bi-platform/
│
├── config.py                    # Configuration centralisée
├── requirements.txt             # Dépendances Python
├── .gitignore
├── README.md
│
├── data/                        # CSV Olist (non versionnés)
│   └── *.csv
│
├── warehouse/
│   └── create_tables.sql        # Création du Star Schema PostgreSQL
│
├── etl/
│   ├── extract.py               # Lecture des 9 CSV
│   ├── transform.py             # Nettoyage, jointures, taux de change
│   ├── load.py                  # Insertion dans PostgreSQL
│   └── run_etl.py               # Pipeline ETL orchestrateur
│
├── kafka/
│   ├── start_kafka.bat          # Démarrage Kafka + création topics
│   ├── producer_orders.py       # Producteur orders-stream
│   ├── producer_payments.py     # Producteur payments-stream
│   ├── producer_user_events.py  # Producteur user-events (Faker)
│   └── consumer.py              # Consommateur multi-topics → PostgreSQL
│
├── ml/
│   ├── churn_prediction.ipynb   # Notebook ML complet
│   └── churn_predictions.csv    # Export des prédictions (généré)
│
├── powerbi/                     # Fichiers .pbix (non versionnés)
└── docs/                        # Documentation complémentaire
```

---

## Topics Kafka

| Topic | Partitions | Producteur | Débit |
|-------|-----------|-----------|-------|
| `orders-stream` | 3 | `producer_orders.py` | 20 msg/sec |
| `payments-stream` | 3 | `producer_payments.py` | 50 msg/sec |
| `user-events` | 5 | `producer_user_events.py` | 100 events/sec |

---

## Tables PostgreSQL

| Table | Type | Lignes (approx.) |
|-------|------|-----------------|
| `fait_commandes` | Faits | ~99 000 |
| `dim_client` | Dimension | ~99 000 |
| `dim_produit` | Dimension | ~33 000 |
| `dim_vendeur` | Dimension | ~3 000 |
| `dim_temps` | Dimension | ~1 000 |
| `dim_localisation` | Dimension | ~19 000 |
| `fait_commandes_stream` | Stream | variable |
| `fait_paiements_stream` | Stream | variable |
| `fait_user_events` | Stream | variable |

---

## Modèle ML — Prédiction Churn

- **Algorithme** : Random Forest (200 arbres, class_weight='balanced')
- **Features** : Récence, Fréquence, Montant total/moyen, Satisfaction, Durée client, Taux livraison
- **Label** : churn = 1 si dernier achat > 180 jours
- **Output** : `client_id`, `probabilite_churn`, `classification` (high/medium/low)

---



## Captures d'écran
<img width="855" height="497" alt="image" src="https://github.com/user-attachments/assets/ed54f938-04e2-4e0f-95ff-8dd5592d4f03" />
<img width="831" height="492" alt="image" src="https://github.com/user-attachments/assets/f835f16e-a583-47e1-948e-cc670434e29e" />
<img width="831" height="459" alt="image" src="https://github.com/user-attachments/assets/50af1c73-7db5-4b1a-a5cd-232a942cd021" />
<img width="852" height="498" alt="image" src="https://github.com/user-attachments/assets/55f8aff5-7b78-4e7a-af2a-954b2477976e" />



