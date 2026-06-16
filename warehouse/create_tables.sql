-- =============================================================================
-- create_tables.sql - Star Schema pour l'entrepôt de données E-Commerce
-- Base de données : ecommerce_dw
-- =============================================================================

-- Nettoyage des tables existantes (ordre inverse des FK)
DROP TABLE IF EXISTS fait_commandes CASCADE;
DROP TABLE IF EXISTS dim_client CASCADE;
DROP TABLE IF EXISTS dim_produit CASCADE;
DROP TABLE IF EXISTS dim_vendeur CASCADE;
DROP TABLE IF EXISTS dim_temps CASCADE;
DROP TABLE IF EXISTS dim_localisation CASCADE;
DROP TABLE IF EXISTS fait_commandes_stream CASCADE;
DROP TABLE IF EXISTS fait_paiements_stream CASCADE;
DROP TABLE IF EXISTS fait_user_events CASCADE;

-- =============================================================================
-- TABLES DE DIMENSION
-- =============================================================================

-- Dimension Client
CREATE TABLE dim_client (
    client_id         VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    ville             VARCHAR(100),
    etat              CHAR(2),
    code_postal       VARCHAR(10)
);

-- Dimension Produit
CREATE TABLE dim_produit (
    produit_id    VARCHAR(50) PRIMARY KEY,
    categorie_pt  VARCHAR(100),
    categorie_en  VARCHAR(100),
    poids_g       NUMERIC(10, 2),
    longueur_cm   NUMERIC(8, 2),
    hauteur_cm    NUMERIC(8, 2),
    largeur_cm    NUMERIC(8, 2)
);

-- Dimension Vendeur
CREATE TABLE dim_vendeur (
    vendeur_id  VARCHAR(50) PRIMARY KEY,
    ville       VARCHAR(100),
    etat        CHAR(2),
    code_postal VARCHAR(10)
);

-- Dimension Temps
CREATE TABLE dim_temps (
    date_id       SERIAL PRIMARY KEY,
    date_complete DATE NOT NULL UNIQUE,
    jour          SMALLINT NOT NULL CHECK (jour BETWEEN 1 AND 31),
    mois          SMALLINT NOT NULL CHECK (mois BETWEEN 1 AND 12),
    trimestre     SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee         SMALLINT NOT NULL,
    jour_semaine  VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN NOT NULL DEFAULT FALSE
);

-- Dimension Localisation
CREATE TABLE dim_localisation (
    localisation_id SERIAL PRIMARY KEY,
    code_postal     VARCHAR(10) NOT NULL,
    ville           VARCHAR(100),
    etat            CHAR(2),
    lat             NUMERIC(10, 6),
    lng             NUMERIC(10, 6)
);

-- =============================================================================
-- TABLE DE FAITS
-- =============================================================================

CREATE TABLE fait_commandes (
    order_id         VARCHAR(50) PRIMARY KEY,
    client_id        VARCHAR(50)  REFERENCES dim_client(client_id),
    produit_id       VARCHAR(50)  REFERENCES dim_produit(produit_id),
    vendeur_id       VARCHAR(50)  REFERENCES dim_vendeur(vendeur_id),
    date_id          INTEGER      REFERENCES dim_temps(date_id),
    localisation_id  INTEGER      REFERENCES dim_localisation(localisation_id),
    montant_brl      NUMERIC(12, 2),
    montant_eur      NUMERIC(12, 4),
    montant_mad      NUMERIC(12, 4),
    frais_livraison  NUMERIC(10, 2),
    score_satisfaction SMALLINT   CHECK (score_satisfaction BETWEEN 1 AND 5),
    statut           VARCHAR(50),
    nb_versements    SMALLINT,
    type_paiement    VARCHAR(30)
);

-- =============================================================================
-- TABLES KAFKA STREAM (créées ici pour référence, aussi créées par consumer.py)
-- =============================================================================

CREATE TABLE fait_commandes_stream (
    id            SERIAL PRIMARY KEY,
    order_id      VARCHAR(50),
    customer_id   VARCHAR(50),
    statut        VARCHAR(50),
    received_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fait_paiements_stream (
    id               SERIAL PRIMARY KEY,
    order_id         VARCHAR(50),
    payment_type     VARCHAR(30),
    payment_value    NUMERIC(12, 2),
    installments     SMALLINT,
    received_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fait_user_events (
    id          SERIAL PRIMARY KEY,
    user_id     VARCHAR(50),
    action      VARCHAR(50),
    product_id  VARCHAR(50),
    session_id  VARCHAR(50),
    device      VARCHAR(30),
    event_time  TIMESTAMP,
    received_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- INDEX pour optimiser les jointures et les requêtes analytiques
-- =============================================================================

-- Index sur la table de faits (colonnes FK et métriques clés)
CREATE INDEX idx_fait_commandes_client     ON fait_commandes(client_id);
CREATE INDEX idx_fait_commandes_produit    ON fait_commandes(produit_id);
CREATE INDEX idx_fait_commandes_vendeur    ON fait_commandes(vendeur_id);
CREATE INDEX idx_fait_commandes_date       ON fait_commandes(date_id);
CREATE INDEX idx_fait_commandes_localisation ON fait_commandes(localisation_id);
CREATE INDEX idx_fait_commandes_statut     ON fait_commandes(statut);

-- Index sur les dimensions
CREATE INDEX idx_dim_client_unique_id  ON dim_client(customer_unique_id);
CREATE INDEX idx_dim_client_etat       ON dim_client(etat);
CREATE INDEX idx_dim_produit_categorie ON dim_produit(categorie_en);
CREATE INDEX idx_dim_vendeur_etat      ON dim_vendeur(etat);
CREATE INDEX idx_dim_temps_annee_mois  ON dim_temps(annee, mois);
CREATE INDEX idx_dim_localisation_cp   ON dim_localisation(code_postal);

-- Index sur les tables stream
CREATE INDEX idx_stream_orders_order_id    ON fait_commandes_stream(order_id);
CREATE INDEX idx_stream_payments_order_id  ON fait_paiements_stream(order_id);
CREATE INDEX idx_stream_events_user_id     ON fait_user_events(user_id);

-- =============================================================================
-- Confirmation
-- =============================================================================
SELECT 'Star Schema créé avec succès !' AS message,
       COUNT(*) AS nb_tables
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';
