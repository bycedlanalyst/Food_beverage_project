-- ============================================================
-- FICHIER     : Load_data.sql
-- PROJET      : AnyCompany Food & Beverage - Lab Snowflake
-- PHASE       : 1 - Preparation et Ingestion des donnees
-- DESCRIPTION : Preparation de l environnement Snowflake,
--               creation des tables BRONZE et chargement
--               des donnees depuis Amazon S3.
-- ORDRE D EXECUTION :
--   Etape 1 : Preparation de l environnement
--   Etape 2 : Creation des tables BRONZE
--   Etape 3 : Chargement des donnees (COPY INTO)
--   Etape 4 : Verifications et exploration
-- ============================================================


-- ============================================================
-- ETAPE 1 : PREPARATION DE L ENVIRONNEMENT SNOWFLAKE
-- ============================================================

-- 1.1 Creation de la base de donnees
CREATE OR REPLACE DATABASE ANYCOMPANY_LAB;
USE DATABASE ANYCOMPANY_LAB;

-- 1.2 Creation du schema BRONZE
-- BRONZE = donnees brutes, telles que recues depuis S3
-- Tout en VARCHAR ou VARIANT - aucun nettoyage a ce stade
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Donnees brutes chargees depuis S3';

-- 1.3 Creation du schema SILVER
-- SILVER = donnees nettoyees, typees et harmonisees
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Donnees nettoyees et exploitables';

USE SCHEMA BRONZE;

-- 1.4 Creation du Stage externe pointant vers S3
-- Un Stage est un pointeur vers un emplacement de fichiers exterieurs.
-- Les fichiers sont publics sur S3 donc pas de credentials necessaires.
CREATE OR REPLACE STAGE BRONZE.S3_FOOD_BEVERAGE_STAGE
    URL     = 's3://logbrain-datalake/datasets/food-beverage/'
    COMMENT = 'Stage pointant vers le bucket S3 des donnees Food & Beverage';

-- Verification : lister les fichiers disponibles sur le stage
LIST @BRONZE.S3_FOOD_BEVERAGE_STAGE;

-- Inspection d un fichier CSV pour confirmer le delimiteur
-- Si les colonnes sont bien separees -> delimiteur virgule confirme
SELECT $1, $2, $3
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/customer_demographics.csv
LIMIT 5;


-- 1.5 Creation du File Format CSV
-- Utilise pour les 9 fichiers CSV standards
-- FIELD_DELIMITER = ','  : virgule comme separateur
-- SKIP_HEADER = 1        : ignorer la ligne d en-tete
-- FIELD_OPTIONALLY_ENCLOSED_BY = '"' : gerer les valeurs entre guillemets
CREATE OR REPLACE FILE FORMAT BRONZE.FF_CSV_BRONZE
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = ','
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL          = TRUE
    COMMENT                      = 'File Format CSV pour les fichiers standards';

-- 1.6 Creation du File Format RAW LINE pour product_reviews
-- Ce fichier a un format non standard avec des textes libres contenant
-- des espaces et tabulations. On charge chaque ligne entiere dans une
-- seule colonne VARCHAR. Le parsing sera fait en SILVER.
-- FIELD_DELIMITER = '\x00' : caractere nul, jamais present dans le texte
-- donc Snowflake ne decoupera jamais la ligne.
CREATE OR REPLACE FILE FORMAT BRONZE.FF_RAW_LINE
    TYPE            = 'CSV'
    FIELD_DELIMITER = '\x00'
    COMMENT         = 'File Format pour charger chaque ligne brute sans decoupage';

-- 1.7 Creation du File Format JSON
-- STRIP_OUTER_ARRAY = TRUE : le fichier JSON est un tableau [{ },{ }]
-- Cette option decoupe en une ligne par objet { }
CREATE OR REPLACE FILE FORMAT BRONZE.FF_JSON_BRONZE
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT           = 'File Format pour les fichiers JSON';


-- ============================================================
-- ETAPE 2 : CREATION DES TABLES BRONZE
-- ============================================================
-- Une table par fichier source.
-- Tous les champs CSV sont en VARCHAR (pas de typage).
-- Les fichiers JSON sont stockes dans une colonne VARIANT.
-- Le typage sera applique dans le schema SILVER.

-- Table : CUSTOMER_DEMOGRAPHICS
-- Source : customer_demographics.csv
-- Contenu : Profil demographique de chaque client
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_DEMOGRAPHICS (
    CUSTOMER_ID     VARCHAR,
    NAME            VARCHAR,
    DATE_OF_BIRTH   VARCHAR,
    GENDER          VARCHAR,
    REGION          VARCHAR,
    COUNTRY         VARCHAR,
    CITY            VARCHAR,
    MARITAL_STATUS  VARCHAR,
    ANNUAL_INCOME   VARCHAR
);

-- Table : CUSTOMER_SERVICE_INTERACTIONS
-- Source : customer_service_interactions.csv
-- Contenu : Historique des contacts avec le service client
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_SERVICE_INTERACTIONS (
    INTERACTION_ID          VARCHAR,
    INTERACTION_DATE        VARCHAR,
    INTERACTION_TYPE        VARCHAR,
    ISSUE_CATEGORY          VARCHAR,
    DESCRIPTION             VARCHAR,
    DURATION_MINUTES        VARCHAR,
    RESOLUTION_STATUS       VARCHAR,
    FOLLOW_UP_REQUIRED      VARCHAR,
    CUSTOMER_SATISFACTION   VARCHAR
);

-- Table : FINANCIAL_TRANSACTIONS
-- Source : financial_transactions.csv
-- Contenu : Transactions financieres (ventes, remboursements, etc.)
CREATE OR REPLACE TABLE BRONZE.FINANCIAL_TRANSACTIONS (
    TRANSACTION_ID      VARCHAR,
    TRANSACTION_DATE    VARCHAR,
    TRANSACTION_TYPE    VARCHAR,
    AMOUNT              VARCHAR,
    PAYMENT_METHOD      VARCHAR,
    ENTITY              VARCHAR,
    REGION              VARCHAR,
    ACCOUNT_CODE        VARCHAR
);

-- Table : PROMOTIONS_DATA
-- Source : promotions-data.csv
-- Contenu : Promotions par categorie, type et region
CREATE OR REPLACE TABLE BRONZE.PROMOTIONS_DATA (
    PROMOTION_ID            VARCHAR,
    PRODUCT_CATEGORY        VARCHAR,
    PROMOTION_TYPE          VARCHAR,
    DISCOUNT_PERCENTAGE     VARCHAR,
    START_DATE              VARCHAR,
    END_DATE                VARCHAR,
    REGION                  VARCHAR
);

-- Table : MARKETING_CAMPAIGNS
-- Source : marketing_campaigns.csv
-- Contenu : Campagnes marketing avec budget, reach et taux de conversion
CREATE OR REPLACE TABLE BRONZE.MARKETING_CAMPAIGNS (
    CAMPAIGN_ID         VARCHAR,
    CAMPAIGN_NAME       VARCHAR,
    CAMPAIGN_TYPE       VARCHAR,
    PRODUCT_CATEGORY    VARCHAR,
    TARGET_AUDIENCE     VARCHAR,
    START_DATE          VARCHAR,
    END_DATE            VARCHAR,
    REGION              VARCHAR,
    BUDGET              VARCHAR,
    REACH               VARCHAR,
    CONVERSION_RATE     VARCHAR
);

-- Table : PRODUCT_REVIEWS
-- Source : product_reviews.csv (fichier a format non standard)
-- Contenu : Avis produits avec textes libres
-- Strategie : charger chaque ligne entiere dans RAW_LINE (VARCHAR)
-- Le parsing sera fait en SILVER avec SPLIT_PART sur tabulation \t
CREATE OR REPLACE TABLE BRONZE.PRODUCT_REVIEWS (
    RAW_LINE VARCHAR
);

-- Table : LOGISTICS_AND_SHIPPING
-- Source : logistics_and_shipping.csv
-- Contenu : Expeditions, livraisons et retours
CREATE OR REPLACE TABLE BRONZE.LOGISTICS_AND_SHIPPING (
    SHIPMENT_ID             VARCHAR,
    ORDER_ID                VARCHAR,
    SHIP_DATE               VARCHAR,
    ESTIMATED_DELIVERY      VARCHAR,
    SHIPPING_METHOD         VARCHAR,
    STATUS                  VARCHAR,
    SHIPPING_COST           VARCHAR,
    DESTINATION_REGION      VARCHAR,
    DESTINATION_COUNTRY     VARCHAR,
    CARRIER                 VARCHAR
);

-- Table : SUPPLIER_INFORMATION
-- Source : supplier_information.csv
-- Contenu : Fournisseurs avec scores de fiabilite et qualite
CREATE OR REPLACE TABLE BRONZE.SUPPLIER_INFORMATION (
    SUPPLIER_ID         VARCHAR,
    SUPPLIER_NAME       VARCHAR,
    PRODUCT_CATEGORY    VARCHAR,
    REGION              VARCHAR,
    COUNTRY             VARCHAR,
    CITY                VARCHAR,
    LEAD_TIME           VARCHAR,
    RELIABILITY_SCORE   VARCHAR,
    QUALITY_RATING      VARCHAR
);

-- Table : EMPLOYEE_RECORDS
-- Source : employee_records.csv
-- Contenu : Donnees organisationnelles des employes
CREATE OR REPLACE TABLE BRONZE.EMPLOYEE_RECORDS (
    EMPLOYEE_ID     VARCHAR,
    NAME            VARCHAR,
    DATE_OF_BIRTH   VARCHAR,
    HIRE_DATE       VARCHAR,
    DEPARTMENT      VARCHAR,
    JOB_TITLE       VARCHAR,
    SALARY          VARCHAR,
    REGION          VARCHAR,
    COUNTRY         VARCHAR,
    EMAIL           VARCHAR
);

-- Table : INVENTORY (JSON)
-- Source : inventory.json
-- Contenu : Niveaux de stock par produit et entrepot
-- Colonne VARIANT pour stocker le JSON brut
CREATE OR REPLACE TABLE BRONZE.INVENTORY (
    RAW_DATA VARIANT
);

-- Table : STORE_LOCATIONS (JSON)
-- Source : store_locations.json
-- Contenu : Informations geographiques des magasins
CREATE OR REPLACE TABLE BRONZE.STORE_LOCATIONS (
    RAW_DATA VARIANT
);


-- ============================================================
-- ETAPE 3 : CHARGEMENT DES DONNEES (COPY INTO)
-- ============================================================
-- COPY INTO lit les fichiers depuis le Stage S3 et les insere
-- dans les tables BRONZE en utilisant les File Formats definis.
-- On prefixe toujours par BRONZE. pour eviter les erreurs
-- liees au schema actif courant.

-- Chargement : customer_demographics
COPY INTO BRONZE.CUSTOMER_DEMOGRAPHICS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/customer_demographics.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : customer_service_interactions
COPY INTO BRONZE.CUSTOMER_SERVICE_INTERACTIONS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/customer_service_interactions.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : financial_transactions
COPY INTO BRONZE.FINANCIAL_TRANSACTIONS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/financial_transactions.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : promotions-data
COPY INTO BRONZE.PROMOTIONS_DATA
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/promotions-data.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : marketing_campaigns
COPY INTO BRONZE.MARKETING_CAMPAIGNS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/marketing_campaigns.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : product_reviews (format non standard - ligne brute)
COPY INTO BRONZE.PRODUCT_REVIEWS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/product_reviews.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_RAW_LINE);

-- Chargement : logistics_and_shipping
COPY INTO BRONZE.LOGISTICS_AND_SHIPPING
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/logistics_and_shipping.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : supplier_information
COPY INTO BRONZE.SUPPLIER_INFORMATION
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/supplier_information.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement : employee_records
COPY INTO BRONZE.EMPLOYEE_RECORDS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/employee_records.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV_BRONZE);

-- Chargement JSON : inventory
COPY INTO BRONZE.INVENTORY
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/inventory.json
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_JSON_BRONZE);

-- Chargement JSON : store_locations
COPY INTO BRONZE.STORE_LOCATIONS
FROM @BRONZE.S3_FOOD_BEVERAGE_STAGE/store_locations.json
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_JSON_BRONZE);


-- ============================================================
-- ETAPE 4 : VERIFICATIONS ET EXPLORATION
-- ============================================================

-- 4.1 Comptage de toutes les tables BRONZE
-- Permet de verifier que toutes les tables ont ete chargees
SELECT 'CUSTOMER_DEMOGRAPHICS'         AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.CUSTOMER_DEMOGRAPHICS         UNION ALL
SELECT 'CUSTOMER_SERVICE_INTERACTIONS' AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS UNION ALL
SELECT 'FINANCIAL_TRANSACTIONS'        AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.FINANCIAL_TRANSACTIONS        UNION ALL
SELECT 'PROMOTIONS_DATA'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.PROMOTIONS_DATA               UNION ALL
SELECT 'MARKETING_CAMPAIGNS'           AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.MARKETING_CAMPAIGNS           UNION ALL
SELECT 'PRODUCT_REVIEWS'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.PRODUCT_REVIEWS               UNION ALL
SELECT 'LOGISTICS_AND_SHIPPING'        AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.LOGISTICS_AND_SHIPPING        UNION ALL
SELECT 'SUPPLIER_INFORMATION'          AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.SUPPLIER_INFORMATION          UNION ALL
SELECT 'EMPLOYEE_RECORDS'              AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.EMPLOYEE_RECORDS              UNION ALL
SELECT 'INVENTORY'                     AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.INVENTORY                     UNION ALL
SELECT 'STORE_LOCATIONS'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.STORE_LOCATIONS
ORDER BY NB_ROWS DESC;

-- 4.2 Apercu des tables (executer separement)
SELECT * FROM BRONZE.CUSTOMER_DEMOGRAPHICS          LIMIT 10;
SELECT * FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS  LIMIT 10;
SELECT * FROM BRONZE.FINANCIAL_TRANSACTIONS         LIMIT 10;
SELECT * FROM BRONZE.PROMOTIONS_DATA                LIMIT 10;
SELECT * FROM BRONZE.MARKETING_CAMPAIGNS            LIMIT 10;
SELECT * FROM BRONZE.PRODUCT_REVIEWS                LIMIT 10;
SELECT * FROM BRONZE.LOGISTICS_AND_SHIPPING         LIMIT 10;
SELECT * FROM BRONZE.SUPPLIER_INFORMATION           LIMIT 10;
SELECT * FROM BRONZE.EMPLOYEE_RECORDS               LIMIT 10;
SELECT * FROM BRONZE.INVENTORY                      LIMIT 10;
SELECT * FROM BRONZE.STORE_LOCATIONS                LIMIT 10;
