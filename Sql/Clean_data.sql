-- ============================================================
-- FICHIER     : clean_data.sql
-- PROJET      : AnyCompany Food & Beverage - Lab Snowflake
-- PHASE       : 1 - Etape 5 : Data Cleaning BRONZE -> SILVER
-- DESCRIPTION : Creation des tables nettoyees dans le schema
--               SILVER a partir des donnees brutes du schema BRONZE.
-- REGLES APPLIQUEES :
--   1. Gestion des valeurs manquantes (exclusion des IDs nuls)
--   2. Suppression des doublons (QUALIFY ROW_NUMBER ou GROUP BY ALL)
--   3. Harmonisation des formats (dates, majuscules, espaces)
--   4. Typage correct (TRY_TO_DATE, TRY_TO_DOUBLE)
--   5. Valeurs positives pour les montants financiers
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- ============================================================
-- TABLE 1 : PRODUCT_REVIEWS_CLEAN
-- Source : BRONZE.PRODUCT_REVIEWS (colonne RAW_LINE)
-- ============================================================
-- Ce fichier est au format TSV (tabulation comme separateur).
-- Chaque champ est extrait avec SPLIT_PART(RAW_LINE, '\t', N).
-- La date est splittee en deux colonnes : REVIEW_DATE et REVIEW_TIME.

-- Verifications prealables sur la table BRONZE
SELECT COUNT(*) AS TOTAL FROM BRONZE.PRODUCT_REVIEWS;

-- Verification des valeurs nulles
SELECT 
    COUNT(*) AS TOTAL,
    SUM(CASE WHEN SPLIT_PART(RAW_LINE, '\t', 1) IS NULL THEN 1 ELSE 0 END) AS NULL_REVIEW_ID,
    SUM(CASE WHEN SPLIT_PART(RAW_LINE, '\t', 2) IS NULL THEN 1 ELSE 0 END) AS NULL_PRODUCT_ID,
    SUM(CASE WHEN SPLIT_PART(RAW_LINE, '\t', 8) IS NULL THEN 1 ELSE 0 END) AS NULL_REVIEW_DATE
FROM BRONZE.PRODUCT_REVIEWS;

-- Verification des doublons sur toute la ligne
SELECT RAW_LINE, COUNT(*) AS NB
FROM BRONZE.PRODUCT_REVIEWS
GROUP BY RAW_LINE
HAVING COUNT(*) > 1;

-- Creation de la table SILVER.PRODUCT_REVIEWS_CLEAN
-- Colonnes extraites par position (tabulation comme separateur)
-- REVIEW_DATE splittee en DATE + TIME pour une granularite fine
CREATE OR REPLACE TABLE SILVER.PRODUCT_REVIEWS_CLEAN AS
SELECT
    -- Identifiants extraits par position
    SPLIT_PART(RAW_LINE, '\t', 1)                               AS REVIEW_ID,
    SPLIT_PART(RAW_LINE, '\t', 2)                               AS PRODUCT_ID,
    SPLIT_PART(RAW_LINE, '\t', 3)                               AS REVIEWER_ID,
    SPLIT_PART(RAW_LINE, '\t', 4)                               AS REVIEWER_NAME,
    -- Ratings convertis en DOUBLE
    TRY_TO_DOUBLE(SPLIT_PART(RAW_LINE, '\t', 5))                AS RATING_1,
    TRY_TO_DOUBLE(SPLIT_PART(RAW_LINE, '\t', 6))                AS RATING_2,
    TRY_TO_DOUBLE(SPLIT_PART(RAW_LINE, '\t', 7))                AS RATING_3,
    -- Date splittee en DATE et TIME pour analyses temporelles
    TRY_TO_TIMESTAMP(SPLIT_PART(RAW_LINE, '\t', 8))::DATE       AS REVIEW_DATE,
    TRY_TO_TIMESTAMP(SPLIT_PART(RAW_LINE, '\t', 8))::TIME       AS REVIEW_TIME,
    -- Contenu textuel
    SPLIT_PART(RAW_LINE, '\t', 9)                               AS REVIEW_TITLE,
    SPLIT_PART(RAW_LINE, '\t', 10)                              AS REVIEW_TEXT,
    -- Categories produit
    UPPER(TRIM(SPLIT_PART(RAW_LINE, '\t', 11)))                 AS PRODUCT_SUBCATEGORY,
    UPPER(TRIM(SPLIT_PART(RAW_LINE, '\t', 12)))                 AS PRODUCT_CATEGORY,
    SPLIT_PART(RAW_LINE, '\t', 13)                              AS PRODUCT_DESCRIPTION
FROM BRONZE.PRODUCT_REVIEWS
WHERE SPLIT_PART(RAW_LINE, '\t', 1) IS NOT NULL;

-- Verification finale
SELECT * FROM SILVER.PRODUCT_REVIEWS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 2 : CUSTOMER_DEMOGRAPHICS_CLEAN
-- Source : BRONZE.CUSTOMER_DEMOGRAPHICS
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.CUSTOMER_DEMOGRAPHICS;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                    AS TOTAL,
    SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END)                                        AS NULL_CUSTOMER_ID,
    ROUND(SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)             AS PCT_CUSTOMER_ID,
    SUM(CASE WHEN NAME IS NULL THEN 1 ELSE 0 END)                                               AS NULL_NAME,
    ROUND(SUM(CASE WHEN NAME IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                    AS PCT_NAME,
    SUM(CASE WHEN DATE_OF_BIRTH IS NULL THEN 1 ELSE 0 END)                                      AS NULL_DATE_OF_BIRTH,
    ROUND(SUM(CASE WHEN DATE_OF_BIRTH IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)           AS PCT_DATE_OF_BIRTH,
    SUM(CASE WHEN ANNUAL_INCOME IS NULL THEN 1 ELSE 0 END)                                      AS NULL_ANNUAL_INCOME,
    ROUND(SUM(CASE WHEN ANNUAL_INCOME IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)           AS PCT_ANNUAL_INCOME
FROM BRONZE.CUSTOMER_DEMOGRAPHICS;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.CUSTOMER_DEMOGRAPHICS
QUALIFY COUNT(*) OVER (
    PARTITION BY CUSTOMER_ID, NAME, DATE_OF_BIRTH, GENDER,
                 REGION, COUNTRY, CITY, MARITAL_STATUS, ANNUAL_INCOME
) > 1;

-- Distribution du revenu annuel
SELECT 
    MIN(TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')))     AS MIN_INCOME,
    MAX(TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')))     AS MAX_INCOME,
    AVG(TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')))     AS AVG_INCOME,
    SUM(CASE WHEN TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS,
    SUM(CASE WHEN TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')) IS NULL THEN 1 ELSE 0 END) AS NB_NULLS
FROM BRONZE.CUSTOMER_DEMOGRAPHICS;

-- Creation de la table SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
CREATE OR REPLACE TABLE SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN AS
SELECT
    CUSTOMER_ID,
    UPPER(TRIM(NAME))                                           AS NAME,
    -- Conversion de la date de naissance en DATE
    TRY_TO_DATE(DATE_OF_BIRTH, 'YYYY-MM-DD')                    AS DATE_OF_BIRTH,
    UPPER(TRIM(GENDER))                                         AS GENDER,
    UPPER(TRIM(REGION))                                         AS REGION,
    UPPER(TRIM(COUNTRY))                                        AS COUNTRY,
    UPPER(TRIM(CITY))                                           AS CITY,
    UPPER(TRIM(MARITAL_STATUS))                                 AS MARITAL_STATUS,
    -- Nettoyage du revenu : suppression des espaces -> DOUBLE
    TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', ''))              AS ANNUAL_INCOME
FROM BRONZE.CUSTOMER_DEMOGRAPHICS
WHERE
    -- Exclusion des lignes sans ID (cle primaire)
    CUSTOMER_ID IS NOT NULL
    -- Regle qualite : revenus positifs uniquement
    AND TRY_TO_DOUBLE(REPLACE(ANNUAL_INCOME, ' ', '')) > 0;

-- Verification finale
SELECT * FROM SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 3 : CUSTOMER_SERVICE_INTERACTIONS_CLEAN
-- Source : BRONZE.CUSTOMER_SERVICE_INTERACTIONS
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN INTERACTION_ID IS NULL THEN 1 ELSE 0 END)                                        AS NULL_INTERACTION_ID,
    ROUND(SUM(CASE WHEN INTERACTION_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)             AS PCT_INTERACTION_ID,
    SUM(CASE WHEN INTERACTION_DATE IS NULL THEN 1 ELSE 0 END)                                      AS NULL_INTERACTION_DATE,
    ROUND(SUM(CASE WHEN INTERACTION_DATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)           AS PCT_INTERACTION_DATE,
    SUM(CASE WHEN INTERACTION_TYPE IS NULL THEN 1 ELSE 0 END)                                      AS NULL_INTERACTION_TYPE,
    ROUND(SUM(CASE WHEN INTERACTION_TYPE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)           AS PCT_INTERACTION_TYPE,
    SUM(CASE WHEN ISSUE_CATEGORY IS NULL THEN 1 ELSE 0 END)                                        AS NULL_ISSUE_CATEGORY,
    ROUND(SUM(CASE WHEN ISSUE_CATEGORY IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)             AS PCT_ISSUE_CATEGORY,
    SUM(CASE WHEN DURATION_MINUTES IS NULL THEN 1 ELSE 0 END)                                      AS NULL_DURATION_MINUTES,
    ROUND(SUM(CASE WHEN DURATION_MINUTES IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)           AS PCT_DURATION_MINUTES,
    SUM(CASE WHEN CUSTOMER_SATISFACTION IS NULL THEN 1 ELSE 0 END)                                 AS NULL_CUSTOMER_SATISFACTION,
    ROUND(SUM(CASE WHEN CUSTOMER_SATISFACTION IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)      AS PCT_CUSTOMER_SATISFACTION
FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS
QUALIFY COUNT(*) OVER (
    PARTITION BY INTERACTION_ID, INTERACTION_DATE, INTERACTION_TYPE,
                 ISSUE_CATEGORY, DESCRIPTION, DURATION_MINUTES,
                 RESOLUTION_STATUS, FOLLOW_UP_REQUIRED, CUSTOMER_SATISFACTION
) > 1
ORDER BY INTERACTION_ID;

-- Distribution des valeurs numeriques (cast en DOUBLE pour eviter tri alphabetique)
SELECT 
    MIN(TRY_TO_DOUBLE(DURATION_MINUTES))        AS MIN_DURATION,
    MAX(TRY_TO_DOUBLE(DURATION_MINUTES))        AS MAX_DURATION,
    MIN(TRY_TO_DOUBLE(CUSTOMER_SATISFACTION))   AS MIN_SATISFACTION,
    MAX(TRY_TO_DOUBLE(CUSTOMER_SATISFACTION))   AS MAX_SATISFACTION
FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS;

-- Creation de la table SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
CREATE OR REPLACE TABLE SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN AS
SELECT
    INTERACTION_ID,
    TRY_TO_DATE(INTERACTION_DATE, 'YYYY-MM-DD')             AS INTERACTION_DATE,
    UPPER(TRIM(INTERACTION_TYPE))                           AS INTERACTION_TYPE,
    UPPER(TRIM(ISSUE_CATEGORY))                             AS ISSUE_CATEGORY,
    UPPER(TRIM(DESCRIPTION))                                AS DESCRIPTION,
    TRY_TO_DOUBLE(DURATION_MINUTES)                         AS DURATION_MINUTES,
    UPPER(TRIM(RESOLUTION_STATUS))                          AS RESOLUTION_STATUS,
    UPPER(TRIM(FOLLOW_UP_REQUIRED))                         AS FOLLOW_UP_REQUIRED,
    TRY_TO_DOUBLE(CUSTOMER_SATISFACTION)                    AS CUSTOMER_SATISFACTION
FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS
WHERE
    INTERACTION_ID IS NOT NULL
    AND TRY_TO_DOUBLE(DURATION_MINUTES) > 0
    AND TRY_TO_DOUBLE(CUSTOMER_SATISFACTION) > 0;

-- Verification finale
SELECT * FROM SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 4 : FINANCIAL_TRANSACTIONS_CLEAN
-- Source : BRONZE.FINANCIAL_TRANSACTIONS
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.FINANCIAL_TRANSACTIONS;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                    AS TOTAL,
    SUM(CASE WHEN TRANSACTION_ID IS NULL THEN 1 ELSE 0 END)                                    AS NULL_TRANSACTION_ID,
    ROUND(SUM(CASE WHEN TRANSACTION_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)         AS PCT_TRANSACTION_ID,
    SUM(CASE WHEN TRANSACTION_DATE IS NULL THEN 1 ELSE 0 END)                                  AS NULL_TRANSACTION_DATE,
    ROUND(SUM(CASE WHEN TRANSACTION_DATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)       AS PCT_TRANSACTION_DATE,
    SUM(CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END)                                            AS NULL_AMOUNT,
    ROUND(SUM(CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                 AS PCT_AMOUNT
FROM BRONZE.FINANCIAL_TRANSACTIONS;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.FINANCIAL_TRANSACTIONS
QUALIFY COUNT(*) OVER (
    PARTITION BY TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TYPE,
                 AMOUNT, PAYMENT_METHOD, ENTITY, REGION, ACCOUNT_CODE
) > 1
ORDER BY TRANSACTION_ID;

-- Distribution des montants (cast en DOUBLE pour eviter tri alphabetique)
SELECT 
    MIN(TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', '')))    AS MIN_AMOUNT,
    MAX(TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', '')))    AS MAX_AMOUNT,
    AVG(TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', '')))    AS AVG_AMOUNT,
    SUM(CASE WHEN TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', '')) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.FINANCIAL_TRANSACTIONS;

-- Creation de la table SILVER.FINANCIAL_TRANSACTIONS_CLEAN
CREATE OR REPLACE TABLE SILVER.FINANCIAL_TRANSACTIONS_CLEAN AS
SELECT
    TRANSACTION_ID,
    TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD')                 AS TRANSACTION_DATE,
    UPPER(TRIM(TRANSACTION_TYPE))                               AS TRANSACTION_TYPE,
    -- Suppression des espaces dans le montant avant conversion
    TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', ''))                     AS AMOUNT,
    UPPER(TRIM(PAYMENT_METHOD))                                 AS PAYMENT_METHOD,
    UPPER(TRIM(ENTITY))                                         AS ENTITY,
    UPPER(TRIM(REGION))                                         AS REGION,
    UPPER(TRIM(ACCOUNT_CODE))                                   AS ACCOUNT_CODE
FROM BRONZE.FINANCIAL_TRANSACTIONS
WHERE
    TRANSACTION_ID IS NOT NULL
    -- Regle qualite : montants positifs uniquement
    AND TRY_TO_DOUBLE(REPLACE(AMOUNT, ' ', '')) > 0;

-- Verification finale
SELECT * FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 5 : PROMOTIONS_CLEAN
-- Source : BRONZE.PROMOTIONS_DATA
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.PROMOTIONS_DATA;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN PROMOTION_ID IS NULL THEN 1 ELSE 0 END)                                          AS NULL_PROMOTION_ID,
    ROUND(SUM(CASE WHEN PROMOTION_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)               AS PCT_PROMOTION_ID,
    SUM(CASE WHEN DISCOUNT_PERCENTAGE IS NULL THEN 1 ELSE 0 END)                                   AS NULL_DISCOUNT_PERCENTAGE,
    ROUND(SUM(CASE WHEN DISCOUNT_PERCENTAGE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)        AS PCT_DISCOUNT_PERCENTAGE,
    SUM(CASE WHEN START_DATE IS NULL THEN 1 ELSE 0 END)                                            AS NULL_START_DATE,
    ROUND(SUM(CASE WHEN START_DATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                 AS PCT_START_DATE,
    SUM(CASE WHEN END_DATE IS NULL THEN 1 ELSE 0 END)                                              AS NULL_END_DATE,
    ROUND(SUM(CASE WHEN END_DATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                   AS PCT_END_DATE
FROM BRONZE.PROMOTIONS_DATA;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.PROMOTIONS_DATA
QUALIFY COUNT(*) OVER (
    PARTITION BY PROMOTION_ID, PRODUCT_CATEGORY, PROMOTION_TYPE,
                 DISCOUNT_PERCENTAGE, START_DATE, END_DATE, REGION
) > 1
ORDER BY PROMOTION_ID;

-- Distribution du taux de remise (TRY_TO_DOUBLE pour les decimales)
SELECT 
    MIN(TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE))     AS MIN_DISCOUNT,
    MAX(TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE))     AS MAX_DISCOUNT,
    AVG(TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE))     AS AVG_DISCOUNT,
    SUM(CASE WHEN TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.PROMOTIONS_DATA;

-- Creation de la table SILVER.PROMOTIONS_CLEAN
CREATE OR REPLACE TABLE SILVER.PROMOTIONS_CLEAN AS
SELECT
    PROMOTION_ID,
    UPPER(TRIM(PRODUCT_CATEGORY))                               AS PRODUCT_CATEGORY,
    UPPER(TRIM(PROMOTION_TYPE))                                 AS PROMOTION_TYPE,
    -- TRY_TO_DOUBLE car les taux de remise sont decimaux (ex: 0.15)
    TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE)                          AS DISCOUNT_PERCENTAGE,
    TRY_TO_DATE(START_DATE, 'YYYY-MM-DD')                       AS START_DATE,
    TRY_TO_DATE(END_DATE, 'YYYY-MM-DD')                         AS END_DATE,
    UPPER(TRIM(REGION))                                         AS REGION
FROM BRONZE.PROMOTIONS_DATA
WHERE
    PROMOTION_ID IS NOT NULL
    AND TRY_TO_DOUBLE(DISCOUNT_PERCENTAGE) > 0
    -- Coherence des dates : start doit etre avant end
    AND TRY_TO_DATE(START_DATE, 'YYYY-MM-DD') <= TRY_TO_DATE(END_DATE, 'YYYY-MM-DD');

-- Verification finale
SELECT * FROM SILVER.PROMOTIONS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 6 : MARKETING_CAMPAIGNS_CLEAN
-- Source : BRONZE.MARKETING_CAMPAIGNS
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.MARKETING_CAMPAIGNS;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN CAMPAIGN_ID IS NULL THEN 1 ELSE 0 END)                                           AS NULL_CAMPAIGN_ID,
    ROUND(SUM(CASE WHEN CAMPAIGN_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                AS PCT_CAMPAIGN_ID,
    SUM(CASE WHEN BUDGET IS NULL THEN 1 ELSE 0 END)                                                AS NULL_BUDGET,
    ROUND(SUM(CASE WHEN BUDGET IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                     AS PCT_BUDGET,
    SUM(CASE WHEN CONVERSION_RATE IS NULL THEN 1 ELSE 0 END)                                       AS NULL_CONVERSION_RATE,
    ROUND(SUM(CASE WHEN CONVERSION_RATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)            AS PCT_CONVERSION_RATE
FROM BRONZE.MARKETING_CAMPAIGNS;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.MARKETING_CAMPAIGNS
QUALIFY COUNT(*) OVER (
    PARTITION BY CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE,
                 PRODUCT_CATEGORY, TARGET_AUDIENCE, START_DATE,
                 END_DATE, REGION, BUDGET, REACH, CONVERSION_RATE
) > 1
ORDER BY CAMPAIGN_ID;

-- Distribution des valeurs numeriques
SELECT 
    MIN(TRY_TO_DOUBLE(REPLACE(BUDGET, ' ', '')))        AS MIN_BUDGET,
    MAX(TRY_TO_DOUBLE(REPLACE(BUDGET, ' ', '')))        AS MAX_BUDGET,
    MIN(TRY_TO_DOUBLE(CONVERSION_RATE))                 AS MIN_CONVERSION_RATE,
    MAX(TRY_TO_DOUBLE(CONVERSION_RATE))                 AS MAX_CONVERSION_RATE,
    SUM(CASE WHEN TRY_TO_DOUBLE(REPLACE(BUDGET, ' ', '')) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.MARKETING_CAMPAIGNS;

-- Creation de la table SILVER.MARKETING_CAMPAIGNS_CLEAN
CREATE OR REPLACE TABLE SILVER.MARKETING_CAMPAIGNS_CLEAN AS
SELECT
    CAMPAIGN_ID,
    UPPER(TRIM(CAMPAIGN_NAME))                                  AS CAMPAIGN_NAME,
    UPPER(TRIM(CAMPAIGN_TYPE))                                  AS CAMPAIGN_TYPE,
    UPPER(TRIM(PRODUCT_CATEGORY))                               AS PRODUCT_CATEGORY,
    UPPER(TRIM(TARGET_AUDIENCE))                                AS TARGET_AUDIENCE,
    TRY_TO_DATE(START_DATE, 'YYYY-MM-DD')                       AS START_DATE,
    TRY_TO_DATE(END_DATE, 'YYYY-MM-DD')                         AS END_DATE,
    UPPER(TRIM(REGION))                                         AS REGION,
    -- Suppression des espaces dans les montants avant conversion
    TRY_TO_DOUBLE(REPLACE(BUDGET, ' ', ''))                     AS BUDGET,
    TRY_TO_DOUBLE(REPLACE(REACH, ' ', ''))                      AS REACH,
    -- Taux de conversion en DOUBLE (valeur decimale ex: 0.0614)
    TRY_TO_DOUBLE(CONVERSION_RATE)                              AS CONVERSION_RATE
FROM BRONZE.MARKETING_CAMPAIGNS
WHERE
    CAMPAIGN_ID IS NOT NULL
    AND TRY_TO_DOUBLE(REPLACE(BUDGET, ' ', '')) > 0
    AND TRY_TO_DATE(START_DATE, 'YYYY-MM-DD') <= TRY_TO_DATE(END_DATE, 'YYYY-MM-DD');

-- Verification finale
SELECT * FROM SILVER.MARKETING_CAMPAIGNS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 7 : LOGISTICS_CLEAN
-- Source : BRONZE.LOGISTICS_AND_SHIPPING
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.LOGISTICS_AND_SHIPPING;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN SHIPMENT_ID IS NULL THEN 1 ELSE 0 END)                                           AS NULL_SHIPMENT_ID,
    ROUND(SUM(CASE WHEN SHIPMENT_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                AS PCT_SHIPMENT_ID,
    SUM(CASE WHEN SHIPPING_COST IS NULL THEN 1 ELSE 0 END)                                         AS NULL_SHIPPING_COST,
    ROUND(SUM(CASE WHEN SHIPPING_COST IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)              AS PCT_SHIPPING_COST,
    SUM(CASE WHEN SHIP_DATE IS NULL THEN 1 ELSE 0 END)                                             AS NULL_SHIP_DATE,
    ROUND(SUM(CASE WHEN SHIP_DATE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                  AS PCT_SHIP_DATE
FROM BRONZE.LOGISTICS_AND_SHIPPING;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.LOGISTICS_AND_SHIPPING
QUALIFY COUNT(*) OVER (
    PARTITION BY SHIPMENT_ID, ORDER_ID, SHIP_DATE, ESTIMATED_DELIVERY,
                 SHIPPING_METHOD, STATUS, SHIPPING_COST,
                 DESTINATION_REGION, DESTINATION_COUNTRY, CARRIER
) > 1
ORDER BY SHIPMENT_ID;

-- Distribution des couts d expedition
SELECT 
    MIN(TRY_TO_DOUBLE(TRIM(SHIPPING_COST)))     AS MIN_SHIPPING_COST,
    MAX(TRY_TO_DOUBLE(TRIM(SHIPPING_COST)))     AS MAX_SHIPPING_COST,
    AVG(TRY_TO_DOUBLE(TRIM(SHIPPING_COST)))     AS AVG_SHIPPING_COST,
    SUM(CASE WHEN TRY_TO_DOUBLE(TRIM(SHIPPING_COST)) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.LOGISTICS_AND_SHIPPING;

-- Creation de la table SILVER.LOGISTICS_CLEAN
CREATE OR REPLACE TABLE SILVER.LOGISTICS_CLEAN AS
SELECT
    SHIPMENT_ID,
    ORDER_ID,
    TRY_TO_DATE(SHIP_DATE, 'YYYY-MM-DD')                        AS SHIP_DATE,
    TRY_TO_DATE(ESTIMATED_DELIVERY, 'YYYY-MM-DD')               AS ESTIMATED_DELIVERY,
    UPPER(TRIM(SHIPPING_METHOD))                                AS SHIPPING_METHOD,
    UPPER(TRIM(STATUS))                                         AS STATUS,
    TRY_TO_DOUBLE(TRIM(SHIPPING_COST))                          AS SHIPPING_COST,
    UPPER(TRIM(DESTINATION_REGION))                             AS DESTINATION_REGION,
    UPPER(TRIM(DESTINATION_COUNTRY))                            AS DESTINATION_COUNTRY,
    UPPER(TRIM(CARRIER))                                        AS CARRIER
FROM BRONZE.LOGISTICS_AND_SHIPPING
WHERE
    SHIPMENT_ID IS NOT NULL
    AND TRY_TO_DOUBLE(TRIM(SHIPPING_COST)) > 0
    -- Coherence des dates : expedition avant livraison
    AND TRY_TO_DATE(SHIP_DATE, 'YYYY-MM-DD') <= TRY_TO_DATE(ESTIMATED_DELIVERY, 'YYYY-MM-DD');

-- Verification finale
SELECT * FROM SILVER.LOGISTICS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 8 : SUPPLIER_INFORMATION_CLEAN
-- Source : BRONZE.SUPPLIER_INFORMATION
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.SUPPLIER_INFORMATION;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN SUPPLIER_ID IS NULL THEN 1 ELSE 0 END)                                           AS NULL_SUPPLIER_ID,
    ROUND(SUM(CASE WHEN SUPPLIER_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                AS PCT_SUPPLIER_ID,
    SUM(CASE WHEN RELIABILITY_SCORE IS NULL THEN 1 ELSE 0 END)                                     AS NULL_RELIABILITY_SCORE,
    ROUND(SUM(CASE WHEN RELIABILITY_SCORE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)          AS PCT_RELIABILITY_SCORE
FROM BRONZE.SUPPLIER_INFORMATION;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.SUPPLIER_INFORMATION
QUALIFY COUNT(*) OVER (
    PARTITION BY SUPPLIER_ID, SUPPLIER_NAME, PRODUCT_CATEGORY,
                 REGION, COUNTRY, CITY, LEAD_TIME,
                 RELIABILITY_SCORE, QUALITY_RATING
) > 1
ORDER BY SUPPLIER_ID;

-- Distribution des scores (TRY_TO_DOUBLE car valeurs decimales entre 0 et 1)
SELECT 
    MIN(TRY_TO_DOUBLE(RELIABILITY_SCORE))   AS MIN_RELIABILITY,
    MAX(TRY_TO_DOUBLE(RELIABILITY_SCORE))   AS MAX_RELIABILITY,
    MIN(TRY_TO_DOUBLE(LEAD_TIME))           AS MIN_LEAD_TIME,
    MAX(TRY_TO_DOUBLE(LEAD_TIME))           AS MAX_LEAD_TIME
FROM BRONZE.SUPPLIER_INFORMATION;

-- Creation de la table SILVER.SUPPLIER_INFORMATION_CLEAN
CREATE OR REPLACE TABLE SILVER.SUPPLIER_INFORMATION_CLEAN AS
SELECT
    SUPPLIER_ID,
    UPPER(TRIM(SUPPLIER_NAME))                                  AS SUPPLIER_NAME,
    UPPER(TRIM(PRODUCT_CATEGORY))                               AS PRODUCT_CATEGORY,
    UPPER(TRIM(REGION))                                         AS REGION,
    UPPER(TRIM(COUNTRY))                                        AS COUNTRY,
    UPPER(TRIM(CITY))                                           AS CITY,
    TRY_TO_DOUBLE(LEAD_TIME)                                    AS LEAD_TIME,
    -- DOUBLE car les scores sont decimaux (ex: 0.86)
    TRY_TO_DOUBLE(RELIABILITY_SCORE)                            AS RELIABILITY_SCORE,
    UPPER(TRIM(QUALITY_RATING))                                 AS QUALITY_RATING
FROM BRONZE.SUPPLIER_INFORMATION
WHERE
    SUPPLIER_ID IS NOT NULL
    AND TRY_TO_DOUBLE(LEAD_TIME) > 0
    AND TRY_TO_DOUBLE(RELIABILITY_SCORE) > 0;

-- Verification finale
SELECT * FROM SILVER.SUPPLIER_INFORMATION_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 9 : EMPLOYEE_RECORDS_CLEAN
-- Source : BRONZE.EMPLOYEE_RECORDS
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.EMPLOYEE_RECORDS;

-- Valeurs manquantes + pourcentage
SELECT 
    COUNT(*)                                                                                        AS TOTAL,
    SUM(CASE WHEN EMPLOYEE_ID IS NULL THEN 1 ELSE 0 END)                                           AS NULL_EMPLOYEE_ID,
    ROUND(SUM(CASE WHEN EMPLOYEE_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                AS PCT_EMPLOYEE_ID,
    SUM(CASE WHEN SALARY IS NULL THEN 1 ELSE 0 END)                                                AS NULL_SALARY,
    ROUND(SUM(CASE WHEN SALARY IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)                     AS PCT_SALARY
FROM BRONZE.EMPLOYEE_RECORDS;

-- Doublons sur toute la ligne
SELECT * FROM BRONZE.EMPLOYEE_RECORDS
QUALIFY COUNT(*) OVER (
    PARTITION BY EMPLOYEE_ID, NAME, DATE_OF_BIRTH, HIRE_DATE,
                 DEPARTMENT, JOB_TITLE, SALARY, REGION, COUNTRY, EMAIL
) > 1
ORDER BY EMPLOYEE_ID;

-- Distribution des salaires
SELECT 
    MIN(TRY_TO_DOUBLE(REPLACE(SALARY, ' ', '')))    AS MIN_SALARY,
    MAX(TRY_TO_DOUBLE(REPLACE(SALARY, ' ', '')))    AS MAX_SALARY,
    AVG(TRY_TO_DOUBLE(REPLACE(SALARY, ' ', '')))    AS AVG_SALARY,
    SUM(CASE WHEN TRY_TO_DOUBLE(REPLACE(SALARY, ' ', '')) < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.EMPLOYEE_RECORDS;

-- Creation de la table SILVER.EMPLOYEE_RECORDS_CLEAN
CREATE OR REPLACE TABLE SILVER.EMPLOYEE_RECORDS_CLEAN AS
SELECT
    EMPLOYEE_ID,
    UPPER(TRIM(NAME))                                           AS NAME,
    TRY_TO_DATE(DATE_OF_BIRTH, 'YYYY-MM-DD')                    AS DATE_OF_BIRTH,
    TRY_TO_DATE(HIRE_DATE, 'YYYY-MM-DD')                        AS HIRE_DATE,
    UPPER(TRIM(DEPARTMENT))                                     AS DEPARTMENT,
    UPPER(TRIM(JOB_TITLE))                                      AS JOB_TITLE,
    -- Suppression des espaces dans les salaires avant conversion
    TRY_TO_DOUBLE(REPLACE(SALARY, ' ', ''))                     AS SALARY,
    UPPER(TRIM(REGION))                                         AS REGION,
    UPPER(TRIM(COUNTRY))                                        AS COUNTRY,
    -- Email en minuscule (convention standard)
    LOWER(TRIM(EMAIL))                                          AS EMAIL
FROM BRONZE.EMPLOYEE_RECORDS
WHERE
    EMPLOYEE_ID IS NOT NULL
    AND TRY_TO_DOUBLE(REPLACE(SALARY, ' ', '')) > 0;

-- Verification finale
SELECT * FROM SILVER.EMPLOYEE_RECORDS_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 10 : INVENTORY_CLEAN
-- Source : BRONZE.INVENTORY (colonne RAW_DATA VARIANT)
-- ============================================================
-- Pour les fichiers JSON on extrait chaque champ avec la
-- notation RAW_DATA:"nom_du_champ"::TYPE

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.INVENTORY;
SELECT RAW_DATA FROM BRONZE.INVENTORY LIMIT 5;

-- Valeurs manquantes
SELECT
    COUNT(*)                                                                                            AS TOTAL,
    SUM(CASE WHEN RAW_DATA:"product_id"::VARCHAR IS NULL THEN 1 ELSE 0 END)                            AS NULL_PRODUCT_ID,
    ROUND(SUM(CASE WHEN RAW_DATA:"product_id"::VARCHAR IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS PCT_PRODUCT_ID,
    SUM(CASE WHEN RAW_DATA:"current_stock"::DOUBLE IS NULL THEN 1 ELSE 0 END)                          AS NULL_CURRENT_STOCK,
    ROUND(SUM(CASE WHEN RAW_DATA:"current_stock"::DOUBLE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS PCT_CURRENT_STOCK
FROM BRONZE.INVENTORY;

-- Distribution des stocks
SELECT
    MIN(RAW_DATA:"current_stock"::DOUBLE)       AS MIN_STOCK,
    MAX(RAW_DATA:"current_stock"::DOUBLE)       AS MAX_STOCK,
    MIN(RAW_DATA:"reorder_point"::DOUBLE)       AS MIN_REORDER,
    MAX(RAW_DATA:"reorder_point"::DOUBLE)       AS MAX_REORDER,
    SUM(CASE WHEN RAW_DATA:"current_stock"::DOUBLE < 0 THEN 1 ELSE 0 END) AS NB_NEGATIFS
FROM BRONZE.INVENTORY;

-- Doublons sur toute la ligne
SELECT RAW_DATA FROM BRONZE.INVENTORY
QUALIFY COUNT(*) OVER (PARTITION BY RAW_DATA) > 1;

-- Creation de la table SILVER.INVENTORY_CLEAN
CREATE OR REPLACE TABLE SILVER.INVENTORY_CLEAN AS
SELECT
    RAW_DATA:"product_id"::VARCHAR                                          AS PRODUCT_ID,
    UPPER(TRIM(RAW_DATA:"product_category"::VARCHAR))                       AS PRODUCT_CATEGORY,
    UPPER(TRIM(RAW_DATA:"region"::VARCHAR))                                 AS REGION,
    UPPER(TRIM(RAW_DATA:"country"::VARCHAR))                                AS COUNTRY,
    UPPER(TRIM(RAW_DATA:"warehouse"::VARCHAR))                              AS WAREHOUSE,
    RAW_DATA:"current_stock"::DOUBLE                                        AS CURRENT_STOCK,
    RAW_DATA:"reorder_point"::DOUBLE                                        AS REORDER_POINT,
    RAW_DATA:"lead_time"::DOUBLE                                            AS LEAD_TIME,
    TRY_TO_DATE(RAW_DATA:"last_restock_date"::VARCHAR, 'YYYY-MM-DD')        AS LAST_RESTOCK_DATE,
    -- Indicateur de reapprovisionnement necessaire
    -- TRUE si le stock actuel est inferieur ou egal au seuil de recommande
    CASE
        WHEN RAW_DATA:"current_stock"::DOUBLE <= RAW_DATA:"reorder_point"::DOUBLE
        THEN TRUE ELSE FALSE
    END                                                                     AS REORDER_NEEDED
FROM BRONZE.INVENTORY
WHERE RAW_DATA:"product_id"::VARCHAR IS NOT NULL
AND RAW_DATA:"current_stock"::DOUBLE >= 0;

-- Verification finale
SELECT * FROM SILVER.INVENTORY_CLEAN LIMIT 10;


-- ============================================================
-- TABLE 11 : STORE_LOCATIONS_CLEAN
-- Source : BRONZE.STORE_LOCATIONS (colonne RAW_DATA VARIANT)
-- ============================================================

-- Verifications prealables
SELECT COUNT(*) AS TOTAL FROM BRONZE.STORE_LOCATIONS;
SELECT RAW_DATA FROM BRONZE.STORE_LOCATIONS LIMIT 5;

-- Valeurs manquantes
SELECT
    COUNT(*)                                                                                                AS TOTAL,
    SUM(CASE WHEN RAW_DATA:"store_id"::VARCHAR IS NULL THEN 1 ELSE 0 END)                                  AS NULL_STORE_ID,
    ROUND(SUM(CASE WHEN RAW_DATA:"store_id"::VARCHAR IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)       AS PCT_STORE_ID,
    SUM(CASE WHEN RAW_DATA:"square_footage"::DOUBLE IS NULL THEN 1 ELSE 0 END)                             AS NULL_SQUARE_FOOTAGE,
    ROUND(SUM(CASE WHEN RAW_DATA:"square_footage"::DOUBLE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)  AS PCT_SQUARE_FOOTAGE,
    SUM(CASE WHEN RAW_DATA:"employee_count"::DOUBLE IS NULL THEN 1 ELSE 0 END)                             AS NULL_EMPLOYEE_COUNT,
    ROUND(SUM(CASE WHEN RAW_DATA:"employee_count"::DOUBLE IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)  AS PCT_EMPLOYEE_COUNT
FROM BRONZE.STORE_LOCATIONS;

-- Distribution des valeurs numeriques
SELECT
    MIN(RAW_DATA:"square_footage"::DOUBLE)      AS MIN_SQUARE_FOOTAGE,
    MAX(RAW_DATA:"square_footage"::DOUBLE)      AS MAX_SQUARE_FOOTAGE,
    MIN(RAW_DATA:"employee_count"::DOUBLE)      AS MIN_EMPLOYEE_COUNT,
    MAX(RAW_DATA:"employee_count"::DOUBLE)      AS MAX_EMPLOYEE_COUNT
FROM BRONZE.STORE_LOCATIONS;

-- Doublons sur toute la ligne
SELECT RAW_DATA FROM BRONZE.STORE_LOCATIONS
QUALIFY COUNT(*) OVER (PARTITION BY RAW_DATA) > 1;

-- Creation de la table SILVER.STORE_LOCATIONS_CLEAN
CREATE OR REPLACE TABLE SILVER.STORE_LOCATIONS_CLEAN AS
SELECT
    RAW_DATA:"store_id"::VARCHAR                                AS STORE_ID,
    UPPER(TRIM(RAW_DATA:"store_name"::VARCHAR))                 AS STORE_NAME,
    UPPER(TRIM(RAW_DATA:"store_type"::VARCHAR))                 AS STORE_TYPE,
    UPPER(TRIM(RAW_DATA:"region"::VARCHAR))                     AS REGION,
    UPPER(TRIM(RAW_DATA:"country"::VARCHAR))                    AS COUNTRY,
    UPPER(TRIM(RAW_DATA:"city"::VARCHAR))                       AS CITY,
    UPPER(TRIM(RAW_DATA:"address"::VARCHAR))                    AS ADDRESS,
    RAW_DATA:"postal_code"::VARCHAR                             AS POSTAL_CODE,
    RAW_DATA:"square_footage"::DOUBLE                           AS SQUARE_FOOTAGE,
    RAW_DATA:"employee_count"::DOUBLE                           AS EMPLOYEE_COUNT
FROM BRONZE.STORE_LOCATIONS
WHERE
    RAW_DATA:"store_id"::VARCHAR IS NOT NULL
    AND RAW_DATA:"square_footage"::DOUBLE > 0
    AND RAW_DATA:"employee_count"::DOUBLE > 0;

-- Verification finale
SELECT * FROM SILVER.STORE_LOCATIONS_CLEAN LIMIT 10;


-- ============================================================
-- BILAN FINAL : COMPARAISON VOLUMES BRONZE vs SILVER
-- ============================================================
-- Permet de verifier le taux de retention des donnees
-- apres nettoyage (% de lignes conservees)

SELECT 'BRONZE' AS LAYER, 'CUSTOMER_DEMOGRAPHICS'         AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.CUSTOMER_DEMOGRAPHICS         UNION ALL
SELECT 'SILVER' AS LAYER, 'CUSTOMER_DEMOGRAPHICS_CLEAN'   AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN   UNION ALL
SELECT 'BRONZE' AS LAYER, 'CUSTOMER_SERVICE_INTERACTIONS' AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS  UNION ALL
SELECT 'SILVER' AS LAYER, 'CUSTOMER_SERVICE_INTERACTIONS_CLEAN' AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN UNION ALL
SELECT 'BRONZE' AS LAYER, 'FINANCIAL_TRANSACTIONS'        AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.FINANCIAL_TRANSACTIONS        UNION ALL
SELECT 'SILVER' AS LAYER, 'FINANCIAL_TRANSACTIONS_CLEAN'  AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN  UNION ALL
SELECT 'BRONZE' AS LAYER, 'PROMOTIONS_DATA'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.PROMOTIONS_DATA               UNION ALL
SELECT 'SILVER' AS LAYER, 'PROMOTIONS_CLEAN'              AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.PROMOTIONS_CLEAN              UNION ALL
SELECT 'BRONZE' AS LAYER, 'MARKETING_CAMPAIGNS'           AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.MARKETING_CAMPAIGNS           UNION ALL
SELECT 'SILVER' AS LAYER, 'MARKETING_CAMPAIGNS_CLEAN'     AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.MARKETING_CAMPAIGNS_CLEAN     UNION ALL
SELECT 'BRONZE' AS LAYER, 'LOGISTICS_AND_SHIPPING'        AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.LOGISTICS_AND_SHIPPING        UNION ALL
SELECT 'SILVER' AS LAYER, 'LOGISTICS_CLEAN'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.LOGISTICS_CLEAN               UNION ALL
SELECT 'BRONZE' AS LAYER, 'SUPPLIER_INFORMATION'          AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.SUPPLIER_INFORMATION          UNION ALL
SELECT 'SILVER' AS LAYER, 'SUPPLIER_INFORMATION_CLEAN'    AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.SUPPLIER_INFORMATION_CLEAN    UNION ALL
SELECT 'BRONZE' AS LAYER, 'EMPLOYEE_RECORDS'              AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.EMPLOYEE_RECORDS              UNION ALL
SELECT 'SILVER' AS LAYER, 'EMPLOYEE_RECORDS_CLEAN'        AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.EMPLOYEE_RECORDS_CLEAN        UNION ALL
SELECT 'BRONZE' AS LAYER, 'INVENTORY'                     AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.INVENTORY                     UNION ALL
SELECT 'SILVER' AS LAYER, 'INVENTORY_CLEAN'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.INVENTORY_CLEAN               UNION ALL
SELECT 'BRONZE' AS LAYER, 'STORE_LOCATIONS'               AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM BRONZE.STORE_LOCATIONS               UNION ALL
SELECT 'SILVER' AS LAYER, 'STORE_LOCATIONS_CLEAN'         AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM SILVER.STORE_LOCATIONS_CLEAN
ORDER BY TABLE_NAME, LAYER;
