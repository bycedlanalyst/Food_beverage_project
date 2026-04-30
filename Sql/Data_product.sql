-- ============================================================
-- FICHIER     : data_product.sql
-- PROJET      : AnyCompany Food & Beverage - Lab Snowflake
-- PHASE       : 3.1 - Creation du Data Product
-- DESCRIPTION : Creation des tables analytiques dans le schema
--               ANALYTICS. Ces tables combinent plusieurs sources
--               SILVER et sont prets a etre consommes par des
--               outils analytiques et des modeles ML.
-- TABLES CREEES :
--   1. VENTES_ENRICHIES   - transactions + promotions + campagnes + logistique
--   2. PROMOTIONS_ACTIVES - promotions avec KPIs de performance
--   3. CLIENTS_ENRICHIS   - clients avec KPIs service client
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;

-- ============================================================
-- CREATION DU SCHEMA ANALYTICS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS ANYCOMPANY_LAB.ANALYTICS
    COMMENT = 'Data Product - Tables analytiques pretes a consommer';

USE SCHEMA ANALYTICS;


-- ============================================================
-- TABLE 1 : VENTES_ENRICHIES
-- Granularite : une ligne par transaction de vente
-- Jointures   : transactions + promotions + campagnes + logistique
-- Cle         : TRANSACTION_ID
-- Usage       : analyses de ventes, ROI marketing, impact promos
-- ============================================================
CREATE OR REPLACE TABLE ANALYTICS.VENTES_ENRICHIES AS
SELECT
    -- Dimensions temporelles
    t.TRANSACTION_ID,
    t.TRANSACTION_DATE,
    YEAR(t.TRANSACTION_DATE)                                        AS ANNEE,
    MONTH(t.TRANSACTION_DATE)                                       AS MOIS,
    DAYOFWEEK(t.TRANSACTION_DATE)                                   AS JOUR_SEMAINE,

    -- Dimensions transaction
    t.TRANSACTION_TYPE,
    t.AMOUNT,
    t.PAYMENT_METHOD,
    t.ENTITY,
    t.REGION,
    t.ACCOUNT_CODE,

    -- Dimensions promotion
    -- Jointure sur region ET periode pour trouver la promo active
    p.PROMOTION_ID,
    p.PROMOTION_TYPE,
    p.PRODUCT_CATEGORY                                              AS PROMO_PRODUCT_CATEGORY,
    p.DISCOUNT_PERCENTAGE,
    -- Indicateur booleen : la transaction a-t-elle eu lieu pendant une promo ?
    CASE WHEN p.PROMOTION_ID IS NOT NULL THEN TRUE ELSE FALSE END   AS IS_PROMO,

    -- Dimensions campagne marketing
    -- Jointure sur region ET periode pour trouver la campagne active
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,
    c.CAMPAIGN_TYPE,
    c.TARGET_AUDIENCE,
    c.BUDGET                                                        AS CAMPAIGN_BUDGET,
    c.REACH                                                         AS CAMPAIGN_REACH,
    c.CONVERSION_RATE                                               AS CAMPAIGN_CONVERSION_RATE,
    -- Indicateur booleen : la transaction a-t-elle eu lieu pendant une campagne ?
    CASE WHEN c.CAMPAIGN_ID IS NOT NULL THEN TRUE ELSE FALSE END    AS IS_CAMPAIGN,

    -- Dimensions logistique
    -- Jointure sur region de destination ET date d expedition
    l.SHIPMENT_ID,
    l.SHIPPING_METHOD,
    l.STATUS                                                        AS SHIPPING_STATUS,
    l.SHIPPING_COST,
    l.CARRIER,
    l.DESTINATION_REGION,
    DATEDIFF('day', l.SHIP_DATE, l.ESTIMATED_DELIVERY)             AS ESTIMATED_LEAD_DAYS

FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
-- Jointure promotions : region identique ET transaction dans la periode de promo
LEFT JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
-- Jointure campagnes : region identique ET transaction dans la periode de campagne
LEFT JOIN SILVER.MARKETING_CAMPAIGNS_CLEAN c
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
-- Jointure logistique : region de destination identique ET meme date
LEFT JOIN SILVER.LOGISTICS_CLEAN l
    ON  t.REGION = l.DESTINATION_REGION
    AND t.TRANSACTION_DATE = l.SHIP_DATE
-- On ne garde que les transactions de type vente
WHERE t.TRANSACTION_TYPE = 'SALE';


-- ============================================================
-- TABLE 2 : PROMOTIONS_ACTIVES
-- Granularite : une ligne par promotion avec ses KPIs
-- Jointures   : promotions + ventes + avis produits
-- Cle         : PROMOTION_ID
-- Usage       : analyse de l efficacite des promotions
-- ============================================================
CREATE OR REPLACE TABLE ANALYTICS.PROMOTIONS_ACTIVES AS
SELECT
    -- Dimensions promotion
    p.PROMOTION_ID,
    p.PROMOTION_TYPE,
    p.PRODUCT_CATEGORY,
    p.REGION,
    p.START_DATE,
    p.END_DATE,
    -- Conversion du taux en pourcentage pour la lisibilite (ex: 0.15 -> 15%)
    ROUND(p.DISCOUNT_PERCENTAGE * 100, 2)                           AS DISCOUNT_PCT,
    DATEDIFF('day', p.START_DATE, p.END_DATE)                      AS DURATION_DAYS,

    -- KPIs ventes generees pendant la periode de promotion
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                                         AS PANIER_MOYEN,

    -- KPIs avis produits pendant la periode de promotion
    -- Jointure sur categorie produit ET date dans la periode de promo
    ROUND(AVG(r.RATING_3), 2)                                       AS AVG_RATING,
    COUNT(r.REVIEW_ID)                                              AS NB_AVIS,

    -- Indicateur : la promotion est-elle encore active aujourd hui ?
    CASE
        WHEN CURRENT_DATE() BETWEEN p.START_DATE AND p.END_DATE
        THEN TRUE ELSE FALSE
    END                                                             AS IS_ACTIVE

FROM SILVER.PROMOTIONS_CLEAN p
-- Jointure ventes : region identique ET transaction dans la periode de promo
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
-- Jointure avis : categorie identique ET avis dans la periode de promo
LEFT JOIN SILVER.PRODUCT_REVIEWS_CLEAN r
    ON  r.PRODUCT_CATEGORY = p.PRODUCT_CATEGORY
    AND r.REVIEW_DATE BETWEEN p.START_DATE AND p.END_DATE
GROUP BY
    p.PROMOTION_ID, p.PROMOTION_TYPE, p.PRODUCT_CATEGORY,
    p.REGION, p.START_DATE, p.END_DATE, p.DISCOUNT_PERCENTAGE;


-- ============================================================
-- TABLE 3 : CLIENTS_ENRICHIS
-- Granularite : une ligne par client avec ses KPIs
-- Jointures   : demographics + service client
-- Cle         : CUSTOMER_ID
-- Usage       : segmentation clients, scoring satisfaction
-- CORRECTION  : La jointure precedente utilisait d.REGION = s.ISSUE_CATEGORY
--               ce qui etait incorrect. ISSUE_CATEGORY est une categorie de
--               probleme (ex: 'Complaints'), pas une region geographique.
--               La table CUSTOMER_SERVICE_INTERACTIONS ne contient pas de
--               CUSTOMER_ID donc on ne peut pas faire de jointure directe.
--               On garde les KPIs de service client agreges par region.
-- ============================================================
CREATE OR REPLACE TABLE ANALYTICS.CLIENTS_ENRICHIS AS
SELECT
    -- Dimensions client
    d.CUSTOMER_ID,
    d.NAME,
    d.DATE_OF_BIRTH,
    DATEDIFF('year', d.DATE_OF_BIRTH, CURRENT_DATE())               AS AGE,
    -- Segmentation par tranche d age pour analyses demographiques
    CASE
        WHEN DATEDIFF('year', d.DATE_OF_BIRTH, CURRENT_DATE()) < 30  THEN '< 30 ans'
        WHEN DATEDIFF('year', d.DATE_OF_BIRTH, CURRENT_DATE()) BETWEEN 30 AND 44 THEN '30-44 ans'
        WHEN DATEDIFF('year', d.DATE_OF_BIRTH, CURRENT_DATE()) BETWEEN 45 AND 59 THEN '45-59 ans'
        ELSE '60 ans et +'
    END                                                             AS TRANCHE_AGE,
    d.GENDER,
    d.REGION,
    d.COUNTRY,
    d.CITY,
    d.MARITAL_STATUS,
    d.ANNUAL_INCOME,
    -- Segmentation par tranche de revenu pour analyses socio-economiques
    CASE
        WHEN d.ANNUAL_INCOME < 50000                        THEN '< 50K'
        WHEN d.ANNUAL_INCOME BETWEEN 50000 AND 100000       THEN '50K-100K'
        WHEN d.ANNUAL_INCOME BETWEEN 100001 AND 150000      THEN '100K-150K'
        ELSE '> 150K'
    END                                                             AS TRANCHE_REVENU,

    -- KPIs service client agreges par region du client
    -- On joint sur la region car pas de CUSTOMER_ID dans les interactions
    s.NB_INTERACTIONS,
    s.AVG_SATISFACTION,
    s.NB_RESOLVED,
    s.NB_ESCALATED,
    s.NB_FOLLOW_UP,

    -- Segmentation satisfaction basee sur la note moyenne regionale
    CASE
        WHEN s.AVG_SATISFACTION >= 4 THEN 'SATISFAIT'
        WHEN s.AVG_SATISFACTION >= 3 THEN 'NEUTRE'
        WHEN s.AVG_SATISFACTION IS NOT NULL THEN 'INSATISFAIT'
        ELSE 'INCONNU'
    END                                                             AS SEGMENT_SATISFACTION

FROM SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN d
-- Jointure avec les KPIs service client agreges par region
-- Sous-requete pour eviter la multiplication des lignes clients
LEFT JOIN (
    SELECT
        REGION,
        COUNT(DISTINCT INTERACTION_ID)                              AS NB_INTERACTIONS,
        ROUND(AVG(CUSTOMER_SATISFACTION), 2)                       AS AVG_SATISFACTION,
        SUM(CASE WHEN RESOLUTION_STATUS = 'RESOLVED'  THEN 1 ELSE 0 END) AS NB_RESOLVED,
        SUM(CASE WHEN RESOLUTION_STATUS = 'ESCALATED' THEN 1 ELSE 0 END) AS NB_ESCALATED,
        SUM(CASE WHEN FOLLOW_UP_REQUIRED = 'YES'       THEN 1 ELSE 0 END) AS NB_FOLLOW_UP
    FROM SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
    -- ISSUE_CATEGORY en majuscule correspond a REGION en majuscule
    -- On groupe par region pour obtenir des KPIs au niveau regional
    GROUP BY REGION
) s ON d.REGION = s.REGION;


-- ============================================================
-- VERIFICATIONS DES TABLES ANALYTIQUES
-- ============================================================

-- Volumes des tables creees
SELECT 'VENTES_ENRICHIES'   AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM ANALYTICS.VENTES_ENRICHIES   UNION ALL
SELECT 'PROMOTIONS_ACTIVES' AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM ANALYTICS.PROMOTIONS_ACTIVES UNION ALL
SELECT 'CLIENTS_ENRICHIS'   AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM ANALYTICS.CLIENTS_ENRICHIS;

-- Verification du taux de transactions avec promotion
SELECT
    IS_PROMO,
    COUNT(*) AS NB_TRANSACTIONS,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) AS PCT
FROM ANALYTICS.VENTES_ENRICHIES
GROUP BY IS_PROMO;

-- Verification du taux de transactions avec campagne
SELECT
    IS_CAMPAIGN,
    COUNT(*) AS NB_TRANSACTIONS,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) AS PCT
FROM ANALYTICS.VENTES_ENRICHIES
GROUP BY IS_CAMPAIGN;

-- Apercu des tables
SELECT * FROM ANALYTICS.VENTES_ENRICHIES   LIMIT 5;
SELECT * FROM ANALYTICS.PROMOTIONS_ACTIVES LIMIT 5;
SELECT * FROM ANALYTICS.CLIENTS_ENRICHIS   LIMIT 20;
