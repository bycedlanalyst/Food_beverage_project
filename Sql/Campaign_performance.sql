-- ============================================================
-- FICHIER     : campaign_performance.sql
-- PROJET      : AnyCompany Food & Beverage - Lab Snowflake
-- PHASE       : 2.3 - Marketing et Performance Commerciale
-- DESCRIPTION : Analyses de l efficacite des campagnes marketing
--               et de leur impact sur les ventes.
-- ANALYSES :
--   1. Lien campagnes <-> ventes (avec vs sans campagne)
--   2. Classement des campagnes par ROI
--   3. Performance par type de canal
--   4. Performance par region
--   5. Performance par audience cible
--   6. Performance par categorie produit
--   7. Evolution mensuelle du ROI
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- ============================================================
-- 1. LIEN CAMPAGNES <-> VENTES
-- Objectif : Mesurer l impact reel des campagnes sur le CA
--            en comparant les ventes avec et sans campagne active
-- ============================================================
SELECT
    CASE
        WHEN c.CAMPAIGN_ID IS NOT NULL THEN 'AVEC CAMPAGNE'
        ELSE 'SANS CAMPAGNE'
    END                                             AS PERIODE,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                         AS PANIER_MOYEN
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
-- Jointure sur region ET periode pour identifier si une campagne etait active
LEFT JOIN SILVER.MARKETING_CAMPAIGNS_CLEAN c
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY PERIODE
ORDER BY TOTAL_VENTES DESC;


-- ============================================================
-- 2. CLASSEMENT DES CAMPAGNES PAR ROI
-- Objectif : Identifier les campagnes les plus efficaces
--            en mesurant le retour sur investissement reel
-- ROI = (CA genere - Budget) / Budget * 100
-- ============================================================
SELECT
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,
    c.CAMPAIGN_TYPE,
    c.PRODUCT_CATEGORY,
    c.REGION,
    c.TARGET_AUDIENCE,
    ROUND(c.BUDGET, 2)                                              AS BUDGET,
    c.REACH,
    ROUND(c.CONVERSION_RATE * 100, 2)                              AS CONVERSION_PCT,
    -- CA reel genere pendant la periode et la region de la campagne
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS CA_GENERE,
    -- ROI = (CA genere - Budget investi) / Budget investi * 100
    ROUND(
        (SUM(t.AMOUNT) - c.BUDGET) / NULLIF(c.BUDGET, 0) * 100, 2
    )                                                               AS ROI_PCT,
    -- Cout par acquisition estime (budget / nombre de conversions estimees)
    CASE
        WHEN (c.REACH * c.CONVERSION_RATE) > 0
        THEN ROUND(c.BUDGET / (c.REACH * c.CONVERSION_RATE), 2)
        ELSE NULL
    END                                                             AS COST_PER_ACQUISITION
FROM SILVER.MARKETING_CAMPAIGNS_CLEAN c
-- Jointure avec les transactions pour calculer le CA reel genere
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
GROUP BY
    c.CAMPAIGN_ID, c.CAMPAIGN_NAME, c.CAMPAIGN_TYPE, c.PRODUCT_CATEGORY,
    c.REGION, c.TARGET_AUDIENCE, c.BUDGET, c.REACH, c.CONVERSION_RATE
ORDER BY ROI_PCT DESC
LIMIT 20;


-- ============================================================
-- 3. PERFORMANCE MOYENNE PAR TYPE DE CANAL
-- Objectif : Determiner quels canaux (Email, Print, etc.)
--            generent le meilleur retour sur investissement
-- ============================================================
SELECT
    c.CAMPAIGN_TYPE,
    COUNT(DISTINCT c.CAMPAIGN_ID)                                   AS NB_CAMPAGNES,
    ROUND(SUM(c.BUDGET), 2)                                         AS TOTAL_BUDGET,
    ROUND(AVG(c.CONVERSION_RATE) * 100, 2)                         AS AVG_CONVERSION_PCT,
    ROUND(AVG(c.REACH), 0)                                          AS AVG_REACH,
    -- CA reel genere par ce type de canal
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_CA,
    -- Ratio CA / Budget : combien de CA genere pour 1 dollar investi
    ROUND(SUM(t.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 2)             AS RATIO_CA_BUDGET
FROM SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
GROUP BY c.CAMPAIGN_TYPE
ORDER BY RATIO_CA_BUDGET DESC;


-- ============================================================
-- 4. CORRELATION ENTRE BUDGET ET REACH PAR REGION
-- Objectif : Verifier si l investissement est bien reparti
--            geographiquement et identifier les regions sous-investies
-- ============================================================
SELECT
    c.REGION,
    COUNT(DISTINCT c.CAMPAIGN_ID)                                   AS NB_CAMPAGNES,
    ROUND(SUM(c.BUDGET), 2)                                         AS TOTAL_BUDGET,
    ROUND(SUM(c.REACH), 0)                                          AS TOTAL_REACH,
    ROUND(AVG(c.CONVERSION_RATE) * 100, 2)                         AS AVG_CONVERSION_PCT,
    -- CA reel genere dans cette region pendant les campagnes
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_CA,
    ROUND(SUM(t.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 2)             AS RATIO_CA_BUDGET
FROM SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
GROUP BY c.REGION
ORDER BY TOTAL_BUDGET DESC;


-- ============================================================
-- 5. ANALYSE DU CIBLAGE PAR AUDIENCE
-- Objectif : Identifier quel segment de population repond
--            le mieux aux campagnes en termes de CA genere
-- ============================================================
SELECT
    c.TARGET_AUDIENCE,
    COUNT(DISTINCT c.CAMPAIGN_ID)                                   AS NB_CAMPAGNES,
    ROUND(AVG(c.CONVERSION_RATE) * 100, 2)                         AS AVG_CONVERSION_PCT,
    ROUND(AVG(c.REACH), 0)                                          AS AVG_REACH,
    -- CA reel genere par audience
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_CA,
    ROUND(SUM(t.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 2)             AS RATIO_CA_BUDGET
FROM SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
GROUP BY c.TARGET_AUDIENCE
ORDER BY TOTAL_CA DESC;


-- ============================================================
-- 6. PERFORMANCE PAR CATEGORIE PRODUIT
-- Objectif : Identifier quelles categories beneficient le plus
--            des investissements marketing
-- ============================================================
SELECT
    c.PRODUCT_CATEGORY,
    COUNT(DISTINCT c.CAMPAIGN_ID)                                   AS NB_CAMPAGNES,
    ROUND(SUM(c.BUDGET), 2)                                         AS TOTAL_BUDGET,
    ROUND(AVG(c.CONVERSION_RATE) * 100, 2)                         AS AVG_CONVERSION_PCT,
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_CA,
    ROUND(SUM(t.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 2)             AS RATIO_CA_BUDGET
FROM SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
    AND t.TRANSACTION_TYPE = 'SALE'
GROUP BY c.PRODUCT_CATEGORY
ORDER BY TOTAL_CA DESC;


-- ============================================================
-- 7. EVOLUTION MENSUELLE DU ROI DES CAMPAGNES
-- Objectif : Suivre l evolution du ROI dans le temps
--            pour identifier les periodes les plus performantes
-- ============================================================
SELECT
    YEAR(t.TRANSACTION_DATE)                                        AS ANNEE,
    MONTH(t.TRANSACTION_DATE)                                       AS MOIS,
    COUNT(DISTINCT c.CAMPAIGN_ID)                                   AS NB_CAMPAGNES_ACTIVES,
    ROUND(SUM(c.BUDGET), 2)                                         AS TOTAL_BUDGET,
    COUNT(t.TRANSACTION_ID)                                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                                         AS TOTAL_CA,
    -- ROI mensuel
    ROUND(
        (SUM(t.AMOUNT) - SUM(c.BUDGET)) / NULLIF(SUM(c.BUDGET), 0) * 100, 2
    )                                                               AS ROI_PCT
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
LEFT JOIN SILVER.MARKETING_CAMPAIGNS_CLEAN c
    ON  t.REGION = c.REGION
    AND t.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY ANNEE, MOIS
ORDER BY ANNEE, MOIS;
