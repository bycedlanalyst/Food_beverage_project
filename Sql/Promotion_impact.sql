-- ============================================================
-- FICHIER     : promotion_impact.sql
-- PROJET      : AnyCompany Food & Beverage - Lab Snowflake
-- PHASE       : 2.3 - Ventes et Promotions
-- DESCRIPTION : Analyses de l impact des promotions sur les ventes
--               et du comportement des clients en periode promotionnelle.
-- ANALYSES :
--   1. Comparaison ventes avec / sans promotion
--   2. Sensibilite des categories aux promotions
--   3. Impact du taux de remise sur le volume de ventes
--   4. Performance des promotions par region
--   5. Top promotions par CA genere
--   6. Evolution mensuelle des ventes pendant les promotions
--   7. Impact des avis produits sur les ventes
--   8. Impact des interactions service client sur les ventes
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- ============================================================
-- 1. COMPARAISON VENTES AVEC / SANS PROMOTION
-- Objectif : Mesurer l impact global des promotions sur le CA
--            en comparant les periodes avec et sans promotion active
-- ============================================================
SELECT
    CASE
        WHEN p.PROMOTION_ID IS NOT NULL THEN 'AVEC PROMOTION'
        ELSE 'SANS PROMOTION'
    END                                             AS PERIODE,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                         AS PANIER_MOYEN
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
-- Jointure sur region ET periode pour identifier si une promo etait active
LEFT JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY PERIODE
ORDER BY TOTAL_VENTES DESC;


-- ============================================================
-- 2. SENSIBILITE DES CATEGORIES AUX PROMOTIONS
-- Objectif : Identifier quelles categories de produits
--            beneficient le plus des promotions
-- ============================================================
SELECT
    p.PRODUCT_CATEGORY,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                         AS PANIER_MOYEN,
    ROUND(AVG(p.DISCOUNT_PERCENTAGE) * 100, 2)      AS AVG_DISCOUNT_PCT
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
-- INNER JOIN pour ne garder que les transactions avec promo active
INNER JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY p.PRODUCT_CATEGORY
ORDER BY TOTAL_VENTES DESC;


-- ============================================================
-- 3. IMPACT DU TAUX DE REMISE SUR LE VOLUME DE VENTES
-- Objectif : Determiner la tranche de remise qui maximise
--            le CA et le panier moyen
-- ============================================================
SELECT
    CASE
        WHEN p.DISCOUNT_PERCENTAGE < 0.10               THEN '< 10%'
        WHEN p.DISCOUNT_PERCENTAGE BETWEEN 0.10 AND 0.20 THEN '10%-20%'
        WHEN p.DISCOUNT_PERCENTAGE BETWEEN 0.20 AND 0.30 THEN '20%-30%'
        ELSE '> 30%'
    END                                             AS TRANCHE_REMISE,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                         AS PANIER_MOYEN
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
INNER JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY TRANCHE_REMISE
ORDER BY TOTAL_VENTES DESC;


-- ============================================================
-- 4. PERFORMANCE DES PROMOTIONS PAR REGION ET TYPE
-- Objectif : Identifier les combinaisons region/type de promo
--            les plus performantes pour cibler les futurs efforts
-- ============================================================
SELECT
    p.REGION,
    p.PROMOTION_TYPE,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(t.AMOUNT), 2)                         AS PANIER_MOYEN,
    ROUND(AVG(p.DISCOUNT_PERCENTAGE) * 100, 2)      AS AVG_DISCOUNT_PCT
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
INNER JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY p.REGION, p.PROMOTION_TYPE
ORDER BY TOTAL_VENTES DESC;


-- ============================================================
-- 5. TOP 10 PROMOTIONS PAR CHIFFRE D AFFAIRES GENERE
-- Objectif : Identifier les promotions individuelles les plus
--            efficaces pour s en inspirer dans les prochaines
-- ============================================================
SELECT
    p.PROMOTION_ID,
    p.PROMOTION_TYPE,
    p.PRODUCT_CATEGORY,
    p.REGION,
    ROUND(p.DISCOUNT_PERCENTAGE * 100, 2)           AS DISCOUNT_PCT,
    p.START_DATE,
    p.END_DATE,
    DATEDIFF('day', p.START_DATE, p.END_DATE)       AS DURATION_DAYS,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
INNER JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY
    p.PROMOTION_ID, p.PROMOTION_TYPE, p.PRODUCT_CATEGORY,
    p.REGION, p.DISCOUNT_PERCENTAGE, p.START_DATE, p.END_DATE
ORDER BY TOTAL_VENTES DESC
LIMIT 10;


-- ============================================================
-- 6. EVOLUTION MENSUELLE DES VENTES PENDANT LES PROMOTIONS
-- Objectif : Identifier les mois les plus performants
--            en periode promotionnelle pour optimiser le calendrier
-- ============================================================
SELECT
    YEAR(t.TRANSACTION_DATE)                        AS ANNEE,
    MONTH(t.TRANSACTION_DATE)                       AS MOIS,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES,
    ROUND(AVG(p.DISCOUNT_PERCENTAGE) * 100, 2)      AS AVG_DISCOUNT_PCT
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
INNER JOIN SILVER.PROMOTIONS_CLEAN p
    ON  t.REGION = p.REGION
    AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY ANNEE, MOIS
ORDER BY ANNEE, MOIS;


-- ============================================================
-- 7. IMPACT DES AVIS PRODUITS SUR LES VENTES PAR CATEGORIE
-- Objectif : Verifier si les categories mieux notees
--            generent plus de ventes
-- CORRECTION : La requete precedente faisait un produit cartesien
--              (LEFT JOIN sans condition de jointure entre reviews
--              et transactions). On agrege d abord par categorie
--              puis on fait le rapprochement.
-- ============================================================
SELECT
    r.PRODUCT_CATEGORY,
    ROUND(AVG(r.RATING_3), 2)                       AS AVG_RATING,
    COUNT(DISTINCT r.REVIEW_ID)                     AS NB_AVIS,
    -- Ventes de transactions de type Sale
    -- Note : pas de lien direct entre reviews et transactions
    -- On fait le rapprochement via la categorie uniquement
    t.NB_TRANSACTIONS,
    t.TOTAL_VENTES
FROM (
    -- Agregation des avis par categorie
    SELECT
        PRODUCT_CATEGORY,
        AVG(RATING_3)           AS RATING_3,
        COUNT(REVIEW_ID)        AS REVIEW_ID
    FROM SILVER.PRODUCT_REVIEWS_CLEAN
    WHERE PRODUCT_CATEGORY IS NOT NULL
    GROUP BY PRODUCT_CATEGORY
) r
-- Jointure avec les ventes agreges par categorie (via ACCOUNT_CODE comme proxy)
LEFT JOIN (
    SELECT
        ACCOUNT_CODE,
        COUNT(*)                AS NB_TRANSACTIONS,
        ROUND(SUM(AMOUNT), 2)   AS TOTAL_VENTES
    FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    WHERE TRANSACTION_TYPE = 'SALE'
    GROUP BY ACCOUNT_CODE
) t ON r.PRODUCT_CATEGORY = t.ACCOUNT_CODE
ORDER BY r.RATING_3 DESC;


-- ============================================================
-- 8. IMPACT DES INTERACTIONS SERVICE CLIENT SUR LES VENTES
-- Objectif : Analyser si la satisfaction client annuelle
--            est correlee au volume de ventes de la meme annee
-- ============================================================
SELECT
    YEAR(t.TRANSACTION_DATE)                        AS ANNEE,
    ROUND(AVG(s.CUSTOMER_SATISFACTION), 2)          AS AVG_SATISFACTION,
    COUNT(t.TRANSACTION_ID)                         AS NB_TRANSACTIONS,
    ROUND(SUM(t.AMOUNT), 2)                         AS TOTAL_VENTES
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
-- Jointure sur l annee pour corr les ventes et la satisfaction de la meme periode
LEFT JOIN SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN s
    ON YEAR(t.TRANSACTION_DATE) = YEAR(s.INTERACTION_DATE)
WHERE t.TRANSACTION_TYPE = 'SALE'
GROUP BY ANNEE
ORDER BY ANNEE;
