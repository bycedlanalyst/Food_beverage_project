# ============================================================
# FICHIER : streamlit/promotion_analysis.py
# PROJET  : AnyCompany Food & Beverage
# PHASE   : 2 - Visualisations Streamlit
# OBJECTIF: Analyse des promotions
# ============================================================

import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="AnyCompany - Promotion Analysis",
    layout="wide"
)

# ============================================================
# CONNEXION SNOWFLAKE NATIVE
# ============================================================
session = get_active_session()

@st.cache_data(ttl=600)
def run_query(query):
    return session.sql(query).to_pandas()

st.title("Promotion Analysis - AnyCompany Food & Beverage")
st.markdown("---")

# ============================================================
# FILTRES SIDEBAR
# ============================================================
st.sidebar.header("Filtres")

categories = run_query("SELECT DISTINCT PRODUCT_CATEGORY FROM ANALYTICS.PROMOTIONS_ACTIVES ORDER BY PRODUCT_CATEGORY")
selected_categories = st.sidebar.multiselect("Categorie produit", categories["PRODUCT_CATEGORY"].tolist(), default=categories["PRODUCT_CATEGORY"].tolist())

regions = run_query("SELECT DISTINCT REGION FROM ANALYTICS.PROMOTIONS_ACTIVES ORDER BY REGION")
selected_regions = st.sidebar.multiselect("Region", regions["REGION"].tolist(), default=regions["REGION"].tolist())

where_cat    = f"PRODUCT_CATEGORY IN ({','.join([repr(c) for c in selected_categories])})" if selected_categories else "1=1"
where_region = f"REGION IN ({','.join([repr(r) for r in selected_regions])})" if selected_regions else "1=1"
where_clause = f"WHERE {where_cat} AND {where_region}"

# ============================================================
# KPIs
# ============================================================
kpi = run_query(f"""
SELECT
    COUNT(*)                        AS NB_PROMOTIONS,
    ROUND(AVG(DISCOUNT_PCT), 2)     AS AVG_DISCOUNT,
    ROUND(SUM(TOTAL_VENTES), 2)     AS TOTAL_VENTES,
    ROUND(AVG(PANIER_MOYEN), 2)     AS AVG_PANIER,
    SUM(NB_TRANSACTIONS)            AS NB_TRANSACTIONS
FROM ANALYTICS.PROMOTIONS_ACTIVES
{where_clause}
""")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Promotions",       f"{int(kpi['NB_PROMOTIONS'][0]):,}")
col2.metric("Remise moyenne",   f"{kpi['AVG_DISCOUNT'][0]}%")
col3.metric("CA genere",        f"${kpi['TOTAL_VENTES'][0]:,.2f}")
col4.metric("Panier moyen",     f"${kpi['AVG_PANIER'][0]:,.2f}")
col5.metric("Transactions",     f"{int(kpi['NB_TRANSACTIONS'][0]):,}")

st.markdown("---")

# ============================================================
# PERFORMANCE PAR CATEGORIE
# ============================================================
st.subheader("Performance par categorie produit")

perf_cat = run_query(f"""
SELECT
    PRODUCT_CATEGORY,
    COUNT(*) AS NB_PROMOTIONS,
    ROUND(AVG(DISCOUNT_PCT), 2) AS AVG_DISCOUNT,
    ROUND(SUM(TOTAL_VENTES), 2) AS TOTAL_VENTES,
    ROUND(AVG(AVG_RATING), 2) AS AVG_RATING
FROM ANALYTICS.PROMOTIONS_ACTIVES
{where_clause}
GROUP BY PRODUCT_CATEGORY
ORDER BY TOTAL_VENTES DESC
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(perf_cat).mark_bar().encode(
        x=alt.X("PRODUCT_CATEGORY:N", sort="-y", title="Categorie"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        color=alt.Color("AVG_DISCOUNT:Q", scale=alt.Scale(scheme="reds")),
        tooltip=["PRODUCT_CATEGORY", "TOTAL_VENTES", "AVG_DISCOUNT", "NB_PROMOTIONS"]
    ).properties(title="CA par categorie pendant les promotions", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(perf_cat).mark_circle().encode(
        x=alt.X("AVG_DISCOUNT:Q", title="Remise moyenne (%)"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        size=alt.Size("NB_PROMOTIONS:Q"),
        color=alt.Color("PRODUCT_CATEGORY:N"),
        tooltip=["PRODUCT_CATEGORY", "AVG_DISCOUNT", "TOTAL_VENTES", "NB_PROMOTIONS"]
    ).properties(title="Remise vs CA par categorie", height=400)
    st.altair_chart(chart, use_container_width=True)

# ============================================================
# PERFORMANCE PAR REGION
# ============================================================
st.subheader("Performance par region")

perf_region = run_query(f"""
SELECT
    REGION,
    COUNT(*) AS NB_PROMOTIONS,
    ROUND(SUM(TOTAL_VENTES), 2) AS TOTAL_VENTES,
    ROUND(AVG(DISCOUNT_PCT), 2) AS AVG_DISCOUNT
FROM ANALYTICS.PROMOTIONS_ACTIVES
{where_clause}
GROUP BY REGION
ORDER BY TOTAL_VENTES DESC
""")

chart = alt.Chart(perf_region).mark_bar().encode(
    x=alt.X("REGION:N", sort="-y", title="Region"),
    y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
    color=alt.Color("AVG_DISCOUNT:Q", scale=alt.Scale(scheme="blues")),
    tooltip=["REGION", "TOTAL_VENTES", "AVG_DISCOUNT", "NB_PROMOTIONS"]
).properties(title="CA genere par region pendant les promotions", height=400)
st.altair_chart(chart, use_container_width=True)

# ============================================================
# TOP 10 PROMOTIONS
# ============================================================
st.subheader("Top 10 promotions par CA genere")

top_promos = run_query(f"""
SELECT
    PROMOTION_ID, PROMOTION_TYPE, PRODUCT_CATEGORY,
    REGION, DISCOUNT_PCT, NB_TRANSACTIONS,
    ROUND(TOTAL_VENTES, 2) AS TOTAL_VENTES,
    DURATION_DAYS
FROM ANALYTICS.PROMOTIONS_ACTIVES
{where_clause}
ORDER BY TOTAL_VENTES DESC
LIMIT 10
""")
st.dataframe(top_promos, use_container_width=True)

# ============================================================
# IMPACT DU TAUX DE REMISE
# ============================================================
st.subheader("Impact du taux de remise sur les ventes")

remise_impact = run_query(f"""
SELECT
    CASE
        WHEN DISCOUNT_PCT < 10 THEN '< 10%'
        WHEN DISCOUNT_PCT BETWEEN 10 AND 20 THEN '10-20%'
        WHEN DISCOUNT_PCT BETWEEN 20 AND 30 THEN '20-30%'
        ELSE '> 30%'
    END AS TRANCHE_REMISE,
    COUNT(*) AS NB_PROMOTIONS,
    ROUND(SUM(TOTAL_VENTES), 2) AS TOTAL_VENTES,
    ROUND(AVG(PANIER_MOYEN), 2) AS AVG_PANIER
FROM ANALYTICS.PROMOTIONS_ACTIVES
{where_clause}
GROUP BY TRANCHE_REMISE
ORDER BY TOTAL_VENTES DESC
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(remise_impact).mark_bar().encode(
        x=alt.X("TRANCHE_REMISE:N", title="Tranche de remise"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        color=alt.Color("TRANCHE_REMISE:N"),
        tooltip=["TRANCHE_REMISE", "TOTAL_VENTES", "NB_PROMOTIONS"]
    ).properties(title="CA par tranche de remise", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(remise_impact).mark_bar().encode(
        x=alt.X("TRANCHE_REMISE:N", title="Tranche de remise"),
        y=alt.Y("AVG_PANIER:Q", title="Panier moyen ($)"),
        color=alt.Color("TRANCHE_REMISE:N"),
        tooltip=["TRANCHE_REMISE", "AVG_PANIER"]
    ).properties(title="Panier moyen par tranche de remise", height=400)
    st.altair_chart(chart, use_container_width=True)

st.markdown("---")
st.caption("AnyCompany Food & Beverage - Data Analytics Lab")