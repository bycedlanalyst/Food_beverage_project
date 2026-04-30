# ============================================================
# FICHIER : streamlit/sales_dashboard.py
# PROJET  : AnyCompany Food & Beverage
# PHASE   : 2 - Visualisations Streamlit
# OBJECTIF: Tableau de bord des ventes
# ============================================================

import streamlit as st
import pandas as pd
import altair as alt

# ============================================================
# CONFIGURATION DE LA PAGE
# ============================================================
st.set_page_config(
    page_title="AnyCompany - Sales Dashboard",
    layout="wide"
)

# ============================================================
# CONNEXION SNOWFLAKE NATIVE
# ============================================================
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data(ttl=600)
def run_query(query):
    return session.sql(query).to_pandas()

# ============================================================
# TITRE
# ============================================================
st.title("Sales Dashboard - AnyCompany Food & Beverage")
st.markdown("---")

# ============================================================
# FILTRES SIDEBAR
# ============================================================
st.sidebar.header("Filtres")

years = run_query("SELECT DISTINCT ANNEE FROM ANALYTICS.VENTES_ENRICHIES ORDER BY ANNEE")
selected_years = st.sidebar.multiselect("Annee", years["ANNEE"].tolist(), default=years["ANNEE"].tolist())

regions = run_query("SELECT DISTINCT REGION FROM ANALYTICS.VENTES_ENRICHIES ORDER BY REGION")
selected_regions = st.sidebar.multiselect("Region", regions["REGION"].tolist(), default=regions["REGION"].tolist())

promo_filter = st.sidebar.radio("Promotions", ["Toutes", "Avec promotion", "Sans promotion"])

where_years   = f"ANNEE IN ({','.join(map(str, selected_years))})" if selected_years else "1=1"
where_regions = f"REGION IN ({','.join([repr(r) for r in selected_regions])})" if selected_regions else "1=1"
where_promo   = "IS_PROMO = TRUE" if promo_filter == "Avec promotion" else "IS_PROMO = FALSE" if promo_filter == "Sans promotion" else "1=1"
where_clause  = f"WHERE {where_years} AND {where_regions} AND {where_promo}"

# ============================================================
# KPIs
# ============================================================
kpi = run_query(f"""
SELECT
    COUNT(*)                    AS NB_TRANSACTIONS,
    ROUND(SUM(AMOUNT), 2)       AS TOTAL_VENTES,
    ROUND(AVG(AMOUNT), 2)       AS PANIER_MOYEN,
    COUNT(DISTINCT REGION)      AS NB_REGIONS
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Transactions",       f"{int(kpi['NB_TRANSACTIONS'][0]):,}")
col2.metric("Chiffre d affaires", f"${kpi['TOTAL_VENTES'][0]:,.2f}")
col3.metric("Panier moyen",       f"${kpi['PANIER_MOYEN'][0]:,.2f}")
col4.metric("Regions",            f"{int(kpi['NB_REGIONS'][0])}")

st.markdown("---")

# ============================================================
# EVOLUTION DES VENTES
# ============================================================
st.subheader("Evolution des ventes dans le temps")

ventes_temps = run_query(f"""
SELECT ANNEE, MOIS, ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
GROUP BY ANNEE, MOIS ORDER BY ANNEE, MOIS
""")
ventes_temps["PERIODE"] = ventes_temps["ANNEE"].astype(str) + "-" + ventes_temps["MOIS"].astype(str).str.zfill(2)

chart = alt.Chart(ventes_temps).mark_line(point=True).encode(
    x=alt.X("PERIODE:O", title="Periode"),
    y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
    tooltip=["PERIODE", "TOTAL_VENTES"]
).properties(title="CA mensuel", width=900, height=400)
st.altair_chart(chart, use_container_width=True)

# ============================================================
# VENTES PAR REGION
# ============================================================
st.subheader("Performance par region")

ventes_region = run_query(f"""
SELECT REGION, COUNT(*) AS NB_TRANSACTIONS, ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES
FROM ANALYTICS.VENTES_ENRICHIES {where_clause}
GROUP BY REGION ORDER BY TOTAL_VENTES DESC
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(ventes_region).mark_bar().encode(
        x=alt.X("REGION:N", sort="-y", title="Region"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        color=alt.Color("TOTAL_VENTES:Q", scale=alt.Scale(scheme="blues")),
        tooltip=["REGION", "TOTAL_VENTES", "NB_TRANSACTIONS"]
    ).properties(title="CA par region", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(ventes_region).mark_arc().encode(
        theta=alt.Theta("TOTAL_VENTES:Q"),
        color=alt.Color("REGION:N"),
        tooltip=["REGION", "TOTAL_VENTES"]
    ).properties(title="Repartition du CA par region", height=400)
    st.altair_chart(chart, use_container_width=True)

# ============================================================
# IMPACT PROMOTIONS
# ============================================================
st.subheader("Impact des promotions")

promo_impact = run_query(f"""
SELECT
    CASE WHEN IS_PROMO = TRUE THEN 'Avec promotion' ELSE 'Sans promotion' END AS PERIODE,
    COUNT(*) AS NB_TRANSACTIONS,
    ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES,
    ROUND(AVG(AMOUNT), 2) AS PANIER_MOYEN
FROM ANALYTICS.VENTES_ENRICHIES {where_clause}
GROUP BY IS_PROMO
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(promo_impact).mark_bar().encode(
        x=alt.X("PERIODE:N", title="Periode"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        color=alt.Color("PERIODE:N"),
        tooltip=["PERIODE", "TOTAL_VENTES"]
    ).properties(title="CA avec vs sans promotion", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(promo_impact).mark_bar().encode(
        x=alt.X("PERIODE:N", title="Periode"),
        y=alt.Y("PANIER_MOYEN:Q", title="Panier moyen ($)"),
        color=alt.Color("PERIODE:N"),
        tooltip=["PERIODE", "PANIER_MOYEN"]
    ).properties(title="Panier moyen avec vs sans promotion", height=400)
    st.altair_chart(chart, use_container_width=True)

# ============================================================
# TOP 10 ENTITES
# ============================================================
st.subheader("Top 10 entites")

top_entities = run_query(f"""
SELECT ENTITY, ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES
FROM ANALYTICS.VENTES_ENRICHIES {where_clause}
GROUP BY ENTITY ORDER BY TOTAL_VENTES DESC LIMIT 10
""")

chart = alt.Chart(top_entities).mark_bar().encode(
    x=alt.X("TOTAL_VENTES:Q", title="CA ($)"),
    y=alt.Y("ENTITY:N", sort="-x", title="Entite"),
    color=alt.Color("TOTAL_VENTES:Q", scale=alt.Scale(scheme="oranges")),
    tooltip=["ENTITY", "TOTAL_VENTES"]
).properties(title="Top 10 entites par CA", height=400)
st.altair_chart(chart, use_container_width=True)

st.markdown("---")
st.caption("AnyCompany Food & Beverage - Data Analytics Lab")