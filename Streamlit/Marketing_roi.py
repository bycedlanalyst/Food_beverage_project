# ============================================================
# FICHIER : streamlit/marketing_roi.py
# PROJET  : AnyCompany Food & Beverage
# PHASE   : 2 - Visualisations Streamlit
# OBJECTIF: ROI Marketing
# ============================================================

import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="AnyCompany - Marketing ROI",
    layout="wide"
)

# ============================================================
# CONNEXION SNOWFLAKE NATIVE
# ============================================================
session = get_active_session()

@st.cache_data(ttl=600)
def run_query(query):
    return session.sql(query).to_pandas()

st.title("Marketing ROI - AnyCompany Food & Beverage")
st.markdown("---")

# ============================================================
# FILTRES SIDEBAR
# ============================================================
st.sidebar.header("Filtres")

camp_types = run_query("SELECT DISTINCT CAMPAIGN_TYPE FROM ANALYTICS.VENTES_ENRICHIES WHERE CAMPAIGN_TYPE IS NOT NULL ORDER BY CAMPAIGN_TYPE")
selected_types = st.sidebar.multiselect("Type de campagne", camp_types["CAMPAIGN_TYPE"].tolist(), default=camp_types["CAMPAIGN_TYPE"].tolist())

audiences = run_query("SELECT DISTINCT TARGET_AUDIENCE FROM ANALYTICS.VENTES_ENRICHIES WHERE TARGET_AUDIENCE IS NOT NULL ORDER BY TARGET_AUDIENCE")
selected_audiences = st.sidebar.multiselect("Audience cible", audiences["TARGET_AUDIENCE"].tolist(), default=audiences["TARGET_AUDIENCE"].tolist())

where_types    = f"CAMPAIGN_TYPE IN ({','.join([repr(t) for t in selected_types])})" if selected_types else "1=1"
where_audience = f"TARGET_AUDIENCE IN ({','.join([repr(a) for a in selected_audiences])})" if selected_audiences else "1=1"
where_clause   = f"WHERE IS_CAMPAIGN = TRUE AND {where_types} AND {where_audience}"

# ============================================================
# KPIs
# ============================================================
kpi = run_query(f"""
SELECT
    COUNT(DISTINCT CAMPAIGN_ID)                     AS NB_CAMPAGNES,
    ROUND(SUM(CAMPAIGN_BUDGET), 2)                  AS TOTAL_BUDGET,
    ROUND(SUM(AMOUNT), 2)                           AS TOTAL_VENTES,
    ROUND(AVG(CAMPAIGN_CONVERSION_RATE) * 100, 2)   AS AVG_CONVERSION,
    ROUND((SUM(AMOUNT) - SUM(CAMPAIGN_BUDGET)) / NULLIF(SUM(CAMPAIGN_BUDGET), 0) * 100, 2) AS ROI_GLOBAL
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
""")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Campagnes",        f"{int(kpi['NB_CAMPAGNES'][0]):,}")
col2.metric("Budget total",     f"${kpi['TOTAL_BUDGET'][0]:,.2f}")
col3.metric("CA genere",        f"${kpi['TOTAL_VENTES'][0]:,.2f}")
col4.metric("Taux conversion",  f"{kpi['AVG_CONVERSION'][0]}%")
col5.metric("ROI global",       f"{kpi['ROI_GLOBAL'][0]}%")

st.markdown("---")

# ============================================================
# PERFORMANCE PAR TYPE DE CAMPAGNE
# ============================================================
st.subheader("Performance par type de campagne")

perf_type = run_query(f"""
SELECT
    CAMPAIGN_TYPE,
    COUNT(DISTINCT CAMPAIGN_ID)                     AS NB_CAMPAGNES,
    ROUND(SUM(CAMPAIGN_BUDGET), 2)                  AS TOTAL_BUDGET,
    ROUND(SUM(AMOUNT), 2)                           AS TOTAL_VENTES,
    ROUND(AVG(CAMPAIGN_CONVERSION_RATE) * 100, 2)   AS AVG_CONVERSION,
    ROUND((SUM(AMOUNT) - SUM(CAMPAIGN_BUDGET)) / NULLIF(SUM(CAMPAIGN_BUDGET), 0) * 100, 2) AS ROI_PCT
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
GROUP BY CAMPAIGN_TYPE
ORDER BY ROI_PCT DESC
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(perf_type).mark_bar().encode(
        x=alt.X("CAMPAIGN_TYPE:N", sort="-y", title="Type de campagne"),
        y=alt.Y("ROI_PCT:Q", title="ROI (%)"),
        color=alt.Color("ROI_PCT:Q", scale=alt.Scale(scheme="redyellowgreen")),
        tooltip=["CAMPAIGN_TYPE", "ROI_PCT", "TOTAL_VENTES", "TOTAL_BUDGET"]
    ).properties(title="ROI par type de campagne", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(perf_type).mark_circle().encode(
        x=alt.X("TOTAL_BUDGET:Q", title="Budget ($)"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        size=alt.Size("NB_CAMPAGNES:Q"),
        color=alt.Color("CAMPAIGN_TYPE:N"),
        tooltip=["CAMPAIGN_TYPE", "TOTAL_BUDGET", "TOTAL_VENTES", "NB_CAMPAGNES"]
    ).properties(title="Budget vs CA par type de campagne", height=400)
    st.altair_chart(chart, use_container_width=True)

# ============================================================
# PERFORMANCE PAR AUDIENCE
# ============================================================
st.subheader("Performance par audience cible")

perf_audience = run_query(f"""
SELECT
    TARGET_AUDIENCE,
    COUNT(DISTINCT CAMPAIGN_ID)                     AS NB_CAMPAGNES,
    ROUND(SUM(AMOUNT), 2)                           AS TOTAL_VENTES,
    ROUND(AVG(CAMPAIGN_CONVERSION_RATE) * 100, 2)   AS AVG_CONVERSION,
    ROUND((SUM(AMOUNT) - SUM(CAMPAIGN_BUDGET)) / NULLIF(SUM(CAMPAIGN_BUDGET), 0) * 100, 2) AS ROI_PCT
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
GROUP BY TARGET_AUDIENCE
ORDER BY TOTAL_VENTES DESC
""")

col1, col2 = st.columns(2)
with col1:
    chart = alt.Chart(perf_audience).mark_bar().encode(
        x=alt.X("TARGET_AUDIENCE:N", sort="-y", title="Audience"),
        y=alt.Y("TOTAL_VENTES:Q", title="CA ($)"),
        color=alt.Color("AVG_CONVERSION:Q", scale=alt.Scale(scheme="blues")),
        tooltip=["TARGET_AUDIENCE", "TOTAL_VENTES", "AVG_CONVERSION", "NB_CAMPAGNES"]
    ).properties(title="CA par audience cible", height=400)
    st.altair_chart(chart, use_container_width=True)

with col2:
    chart = alt.Chart(perf_audience).mark_arc().encode(
        theta=alt.Theta("TOTAL_VENTES:Q"),
        color=alt.Color("TARGET_AUDIENCE:N"),
        tooltip=["TARGET_AUDIENCE", "TOTAL_VENTES"]
    ).properties(title="Repartition du CA par audience", height=400)
    st.altair_chart(chart, use_container_width=True)

# ============================================================
# TOP 10 CAMPAGNES PAR ROI
# ============================================================
st.subheader("Top 10 campagnes par ROI")

top_camps = run_query(f"""
SELECT
    CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE,
    TARGET_AUDIENCE, REGION,
    ROUND(CAMPAIGN_BUDGET, 2) AS BUDGET,
    ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES,
    ROUND(AVG(CAMPAIGN_CONVERSION_RATE) * 100, 2) AS CONVERSION_PCT,
    ROUND((SUM(AMOUNT) - CAMPAIGN_BUDGET) / NULLIF(CAMPAIGN_BUDGET, 0) * 100, 2) AS ROI_PCT
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE,
         TARGET_AUDIENCE, REGION, CAMPAIGN_BUDGET
ORDER BY ROI_PCT DESC
LIMIT 10
""")
st.dataframe(top_camps, use_container_width=True)

# ============================================================
# EVOLUTION MENSUELLE DU ROI
# ============================================================
st.subheader("Evolution mensuelle du ROI")

roi_temps = run_query(f"""
SELECT
    ANNEE, MOIS,
    ROUND(SUM(AMOUNT), 2) AS TOTAL_VENTES,
    ROUND(SUM(CAMPAIGN_BUDGET), 2) AS TOTAL_BUDGET,
    ROUND((SUM(AMOUNT) - SUM(CAMPAIGN_BUDGET)) / NULLIF(SUM(CAMPAIGN_BUDGET), 0) * 100, 2) AS ROI_PCT
FROM ANALYTICS.VENTES_ENRICHIES
{where_clause}
GROUP BY ANNEE, MOIS
ORDER BY ANNEE, MOIS
""")
roi_temps["PERIODE"] = roi_temps["ANNEE"].astype(str) + "-" + roi_temps["MOIS"].astype(str).str.zfill(2)

chart = alt.Chart(roi_temps).mark_line(point=True).encode(
    x=alt.X("PERIODE:O", title="Periode"),
    y=alt.Y("ROI_PCT:Q", title="ROI (%)"),
    tooltip=["PERIODE", "ROI_PCT", "TOTAL_VENTES", "TOTAL_BUDGET"]
).properties(title="Evolution mensuelle du ROI (%)", width=900, height=400)
st.altair_chart(chart, use_container_width=True)

st.markdown("---")
st.caption("AnyCompany Food & Beverage - Data Analytics Lab")