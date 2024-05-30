import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Create API client for BigQuery.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

st.set_page_config(
    page_title="Kigo - Carriles",
    layout="wide"
)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.write()

with col2:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')

with col3:
    st.title('Kigo Analítica')

with col4:
    st.write()


st.cache_data(ttl=3600)
def get_carriles_month_ca(client):
    try:
        carriles_lecturas = """
        SELECT
        QR,
        name,
        alias,
        SUM(CASE WHEN mes = 1 THEN lecturas ELSE 0 END) AS lecturas_january,
        SUM(CASE WHEN mes = 2 THEN lecturas ELSE 0 END) AS lecturas_february,
        SUM(CASE WHEN mes = 3 THEN lecturas ELSE 0 END) AS lecturas_march,
        SUM(CASE WHEN mes = 4 THEN lecturas ELSE 0 END) AS lecturas_april,
        SUM(CASE WHEN mes = 5 THEN lecturas ELSE 0 END) AS lecturas_may,
        SUM(CASE WHEN mes = 6 THEN lecturas ELSE 0 END) AS lecturas_june,
        SUM(CASE WHEN mes = 7 THEN lecturas ELSE 0 END) AS lecturas_july,
        SUM(CASE WHEN mes = 8 THEN lecturas ELSE 0 END) AS lecturas_august,
        SUM(CASE WHEN mes = 9 THEN lecturas ELSE 0 END) AS lecturas_september,
        SUM(CASE WHEN mes = 10 THEN lecturas ELSE 0 END) AS lecturas_october,
        SUM(CASE WHEN mes = 11 THEN lecturas ELSE 0 END) AS lecturas_november,
        SUM(CASE WHEN mes = 12 THEN lecturas ELSE 0 END) AS lecturas_december
        FROM (
        SELECT
            EXTRACT(MONTH FROM date) AS mes,
            L.QR,
            R.name,
            R.alias,
            COUNT(L.function_) AS lecturas
        FROM
            `parkimovil-app`.geosek_raspis.log_sek AS L
        JOIN
            `parkimovil-app`.geosek_raspis.raspis AS R
            ON L.QR = R.qr
        WHERE
            date >= '2024-01-01 00:00:00' AND R.api_active = 1
        GROUP BY
            EXTRACT(MONTH FROM date), L.QR, R.name, R.alias
        ) AS subquery
        GROUP BY
            QR, name, alias
        ORDER BY
            QR"""
        df_carriles = client.query(carriles_lecturas).to_dataframe()
        return df_carriles
    except Exception as e:
        st.error(f"Error al obtener los datos de carriles de CA: {e}")
        return pd.DataFrame()


st.cache_data(ttl=3600)
def get_carriles_month_ed(client):
    try:
        carriles_lecturas = """
        SELECT
        QR,
        name,
        alias,
        SUM(CASE WHEN mes = 1 THEN lecturas ELSE 0 END) AS lecturas_january,
        SUM(CASE WHEN mes = 2 THEN lecturas ELSE 0 END) AS lecturas_february,
        SUM(CASE WHEN mes = 3 THEN lecturas ELSE 0 END) AS lecturas_march,
        SUM(CASE WHEN mes = 4 THEN lecturas ELSE 0 END) AS lecturas_april,
        SUM(CASE WHEN mes = 5 THEN lecturas ELSE 0 END) AS lecturas_may,
        SUM(CASE WHEN mes = 6 THEN lecturas ELSE 0 END) AS lecturas_june,
        SUM(CASE WHEN mes = 7 THEN lecturas ELSE 0 END) AS lecturas_july,
        SUM(CASE WHEN mes = 8 THEN lecturas ELSE 0 END) AS lecturas_august,
        SUM(CASE WHEN mes = 9 THEN lecturas ELSE 0 END) AS lecturas_september,
        SUM(CASE WHEN mes = 10 THEN lecturas ELSE 0 END) AS lecturas_october,
        SUM(CASE WHEN mes = 11 THEN lecturas ELSE 0 END) AS lecturas_november,
        SUM(CASE WHEN mes = 12 THEN lecturas ELSE 0 END) AS lecturas_december
        FROM (
        SELECT
            EXTRACT(MONTH FROM date) AS mes,
            L.QR,
            R.name,
            R.alias,
            COUNT(L.function_) AS lecturas
        FROM
            `parkimovil-app`.geosek_raspis.log AS L
        JOIN
            `parkimovil-app`.geosek_raspis.raspis AS R
            ON L.QR = R.qr
        WHERE
            date >= '2024-01-01 00:00:00' AND R.api_active = 1
        GROUP BY
            EXTRACT(MONTH FROM date), L.QR, R.name, R.alias
        ) AS subquery
        GROUP BY
            QR, name, alias
        ORDER BY
            QR"""
        df_carriles = client.query(carriles_lecturas).to_dataframe()
        return df_carriles
    except Exception as e:
        st.error(f"Error al obtener los datos de carriles de CA: {e}")
        return pd.DataFrame()

df_carriles = get_carriles_month_ca(client)
df_carriles_ed = get_carriles_month_ed(client)


st.title('Performance de Carriles CA - 2024')
st.write(df_carriles)

st.title('Performance de Carriles ED - 2024')
st.write(df_carriles_ed)