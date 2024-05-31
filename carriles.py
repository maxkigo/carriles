import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import base64

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
    SUM(CASE WHEN mes = 1 THEN lecturas ELSE 0 END) AS lecturas_january,
    SUM(CASE WHEN mes = 1 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_january,
    SUM(CASE WHEN mes = 1 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_january,
    SUM(CASE WHEN mes = 2 THEN lecturas ELSE 0 END) AS lecturas_february,
    SUM(CASE WHEN mes = 2 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_february,
    SUM(CASE WHEN mes = 2 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_february,
    SUM(CASE WHEN mes = 3 THEN lecturas ELSE 0 END) AS lecturas_march,
    SUM(CASE WHEN mes = 3 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_march,
    SUM(CASE WHEN mes = 3 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_march,
    SUM(CASE WHEN mes = 4 THEN lecturas ELSE 0 END) AS lecturas_april,
    SUM(CASE WHEN mes = 4 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_april,
    SUM(CASE WHEN mes = 4 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_april,
    SUM(CASE WHEN mes = 5 THEN lecturas ELSE 0 END) AS lecturas_may,
    SUM(CASE WHEN mes = 5 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_may,
    SUM(CASE WHEN mes = 5 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_may,
    SUM(CASE WHEN mes = 6 THEN lecturas ELSE 0 END) AS lecturas_june,
    SUM(CASE WHEN mes = 6 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_june,
    SUM(CASE WHEN mes = 6 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_june,
    SUM(CASE WHEN mes = 7 THEN lecturas ELSE 0 END) AS lecturas_july,
    SUM(CASE WHEN mes = 7 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_july,
    SUM(CASE WHEN mes = 7 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_july,
    SUM(CASE WHEN mes = 8 THEN lecturas ELSE 0 END) AS lecturas_august,
    SUM(CASE WHEN mes = 8 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_august,
    SUM(CASE WHEN mes = 8 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_august,
    SUM(CASE WHEN mes = 9 THEN lecturas ELSE 0 END) AS lecturas_september,
    SUM(CASE WHEN mes = 9 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_september,
    SUM(CASE WHEN mes = 9 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_september,
    SUM(CASE WHEN mes = 10 THEN lecturas ELSE 0 END) AS lecturas_october,
    SUM(CASE WHEN mes = 10 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_october,
    SUM(CASE WHEN mes = 10 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_october,
    SUM(CASE WHEN mes = 11 THEN lecturas ELSE 0 END) AS lecturas_november,
    SUM(CASE WHEN mes = 11 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_november,
    SUM(CASE WHEN mes = 11 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_november,
    SUM(CASE WHEN mes = 12 THEN lecturas ELSE 0 END) AS lecturas_december,
    SUM(CASE WHEN mes = 12 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_december,
    SUM(CASE WHEN mes = 12 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_december
    FROM (
        SELECT 
        EXTRACT(MONTH FROM date) AS mes, 
        L.QR, 
        R.name, 
        COUNT(L.function_) AS lecturas,
        SUM(CASE WHEN L.function_ = 'open' THEN 1 ELSE 0 END) AS lecturas_correctas,
        SUM(CASE WHEN L.function_ != 'open' THEN 1 ELSE 0 END) AS lecturas_incorrectas
    FROM 
        `parkimovil-app`.geosek_raspis.log_sek AS L
    JOIN 
        `parkimovil-app`.geosek_raspis.raspis AS R
        ON L.QR = R.qr
    WHERE 
        date >= '2024-01-01 00:00:00'
    GROUP BY 
        EXTRACT(MONTH FROM date), L.QR, R.name  
    ) AS subquery
    GROUP BY 
        QR, name
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
    SUM(CASE WHEN mes = 1 THEN lecturas ELSE 0 END) AS lecturas_january,
    SUM(CASE WHEN mes = 1 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_january,
    SUM(CASE WHEN mes = 1 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_january,
    SUM(CASE WHEN mes = 2 THEN lecturas ELSE 0 END) AS lecturas_february,
    SUM(CASE WHEN mes = 2 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_february,
    SUM(CASE WHEN mes = 2 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_february,
    SUM(CASE WHEN mes = 3 THEN lecturas ELSE 0 END) AS lecturas_march,
    SUM(CASE WHEN mes = 3 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_march,
    SUM(CASE WHEN mes = 3 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_march,
    SUM(CASE WHEN mes = 4 THEN lecturas ELSE 0 END) AS lecturas_april,
    SUM(CASE WHEN mes = 4 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_april,
    SUM(CASE WHEN mes = 4 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_april,
    SUM(CASE WHEN mes = 5 THEN lecturas ELSE 0 END) AS lecturas_may,
    SUM(CASE WHEN mes = 5 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_may,
    SUM(CASE WHEN mes = 5 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_may,
    SUM(CASE WHEN mes = 6 THEN lecturas ELSE 0 END) AS lecturas_june,
    SUM(CASE WHEN mes = 6 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_june,
    SUM(CASE WHEN mes = 6 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_june,
    SUM(CASE WHEN mes = 7 THEN lecturas ELSE 0 END) AS lecturas_july,
    SUM(CASE WHEN mes = 7 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_july,
    SUM(CASE WHEN mes = 7 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_july,
    SUM(CASE WHEN mes = 8 THEN lecturas ELSE 0 END) AS lecturas_august,
    SUM(CASE WHEN mes = 8 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_august,
    SUM(CASE WHEN mes = 8 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_august,
    SUM(CASE WHEN mes = 9 THEN lecturas ELSE 0 END) AS lecturas_september,
    SUM(CASE WHEN mes = 9 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_september,
    SUM(CASE WHEN mes = 9 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_september,
    SUM(CASE WHEN mes = 10 THEN lecturas ELSE 0 END) AS lecturas_october,
    SUM(CASE WHEN mes = 10 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_october,
    SUM(CASE WHEN mes = 10 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_october,
    SUM(CASE WHEN mes = 11 THEN lecturas ELSE 0 END) AS lecturas_november,
    SUM(CASE WHEN mes = 11 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_november,
    SUM(CASE WHEN mes = 11 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_november,
    SUM(CASE WHEN mes = 12 THEN lecturas ELSE 0 END) AS lecturas_december,
    SUM(CASE WHEN mes = 12 THEN lecturas_correctas ELSE 0 END) AS lecturas_correctas_december,
    SUM(CASE WHEN mes = 12 THEN lecturas_incorrectas ELSE 0 END) AS lecturas_incorrectas_december
FROM (
    SELECT 
        EXTRACT(MONTH FROM date) AS mes, 
        L.QR, 
        R.name, 
        COUNT(L.function_) AS lecturas,
        SUM(CASE WHEN L.function_ = 'open' THEN 1 ELSE 0 END) AS lecturas_correctas,
        SUM(CASE WHEN L.function_ != 'open' THEN 1 ELSE 0 END) AS lecturas_incorrectas
    FROM 
        `parkimovil-app`.geosek_raspis.log AS L
    JOIN 
        `parkimovil-app`.geosek_raspis.raspis AS R
        ON L.QR = R.qr
    WHERE 
        date >= '2024-01-01 00:00:00'
    GROUP BY 
        EXTRACT(MONTH FROM date), L.QR, R.name
) AS subquery
GROUP BY 
    QR, name
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

@st.cache_data
def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{bin_file}">{file_label}</a>'
    return href

if st.button('Descargar tabla como Excel'):
        with pd.ExcelWriter('carriles_kigo.xlsx', engine='xlsxwriter') as writer:
            df_carriles.to_excel(writer, index=False, sheet_name="CA")
            df_carriles_ed.to_excel(writer, index=False, sheet_name="ED")
        st.success('Tabla descargada exitosamente!')
        st.markdown(get_binary_file_downloader_html('carriles_kigo.xlsx', 'Descargar tabla como Excel'), unsafe_allow_html=True)