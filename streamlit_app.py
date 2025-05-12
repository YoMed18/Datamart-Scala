import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")
st.title("🚖 Tableau de bord - Données Taxi NYC")

@st.cache_data
def load_ids():
    engine = create_engine("postgresql://postgres:admin@localhost:15435/datamart")
    return pd.read_sql_query("SELECT DISTINCT passenger_count_id FROM fact_trips ORDER BY passenger_count_id", engine)

@st.cache_data
def get_data(selected_ids):
    engine = create_engine("postgresql://postgres:admin@localhost:15435/datamart")
    format_ids = ",".join(map(str, selected_ids))
    query = f"""
        SELECT 
            ft.passenger_count_id, 
            ft.trip_distance, 
            ft.fare_amount, 
            ft.total_amount,
            dt.year, dt.month
        FROM fact_trips ft
        JOIN dim_time dt ON ft.pickup_time_id = dt.time_id
        WHERE ft.passenger_count_id IN ({format_ids})
        LIMIT 50000
    """
    return pd.read_sql_query(query, engine)

# Interface Streamlit
passenger_ids = load_ids()["passenger_count_id"].tolist()
selected_ids = st.multiselect("Filtrer par nombre de passagers", passenger_ids, default=passenger_ids)

df = get_data(selected_ids)

if df.empty:
    st.warning("Aucune donnée trouvée.")
else:
    st.subheader("📊 Répartition du montant total moyen par nombre de passagers")
    st.bar_chart(df.groupby("passenger_count_id")["total_amount"].mean())

    st.subheader("📈 Corrélation distance - montant total")
    st.scatter_chart(df[["trip_distance", "total_amount"]])

    st.subheader("📋 Aperçu des données")
    st.dataframe(df.head(10))
