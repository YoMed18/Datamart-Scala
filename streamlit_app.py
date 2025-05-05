import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")
st.title("🚖 Tableau de bord - Données Taxi NYC")

# Connexion PostgreSQL
@st.cache_data
def get_data():
    conn = psycopg2.connect(
        dbname="datamart",
        user="postgres",
        password="admin",
        host="localhost",
        port=15435
    )
    # query = """
    #     SELECT passenger_count_id, trip_distance, fare_amount, total_amount
    #     FROM fact_trips
    #     WHERE passenger_count_id BETWEEN 1 AND 6
    #     LIMIT 50000
    # """
    query = """
        SELECT 
            ft.passenger_count_id, 
            ft.trip_distance, 
            ft.fare_amount, 
            ft.total_amount,
            dt.year, dt.month
        FROM fact_trips ft
        JOIN dim_time dt ON ft.pickup_time_id = dt.time_id
        WHERE ft.passenger_count_id BETWEEN 1 AND 6
        LIMIT 50000
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Charger les données
df = get_data()

# Affichage
if df.empty:
    st.warning("Aucune donnée trouvée.")
else:
    # Récupération des années/mois dans les données
    years = df["year"].unique()
    months = df["month"].unique()

    year_label = ", ".join(map(str, sorted(years)))
    month_label = ", ".join(map(lambda x: f"{int(x):02d}", sorted(months)))

    st.subheader(f"📅 Données disponibles pour : {year_label} / Mois : {month_label}")

    st.subheader("📊 Répartition du montant total moyen par nombre de passagers")
    st.bar_chart(df.groupby("passenger_count_id")["total_amount"].mean())

    st.subheader("📈 Corrélation distance - montant total")
    st.scatter_chart(df[["trip_distance", "total_amount"]])

    st.subheader("📋 Aperçu des données")
    st.dataframe(df.head(10))
