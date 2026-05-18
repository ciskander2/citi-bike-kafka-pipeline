import os
import pandas as pd
import psycopg2
import streamlit as st

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")

st.set_page_config(
    page_title="Citi Bike Streaming Analytics",
    layout="wide"
)

st.title("🚲 Citi Bike Real-Time Streaming Analytics")
st.caption("Kafka → Supabase/Postgres → dbt → Streamlit")


@st.cache_data(ttl=30)
def load_data(query):
    conn = psycopg2.connect(SUPABASE_URL)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


row_count = load_data(
    "SELECT COUNT(*) AS total_rows FROM station_status"
)

st.metric(
    "Total Streaming Events",
    f"{row_count['total_rows'][0]:,}"
)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 20 E-Bike Stations")

    top_ebikes = load_data(
        "SELECT * FROM top_20_stations LIMIT 20"
    )

    st.dataframe(
        top_ebikes,
        use_container_width=True
    )

with col2:
    st.subheader("No Docking Available")

    no_docking = load_data(
        "SELECT * FROM no_docking_but_availability LIMIT 20"
    )

    st.dataframe(
        no_docking,
        use_container_width=True
    )

st.subheader("Large-Capacity Stations With Zero Bike Availability")

wasted = load_data(
    "SELECT * FROM capacity_zero_availability LIMIT 20"
)

st.dataframe(
    wasted,
    use_container_width=True
)

st.subheader("Hourly Bike Shortage Trends")

hourly = load_data(
    """
    SELECT *
    FROM hourly_bike_status
    ORDER BY hour_bucket DESC
    LIMIT 50
    """
)

st.line_chart(
    hourly.set_index("hour_bucket")[
        ["empty_station_rate", "full_station_rate"]
    ]
)

st.subheader("Station Utilization Ratio")

utilization = load_data(
    "SELECT * FROM utlilization_ratio LIMIT 50"
)

st.dataframe(
    utilization,
    use_container_width=True
)