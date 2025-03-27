import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import config

def get_data(query):
    """Fetch data from the PostgreSQL database using the provided SQL query."""
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Set up the dashboard title.
st.title("IoT Data Dashboard")

# --- Building Metadata Section ---
st.header("Building Metadata")
df_building = get_data("SELECT * FROM building_metadata;")
if not df_building.empty:
    st.dataframe(df_building)
    # Create a bar chart showing square footage by building.
    fig_building = px.bar(
        df_building,
        x="building_id",
        y="square_feet",
        title="Square Feet by Building",
        labels={"building_id": "Building ID", "square_feet": "Square Feet"}
    )
    st.plotly_chart(fig_building)
else:
    st.write("No building metadata available.")

# --- Train Data Section ---
st.header("Train Data")
df_train = get_data("SELECT * FROM train_data;")
if not df_train.empty:
    st.dataframe(df_train)
    # Create a box plot to show the distribution of meter readings by building.
    fig_train = px.box(
        df_train,
        x="building_id",
        y="meter_reading",
        title="Meter Readings by Building",
        labels={"building_id": "Building ID", "meter_reading": "Meter Reading"}
    )
    st.plotly_chart(fig_train)
else:
    st.write("No train data available.")

# --- Weather Data Section ---
st.header("Weather Data")
df_weather = get_data("SELECT * FROM weather_train;")
if not df_weather.empty:
    st.dataframe(df_weather)
    # Create a time series chart for air temperature.
    fig_weather = px.line(
        df_weather,
        x="timestamp",
        y="air_temperature",
        title="Air Temperature Over Time",
        labels={"timestamp": "Time", "air_temperature": "Air Temperature"}
    )
    st.plotly_chart(fig_weather)
else:
    st.write("No weather data available.")
