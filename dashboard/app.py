import datetime as dt
import time

import altair as alt
import pandas as pd
import streamlit as st

from cassandra.cluster import Cluster
import mysql.connector

CASSANDRA_HOST = st.secrets["cassandra"]["CASSANDRA_HOST"]
CASSANDRA_KEYSPACE = st.secrets["cassandra"]["CASSANDRA_KEYSPACE"]
CASSANDRA_TABLE = st.secrets["cassandra"]["CASSANDRA_TABLE"]

MYSQL_HOST = st.secrets["mysql"]["MYSQL_HOST"]
MYSQL_PORT = st.secrets["mysql"]["MYSQL_PORT"]
MYSQL_DATABASE = st.secrets["mysql"]["MYSQL_DATABASE"]
MYSQL_USERNAME = st.secrets["mysql"]["MYSQL_USERNAME"]
MYSQL_PASSWORD = st.secrets["mysql"]["MYSQL_PASSWORD"]

st.set_page_config(layout='wide', page_title='Brilian Stock Dashboard')

@st.cache_data(ttl=60)
def get_view_data(view_name):
    # Connect to the database and retrieve the view data in a dataframe
    config = {
        'user': MYSQL_USERNAME,
        'password': MYSQL_PASSWORD,
        'host': MYSQL_HOST,
        'port': MYSQL_PORT,
        'database': MYSQL_DATABASE,
        'raise_on_warnings': True
        }

    cnx = mysql.connector.connect(**config)

    with cnx.cursor(dictionary=True) as c:
        result = c.execute(f"SELECT * FROM {view_name}")

        rows = c.fetchall()

    cnx.close()

    return pd.DataFrame(rows)

@st.cache_data(ttl=15)
def get_raw_data():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = f"SELECT * FROM {CASSANDRA_TABLE}"
    result = session.execute(query)
    result = pd.DataFrame(result)
    return result.sort_values('created_at')
    
today = dt.date.today().strftime("%Y-%m-%d")
title = f"Brilian Stock Dashboard: {today}"
st.header(title)

view_df = get_view_data('volume_today')
view_df['total_volume'] = view_df['total_volume'].astype(int)

chart = alt.Chart(view_df).mark_bar(color="cyan").encode(
            x=alt.X("total_volume", axis=alt.Axis(title="Volume")),
            y = alt.Y('sector', sort='-x', axis=alt.Axis(title=None))
        ).properties(title="Top Volume by Sector", width=1200, height=400)
        
st.altair_chart(chart)

price_df = get_raw_data()
price_df = price_df.tail(10*5)

# highlight = alt.selection_point(
#     on="pointerover", fields=["ticker"], nearest=True
# )

base = alt.Chart(price_df).encode(
    x="created_at:T",
    y=alt.Y("price:Q", scale=alt.Scale(domain=[500, 600])),
    color="ticker:N"
).properties(title="Historical Price Movement", width=1200, height=400)

# points = base.mark_circle().encode(
#     opacity=alt.value(0)
# ).add_params(
#     highlight
# ).properties(
#     width=1200,
#     title="Historical Price Movement"
# )

lines = base.mark_line().encode()

st.altair_chart(lines)
