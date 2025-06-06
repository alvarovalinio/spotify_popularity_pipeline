import streamlit as st
import duckdb
from spotify_dashboard.filters import select_filters
from spotify_dashboard.charts import (
    popularity_trend_chart,
    album_popularity_chart,
    distribution_chart,
)
from spotify_dashboard.utils import artist_list_to_sql, load_sql

st.set_page_config(page_title="Spotify Popularity Dashboard", layout="wide")
st.title("🎧 Spotify Popularity Dashboard")
st.markdown(
    "[🔗 View Source on GitHub](https://github.com/alvarovalinio/spotify_popularity_pipeline)",
    unsafe_allow_html=True,
)

con = duckdb.connect("../data/spotify.duckdb", read_only=True)

## SideBar
selected_date, selected_artists_list = select_filters(con)
st.sidebar.markdown("---")
st.sidebar.markdown(
    "Developed by [Alvaro Valino](https://www.linkedin.com/in/alvaro-valino/)"
)

artist_str = artist_list_to_sql(selected_artists_list)

sql = load_sql("top5_tracks.sql").format(
    selected_date=selected_date, selected_artists=artist_str
)
df1 = con.execute(sql).fetchdf()
st.subheader("Top 5 Songs by Artist")
st.dataframe(df1, hide_index=True)


sql = load_sql("album_popularity.sql").format(
    selected_date=selected_date, selected_artists=artist_str
)
df3 = con.execute(sql).fetchdf()
st.subheader("Top Albums by popularity")
st.plotly_chart(album_popularity_chart(df3), use_container_width=True)

sql = load_sql("distribution.sql").format(
    selected_date=selected_date, selected_artists=artist_str
)
df4 = con.execute(sql).fetchdf()
st.subheader("Popularity Distribution")
st.plotly_chart(distribution_chart(df4), use_container_width=True)

sql = load_sql("trend.sql").format(selected_artists=artist_str)
df2 = con.execute(sql).fetchdf()
st.subheader("Weekly Popularity Evolution")
st.plotly_chart(popularity_trend_chart(df2), use_container_width=True)

st.caption("Data visualized from DuckDB - Spotify Popularity Project")
