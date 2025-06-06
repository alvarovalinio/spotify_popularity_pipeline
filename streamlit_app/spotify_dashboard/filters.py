import streamlit as st
import duckdb
from typing import Tuple, List


def select_filters(con: duckdb.DuckDBPyConnection) -> Tuple[str, List[str]]:
    """Render sidebar filters and return selected date and artist list."""
    st.sidebar.header("Filters")
    dates = con.execute(
        "SELECT DISTINCT loaded_at FROM analytics.fact_track_popularity ORDER BY loaded_at"
    ).fetchdf()["loaded_at"]
    artists = con.execute(
        "SELECT DISTINCT artist_name FROM analytics.dim_artist ORDER BY artist_name"
    ).fetchdf()["artist_name"]

    selected_date = st.sidebar.selectbox("Select Date", dates[::-1])
    selected_artists = st.sidebar.multiselect(
        "Select Artists", artists, default=artists
    )
    return selected_date, selected_artists
