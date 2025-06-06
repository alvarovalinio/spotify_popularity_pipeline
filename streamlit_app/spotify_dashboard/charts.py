import plotly.express as px
import pandas as pd
from plotly.graph_objects import Figure


def popularity_trend_chart(df: pd.DataFrame) -> Figure:
    """Return bar chart of average popularity over time."""
    return px.bar(
        df,
        x="Loaded At",
        y="Average Popularity",
        color="Artist Name",
        title="Popularity Over Time",
        barmode="group",
        hover_data=["Artist Name", "Average Popularity"],
    ).update_xaxes(tickformat="%Y-%m-%d")


def album_popularity_chart(df: pd.DataFrame) -> Figure:
    """Return horizontal bar chart of average popularity by album."""
    return px.bar(
        df,
        x="Album Popularity",
        y="Album Name",
        orientation="h",
        title="Top 10 Albums by Popularity",
        hover_data=["Artist Name", "Album Popularity"],
    )


def distribution_chart(df: pd.DataFrame) -> Figure:
    """Return box plot showing popularity distribution by artist."""
    return px.box(
        df,
        x="Artist Name",
        y="Popularity",
        title="Popularity Distribution by Artist",
    ).update_xaxes(categoryorder="category ascending")
