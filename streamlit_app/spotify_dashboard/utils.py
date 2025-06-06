from pathlib import Path
from typing import List


def artist_list_to_sql(artists: List[str]) -> str:
    """Convert list of artist names to SQL string."""
    return ", ".join([f"'{a}'" for a in artists])


def load_sql(name: str) -> str:
    """Load SQL template from queries folder."""
    path = Path(__file__).parent / "queries" / name
    return path.read_text()
