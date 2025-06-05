import requests
import base64
import os
from retrying import retry
import logging
from typing import Any, Dict, List
import json
from pathlib import Path
import csv
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RETRIES = 3
BASE_WAIT_TIME = 2
SPOTIFY_CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
SPOTIFY_CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]

URL_TOKEN = "https://accounts.spotify.com/api/token"
URL_ARTIST = "https://api.spotify.com/v1/artists/"
URL_TRACK = "https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=ES"
URL_ALBUM = "https://api.spotify.com/v1/albums/?market=ES"


@retry(
    retry_on_exception=(requests.HTTPError,),
    wait_exponential_multiplier=BASE_WAIT_TIME,
    stop_max_attempt_number=MAX_RETRIES,
)
def _do_post(url: str, headers: dict, body: dict) -> requests.Response:
    """Return the response of a POST request handling 500 response errors
    Args:
        url (str): url to generate the request
        headers (dict): header of the API request
        body (dict): body of the API request
    Returns:
        requests.Response: server's response to an HTTP POST request.
    """

    response = requests.post(url=url, headers=headers, data=body, timeout=30)
    response.raise_for_status()
    return response


@retry(
    retry_on_exception=(requests.HTTPError,),
    wait_exponential_multiplier=BASE_WAIT_TIME,
    stop_max_attempt_number=MAX_RETRIES,
)
def _do_get(url: str, token: str, params: dict = None) -> requests.Response:
    """Return the response of a GET request handling 500 response errors
    Args:
        url (str): url to generate the request
        token (str):
        params (dict) (optional):
    Returns:
        requests.Response: server's response to an HTTP POST request.
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url=url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response


def _get_access_token(
    client_id: str = SPOTIFY_CLIENT_ID, client_secret: str = SPOTIFY_CLIENT_SECRET
) -> str:
    """_summary_

    Args:
        client_id (str): _description_
        client_secret (str): _description_

    Returns:
        str: _description_
    """
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {"grant_type": "client_credentials"}
    response = _do_post(URL_TOKEN, headers, body)
    return response.json()["access_token"]


def _fetch_artists_data(artist_id: str, url: str, token: str) -> dict:
    """
    Fetch metadata for multiple artists from the Spotify API.

    This function extracts artist IDs from the given config dictionary,
    builds the request URL by appending the IDs, performs a GET request
    to the Spotify API using the provided token, and returns the parsed JSON response.

    Args:
        config (dict): Configuration dictionary containing an 'artists' key with a list of artist dictionaries.
        url (str): Base URL for the Spotify API endpoint (e.g., 'https://api.spotify.com/v1/artists?ids=').
        token (str): Bearer token used for authenticating the request.

    Returns:
        dict: Parsed JSON response from the Spotify API containing metadata for the requested artists.
    """
    params = {"ids": artist_id}
    artists_data = _do_get(url, token, params=params).json()
    return artists_data


def _parse_artist_data(
    raw_artist_data: Dict[str, Any], loaded_at: str
) -> Dict[str, list]:
    """
    Transforms raw Spotify artist data into a structured dictionary with artist IDs,
    full attributes, and a loaded_at field.

    Args:
        raw_artist_data (Dict[str, Any]): Dictionary with key 'artists' containing a list of artist data.
        loaded_at (str, optional): ISO formatted date string.

    Returns:
        Dict[str, list]: A dictionary with 'artist_id', 'attributes', and 'loaded_at' keys.
    """
    artists_attributes = raw_artist_data.get("artists", [])
    return {
        "artist_id": [artist["id"] for artist in artists_attributes],
        "attributes": [
            json.dumps(artists_attribute) for artists_attribute in artists_attributes
        ],
        "loaded_at": [loaded_at for _ in artists_attributes],
    }


def _fetch_tracks_data(artist_id: str, url: str, token: str):
    """
    Fetch top tracks for a given artist from the Spotify API.

    Args:
        artist_id (str): Spotify ID of the artist.
        url (str): URL template with a placeholder for the artist ID.
        token (str): OAuth access token.

    Returns:
        dict: Parsed JSON response containing the artist's top tracks.
    """
    url = url.format(artist_id=artist_id)
    track_data = _do_get(url, token).json()
    return track_data


def _parse_tracks_data(
    raw_track_data: Dict[str, Any], loaded_at: str
) -> Dict[str, list]:
    """
    Parse Spotify track data into a structured dictionary.

    Args:
        raw_track_data (Dict[str, Any]): Raw track data from Spotify API.
        loaded_at (str): ISO-formatted string indicating when the data was loaded.

    Returns:
        Dict[str,list]: Dictionary with track_id, serialized attributes,
                        custom attributes (e.g., rank), and loaded_at.
    """
    tracks_attributes = raw_track_data.get("tracks", [])
    return {
        "track_id": [track["id"] for track in tracks_attributes],
        "attributes": [
            json.dumps(tracks_attribute) for tracks_attribute in tracks_attributes
        ],
        "custom_attributes": [
            json.dumps({"rank_in_artist": i})
            for i in range(1, len(tracks_attributes) + 1)
        ],
        "loaded_at": [loaded_at for _ in tracks_attributes],
    }


def _fetch_albums_data(album_id: dict, url: str, token: str) -> dict:
    params = {"ids": album_id}
    albums_data = _do_get(url, token, params=params).json()
    return albums_data


def _parse_album_data(
    raw_album_data: Dict[str, Any], loaded_at: str
) -> Dict[str, list]:
    albums_attributes = raw_album_data.get("albums", [])
    return {
        "album_id": [album["id"] for album in albums_attributes],
        "attributes": [
            json.dumps(albums_attribute) for albums_attribute in albums_attributes
        ],
        "loaded_at": [loaded_at for _ in albums_attributes],
    }


def _generate_parsed_dataset(
    parsed_data: List[Dict[str, List[Any]]],
) -> Dict[str, List[Any]]:
    """
    Merge a list of parsed data dictionaries into a single dataset.

    Args:
        parsed_data (List[Dict[str, List[Any]]]): List of dictionaries with the same structure,
                                                    where each key maps to a list of values.

    Returns:
        Dict[str, List[Any]]: A single dictionary with combined lists for each key.
    """
    result = defaultdict(list)

    for single_entry in parsed_data:
        for key, values in single_entry.items():
            result[key].extend(values)

    return dict(result)


def _save_dict_to_csv(data: Dict[str, List[Any]], path: Path) -> None:
    """
    Save a dictionary of lists to a CSV file. Assumes all lists have the same length.
    Values (e.g., attributes) must be pre-processed (e.g., with json.dumps) before calling this function.

    Args:
        data (Dict[str, List[Any]]): Dictionary with columns as keys and lists of values.
        path (Path): Output CSV file path as a pathlib.Path object.
    """
    keys = list(data.keys())
    with path.open(mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for i in range(len(data[keys[0]])):
            writer.writerow({k: data[k][i] for k in keys})


def artist_to_csv(
    config: dict, loaded_at: str, url: str = URL_ARTIST, **kwargs
) -> None:
    """
    Fetches artist data from the Spotify API and saves it as a CSV file.

    The data is transformed into a standardized dictionary format, where each artist's attributes are serialized
    as JSON. The resulting file is saved to a path based on the provided configuration and `loaded_at` date.

    Args:
        config (dict): Configuration dictionary containing the 'raw_data_path' and artist list.
        loaded_at (str): Date string used to version the output file (e.g., '2025-01-01').
        url (str): Base URL for the Spotify API request.

    Returns:
        None
    """
    token = _get_access_token()
    artists_path = (
        Path(config.get("raw_data_path")) / f"artists/artists_data_{loaded_at}.csv"
    )
    artists_path.parent.mkdir(parents=True, exist_ok=True)
    artist_ids = [artist.get("id") for artist in config.get("artists")]
    parsed_data = _generate_parsed_dataset(
        [
            _parse_artist_data(_fetch_artists_data(artist_id, url, token), loaded_at)
            for artist_id in artist_ids
        ]
    )
    _save_dict_to_csv(parsed_data, artists_path.resolve())
    logger.info("Saved CSV to %s", artists_path)
    kwargs["ti"].xcom_push(key="artist_ids", value=parsed_data.get("artist_id"))


def tracks_to_csv(
    config: dict, artist_ids: List[str], loaded_at: str, url: str = URL_TRACK, **kwargs
) -> None:
    """
    Fetch and store track data for each artist into a CSV file.

    Args:
        config (dict): Dictionary containing 'raw_data_path'.
        artist_ids (List[str]): List of Spotify artist IDs.
        loaded_at (str): Date string used to version the output file.
        url (str, optional): URL template for fetching top tracks. Defaults to URL_TRACK.

    Returns:
        None
    """
    token = _get_access_token()
    tracks_path = (
        Path(config.get("raw_data_path")) / f"tracks/tracks_data_{loaded_at}.csv"
    )
    tracks_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_data = _generate_parsed_dataset(
        [
            _parse_tracks_data(_fetch_tracks_data(artist_id, url, token), loaded_at)
            for artist_id in artist_ids
        ]
    )
    _save_dict_to_csv(parsed_data, tracks_path.resolve())
    logger.info("Saved CSV to %s", tracks_path)
    albums_id = [
        json.loads(attribute).get("album").get("id")
        for attribute in parsed_data.get("attributes")
    ]
    kwargs["ti"].xcom_push(key="album_ids", value=list(set(albums_id)))


def albums_to_csv(
    config: dict, album_ids: List[str], loaded_at: str, url: str = URL_ALBUM, **kwargs
) -> None:
    """
    Fetch and store album data for a list of album IDs into a CSV file.

    Args:
        config (dict): Dictionary containing 'raw_data_path'.
        album_ids (List[str]): List of Spotify album IDs.
        loaded_at (str): Date string used to version the output file.
        url (str, optional): Spotify API endpoint for albums. Defaults to URL_ALBUM.

    Returns:
        None
    """
    token = _get_access_token()
    albums_path = (
        Path(config.get("raw_data_path")) / f"albums/albums_data_{loaded_at}.csv"
    )
    albums_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_data = _generate_parsed_dataset(
        [
            _parse_album_data(_fetch_albums_data(album_id, url, token), loaded_at)
            for album_id in album_ids
        ]
    )
    _save_dict_to_csv(parsed_data, albums_path.resolve())
    logger.info("Saved CSV to %s", albums_path)
