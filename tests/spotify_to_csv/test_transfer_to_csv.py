import pytest
from unittest.mock import patch, Mock
import json
import os

os.environ["SPOTIFY_CLIENT_ID"] = "fake_id"
os.environ["SPOTIFY_CLIENT_SECRET"] = "fake_secret"

from dags.spotify_to_csv.transfer_to_csv import (
    _get_access_token,
    _fetch_artists_data,
    _parse_artist_data,
    _fetch_tracks_data,
    _parse_tracks_data,
    _fetch_albums_data,
    _parse_album_data,
    _generate_parsed_dataset,
    _save_dict_to_csv,
)


@pytest.fixture
def fake_token():
    return "fake_token"


@pytest.fixture
def fake_artist_data():
    return {
        "artists": [
            {"id": "123", "name": "Artist A"},
            {"id": "456", "name": "Artist B"},
        ]
    }


@pytest.fixture
def fake_track_data():
    return {
        "tracks": [
            {"id": "t1", "name": "Track 1", "album": {"id": "a1"}},
            {"id": "t2", "name": "Track 2", "album": {"id": "a2"}},
        ]
    }


@pytest.fixture
def fake_album_data():
    return {
        "albums": [{"id": "a1", "name": "Album A"}, {"id": "a2", "name": "Album B"}]
    }


def test_get_access_token():
    with patch("dags.spotify_to_csv.transfer_to_csv._do_post") as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test_token"}
        mock_post.return_value = mock_response

        token = _get_access_token("id", "secret")
        assert token == "test_token"


def test_fetch_artists_data(fake_token):
    with patch("dags.spotify_to_csv.transfer_to_csv._do_get") as mock_get:
        mock_get.return_value.json.return_value = {"artists": [{"id": "123"}]}
        result = _fetch_artists_data("123", "url", fake_token)
        assert result["artists"][0]["id"] == "123"


def test_parse_artist_data(fake_artist_data):
    loaded_at = "2025-06-01"
    parsed = _parse_artist_data(fake_artist_data, loaded_at)
    assert parsed["artist_id"] == ["123", "456"]
    assert len(parsed["attributes"]) == 2
    assert parsed["loaded_at"] == [loaded_at, loaded_at]


def test_fetch_tracks_data(fake_token):
    with patch("dags.spotify_to_csv.transfer_to_csv._do_get") as mock_get:
        mock_get.return_value.json.return_value = {"tracks": [{"id": "t1"}]}
        result = _fetch_tracks_data("artist_id", "url/{artist_id}", fake_token)
        assert result["tracks"][0]["id"] == "t1"


def test_parse_tracks_data(fake_track_data):
    loaded_at = "2025-06-01"
    parsed = _parse_tracks_data(fake_track_data, loaded_at)
    assert parsed["track_id"] == ["t1", "t2"]
    assert parsed["loaded_at"] == [loaded_at, loaded_at]
    assert json.loads(parsed["custom_attributes"][0])["rank_in_artist"] == 1


def test_fetch_albums_data(fake_token):
    with patch("dags.spotify_to_csv.transfer_to_csv._do_get") as mock_get:
        mock_get.return_value.json.return_value = {"albums": [{"id": "a1"}]}
        result = _fetch_albums_data("a1", "url", fake_token)
        assert result["albums"][0]["id"] == "a1"


def test_parse_album_data(fake_album_data):
    loaded_at = "2025-06-01"
    parsed = _parse_album_data(fake_album_data, loaded_at)
    assert parsed["album_id"] == ["a1", "a2"]
    assert len(parsed["attributes"]) == 2
    assert parsed["loaded_at"] == [loaded_at, loaded_at]


def test_generate_parsed_dataset():
    data = [{"id": ["1"], "val": ["a"]}, {"id": ["2"], "val": ["b"]}]
    result = _generate_parsed_dataset(data)
    assert result == {"id": ["1", "2"], "val": ["a", "b"]}


def test_save_dict_to_csv(tmp_path):
    data = {"col1": ["a", "b"], "col2": ["1", "2"]}
    file_path = tmp_path / "test.csv"
    _save_dict_to_csv(data, file_path)
    lines = file_path.read_text().splitlines()
    assert lines[0] == "col1,col2"
    assert "a,1" in lines
    assert "b,2" in lines
