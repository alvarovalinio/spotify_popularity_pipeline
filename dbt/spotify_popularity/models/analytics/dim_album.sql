{{ config(unique_key = 'album_id') }}

WITH tmp_dim_album AS (
SELECT
  album_id,
  CAST(attibutes.name AS VARCHAR) AS album_name,
  CAST(attibutes.album_type AS VARCHAR) AS album_type,
  CAST(attibutes.release_date AS VARCHAR) AS released_at,
  CAST(attibutes.total_tracks AS INT) AS total_tracks,
  CAST(attibutes.popularity AS INT) AS popularity
FROM {{ ref('stg_album') }}
)

SELECT
  album_id,
  TRIM(BOTH '"' FROM album_name) AS album_name,
  TRIM(BOTH '"' FROM album_type) AS album_type,
  CAST(LEFT(TRIM(BOTH '"' FROM released_at),4) AS INT) released_at_year,
  total_tracks,
  popularity
FROM tmp_dim_album
