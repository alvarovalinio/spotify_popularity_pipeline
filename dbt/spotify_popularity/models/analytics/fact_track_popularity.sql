WITH tmp_fact_track_popularity AS (
SELECT 
  track_id,
  CAST(attibutes.album.id AS VARCHAR) AS album_id,
  CAST(attibutes.artists AS JSON[]) AS artists,
  loaded_at,
  CAST(attibutes.popularity AS INT) AS popularity,
  CAST(custom_attibutes.rank_in_artist AS INT) rank_in_artist
FROM {{ ref('stg_track') }}
), 
tmp_distinct_artist_ids AS (
  SELECT 
    DISTINCT artist_id
  FROM {{ ref('dim_artist') }}
), tmp_transform_fact_track_popularity AS (
SELECT 
  track_id,
  TRIM(BOTH '"' FROM album_id) AS album_id,
  TRIM(BOTH '"' FROM CAST(j.id AS VARCHAR)) AS artist_id,
  loaded_at,
  popularity,
  rank_in_artist
FROM tmp_fact_track_popularity,
LATERAL UNNEST(artists) AS t(j)
WHERE 
  artist_id IN (SELECT artist_id FROM tmp_distinct_artist_ids)
)

SELECT
{{ dbt_utils.generate_surrogate_key(['track_id', 'album_id', 'artist_id', 'loaded_at']) }} AS track_popularity_id,
*
FROM tmp_transform_fact_track_popularity