WITH tmp_dim_artist AS (
SELECT 
  artist_id,
  CAST(attibutes.name AS VARCHAR) AS artist_name,
  CAST(attibutes.genres AS VARCHAR[]) AS genres,
  CAST(attibutes.followers.total AS INT) AS followers,
  CAST(attibutes.popularity AS INT) AS popularity
FROM {{ ref('stg_artist') }}
  )

SELECT
  artist_id,
  TRIM(BOTH '"' FROM artist_name) AS artist_name,
  CASE WHEN len(genres) > 0 THEN genres END AS genres,
  followers,
  popularity
FROM tmp_dim_artist
