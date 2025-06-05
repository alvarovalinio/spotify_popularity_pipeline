WITH tmp_dim_track AS (
SELECT
  track_id,
  CAST(attibutes.name AS VARCHAR) AS track_name,
  CAST(attibutes.duration_ms AS INT) AS duration_ms,
  CAST(attibutes.explicit AS BOOLEAN) AS is_explicit
FROM {{ ref('stg_track') }} 
)

SELECT 
  track_id,
  TRIM(BOTH '"' FROM track_name) AS track_name,
  duration_ms,
  is_explicit
FROM tmp_dim_track
