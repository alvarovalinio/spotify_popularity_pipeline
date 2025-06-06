WITH raw_artist AS (
SELECT 
  artist_id,
  attributes,
  loaded_at
FROM read_csv(
  "../../data/raw/artists/artists_data_{{ var('start_date') }}.csv",
   header = true, 
   all_varchar = True)
)

SELECT
  artist_id,
  CAST(attributes AS JSON) AS attibutes,
  CAST(loaded_at AS DATE) AS loaded_at
FROM raw_artist

