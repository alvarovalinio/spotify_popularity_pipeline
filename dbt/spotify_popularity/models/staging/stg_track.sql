WITH raw_track AS (
SELECT 
  track_id,
  attributes,
  custom_attributes,
  loaded_at
FROM read_csv(
  "../../data/raw/tracks/tracks_data_{{ var('start_date') }}.csv",
   header = true, 
   all_varchar = True)
)

SELECT
  track_id,
  CAST(attributes AS JSON) AS attibutes,
  CAST(custom_attributes AS JSON) AS custom_attibutes,
  loaded_at
FROM raw_track


