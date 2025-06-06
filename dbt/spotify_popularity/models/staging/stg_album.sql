WITH raw_album AS (
SELECT 
  album_id,
  attributes,
  loaded_at
FROM read_csv(
  "../../data/raw/albums/albums_data_{{ var('start_date') }}.csv",
   header = true, 
   all_varchar = True)
)

SELECT
  album_id,
  CAST(attributes AS JSON) AS attibutes,
  CAST(loaded_at AS DATE) AS loaded_at
FROM raw_album
