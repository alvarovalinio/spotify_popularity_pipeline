SELECT 
    a.artist_name AS 'Artist Name', 
    f.popularity AS 'Popularity'
FROM analytics.fact_track_popularity f
INNER JOIN analytics.dim_artist a USING (artist_id)
WHERE
    f.loaded_at = '{selected_date}'
    AND a.artist_name IN ({selected_artists})