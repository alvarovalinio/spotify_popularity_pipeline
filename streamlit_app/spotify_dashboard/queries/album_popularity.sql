SELECT 
    d.album_name AS 'Album Name', 
    d.popularity AS 'Album Popularity',
    a.artist_name AS 'Artist Name'
FROM analytics.fact_track_popularity f
INNER JOIN analytics.dim_album d USING (album_id)
INNER JOIN analytics.dim_artist a USING (artist_id)
WHERE 
    f.loaded_at = '{selected_date}'
    AND a.artist_name IN ({selected_artists})
GROUP BY 
    1,2,3
ORDER BY 
    2 DESC
LIMIT 10