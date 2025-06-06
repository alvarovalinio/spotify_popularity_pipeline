SELECT 
    DATE_TRUNC('week',f.loaded_at) AS 'Loaded At', 
    a.artist_name AS 'Artist Name', 
    AVG(f.popularity) AS 'Average Popularity'
FROM analytics.fact_track_popularity f
INNER JOIN analytics.dim_artist a USING (artist_id)
WHERE
    a.artist_name IN ({selected_artists})
GROUP BY 
    1,2
ORDER BY 1,2