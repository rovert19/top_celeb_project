WITH celeb_top_10 as (
    SELECT
        id,
        name,
        gender,
        known_for
    FROM celebs_cum
    WHERE EXTRACT(week FROM updated_at) = EXTRACT(week FROM DATE('2024-12-30')) AND 
        rank IS NOT NULL AND rank <= 10
    ORDER BY rank ASC
)

SELECT * 
FROM celeb_top_10;