WITH last_celeb_cum AS (
    SELECT *
    FROM celebs_cum
    WHERE updated_at = DATE('2024-12-16')
),
current_top_celeb AS (
    SELECT *
    FROM celebs_raw
    WHERE ingest_at = DATE('2024-12-23')
)

INSERT INTO celebs_cum
SELECT
    COALESCE(ct.id, lc.id) as id,
    COALESCE(ct.name, lc.name) as name,
    COALESCE(ct.imdb_id, lc.imdb_id) as imdb_id,
    COALESCE(ct.roles, lc.roles) as roles,
    COALESCE(ct.gender, lc.gender) as gender,
    COALESCE(ct.known_for, lc.known_for) as known_for,
    CASE 
        WHEN ct.id IS NULL THEN NULL::INT 
        ELSE ct.rank
    END as rank,
    COALESCE(lc.rank_array, 
    EXTRACT(year FROM DATE('2024-12-23'))::INT as year,
        ARRAY_FILL(NULL::INT, ARRAY[(EXTRACT(week FROM ingest_at) - EXTRACT(week FROM DATE('2024-12-16')))::INT]) --fecha inicial del dag
    ) || ARRAY[COALESCE(ct.rank, NULL::INT)] as rank_array,
    DATE('2024-12-23') as updated_at
FROM last_celeb_cum as lc
FULL OUTER JOIN current_top_celeb as ct
ON lc.id = ct.id