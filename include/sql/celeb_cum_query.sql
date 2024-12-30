WITH last_celeb_cum AS (
    SELECT *
    FROM celebs_cum
    WHERE updated_at = {{ params.current_date }} - INTERVAL '7 days'
),
current_top_celeb AS (
    SELECT 
        *,
        CASE
            WHEN EXTRACT(week FROM ingest_at)::INT = 1 AND EXTRACT(month FROM ingest_at)::INT = 12
                THEN EXTRACT(year FROM ingest_at)::INT + 1
            ELSE EXTRACT(year FROM ingest_at)::INT
        END as period
    FROM celebs_raw
    WHERE ingest_at = {{ params.current_date }}
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
    CASE
        WHEN lc.period != ct.period
            THEN ARRAY[COALESCE(ct.rank, NULL::INT)]
        ELSE
            COALESCE(lc.rank_array, 
                ARRAY_FILL(NULL::INT, ARRAY[EXTRACT(week FROM ingest_at)::INT - 1]) --fecha inicial del dag
            ) || ARRAY[COALESCE(ct.rank, NULL::INT)]
    END as rank_array,
    ct.period as period,
    ingest_at as updated_at
FROM last_celeb_cum as lc
FULL OUTER JOIN current_top_celeb as ct
ON lc.id = ct.id


-- INSERT INTO celebs_cum
-- SELECT
--     COALESCE(ct.id, lc.id) as id,
--     COALESCE(ct.name, lc.name) as name,
--     COALESCE(ct.imdb_id, lc.imdb_id) as imdb_id,
--     COALESCE(ct.roles, lc.roles) as roles,
--     COALESCE(ct.gender, lc.gender) as gender,
--     COALESCE(ct.known_for, lc.known_for) as known_for,
--     CASE 
--         WHEN ct.id IS NULL THEN NULL::INT 
--         ELSE ct.rank
--     END as rank,
--     COALESCE(lc.rank_array, 
--         ARRAY_FILL(NULL::INT, ARRAY[(EXTRACT(week FROM ingest_at) - EXTRACT(week FROM DATE('2024-12-16')))::INT]) --fecha inicial del dag
--     ) || ARRAY[COALESCE(ct.rank, NULL::INT)] as rank_array,

--     EXTRACT(year FROM DATE('2024-12-23'))::INT as year,
--     DATE('2024-12-23') as updated_at
-- FROM last_celeb_cum as lc
-- FULL OUTER JOIN current_top_celeb as ct
-- ON lc.id = ct.id