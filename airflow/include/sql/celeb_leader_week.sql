WITH total_cast_movie as (
    SELECT
        cm.id,
        count(m.id) as total_movies
    FROM celeb_movies as cm
    JOIN movies as m
        ON cm.movie_id = m.id
    WHERE m.status = 'Released'
    GROUP BY cm.id
), 
avg_cast_movie as (
    SELECT
        cm.id,
        avg(m.id) as avg_score_movies
    FROM celeb_movies as cm
    JOIN movies as m
        ON cm.movie_id = m.id
    WHERE m.status = 'Released' AND m.score != 0
    GROUP BY cm.id
), 
total_cast_series as (
    SELECT
        cs.id,
        count(s.id) as total_series
    FROM celeb_series as cs
    JOIN series as s
        ON cs.series_id = s.id
    WHERE s.status = 'Released'
    GROUP BY cs.id
), 
avg_cast_series as (
    SELECT
        cs.id,
        avg(s.id) as avg_score_series
    FROM celeb_series as cs
    JOIN series as s
        ON cs.series_id = s.id
    WHERE s.status = 'Released' AND s.score != 0
    GROUP BY cs.id
),
celeb_movie_agg as (
    SELECT
        tc.id,
        tc.total_movies,
        ac.avg_score_movies
    FROM total_cast_movie as tc
    JOIN avg_cast_movie as ac
    ON tc.id = ac.id
),
celeb_series_agg as (
    SELECT
        tc.id,
        tc.total_series,
        ac.avg_score_series
    FROM total_cast_series as tc
    JOIN avg_cast_series as ac
    ON tc.id = ac.id
), 
celeb_leader as (
    SELECT *
    FROM celebs_cum
    WHERE rank = 1 AND 
        EXTRACT(week FROM updated_at) = EXTRACT(week FROM DATE('2024-12-30'))
),
celeb_leader_agg as (
    SELECT *
    FROM celeb_leader cl
    JOIN celeb_movie_agg cm
        ON cl.id = cm.id
    JOIN celeb_series_agg cs
        ON cl.id = cs.id
)

SELECT *
FROM celeb_leader_agg;