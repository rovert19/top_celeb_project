SELECT
    name,
    gender,
    cardinality(array_remove(rank_array, NULL::INT)) as weeks_top
FROM celebs_cum
WHERE period = EXTRACT(year FROM DATE('2024-12-30'))::INT + ( --current_date
    CASE
        WHEN EXTRACT(week FROM DATE('2024-12-30'))::INT = 1 AND 
            EXTRACT(month FROM DATE('2024-12-30'))::INT = 12 THEN 1
        ELSE 0
    END)
ORDER BY weeks_top DESC
LIMIT 10;