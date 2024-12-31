DROP TABLE IF EXISTS celebs_cum;
CREATE TABLE IF NOT EXISTS celebs_cum (
    id INT,
    name TEXT,
    imdb_id TEXT,
    roles TEXT[],
    gender INT,
    known_for TEXT[],
    rank INT,
    rank_array INT[],
    --total_reparto INT,
    --reparto_array TEXT[],
    -- start_date DATE,
    period INT,
    updated_at DATE,
    PRIMARY KEY(id, updated_at)
)