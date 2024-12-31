DROP TABLE IF EXISTS celebs_raw;
-- DROP TABLE IF EXISTS celebs_info_raw;
CREATE TABLE IF NOT EXISTS celebs_raw (
    id INT,
    name TEXT,
    imdb_id TEXT,
    roles TEXT[],
    gender INT,
    known_for TEXT[][],
    rank INT,
    ingest_at DATE,
    PRIMARY KEY (id, ingest_at) 
);


-- CREATE TABLE IF NOT EXISTS celebs_info_raw (
--     id TEXT,
--     known_array TEXT[],
--     total_films INT,
--     films_array TEXT[][],
--     ingest_at DATE,
--     PRIMARY KEY(id, ingest_at)
-- )