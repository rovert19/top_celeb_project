DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS series;
CREATE TABLE IF NOT EXISTS movies (
    id INT,
    title TEXT,
    genres TEXT[],
    origin TEXT,
    release_date DATE,
    status TEXT,
    score FLOAT,
    -- ingest_at DATE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS series (
    id INT,
    title TEXT,
    genres TEXT[],
    origin TEXT,
    release_date DATE,
    seasons INT,
    status TEXT,
    score FLOAT,
    -- ingest_at DATE,
    PRIMARY KEY (id)
);