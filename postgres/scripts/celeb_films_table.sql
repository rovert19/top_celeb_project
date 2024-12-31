DROP TABLE IF EXISTS celeb_movies;
DROP TABLE IF EXISTS celeb_series;

CREATE TABLE IF NOT EXISTS celeb_movies (
    id INT,
    movie_id INT,
    PRIMARY KEY(id, movie_id)
);

CREATE TABLE IF NOT EXISTS celeb_series (
    id INT,
    series_id INT,
    PRIMARY KEY(id, series_id)
);