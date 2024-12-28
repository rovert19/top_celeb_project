from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def load_top_celebs(top_celebs_info):
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN", schema="public")
    
    top_celebs_rows = []
    for celeb in top_celebs_info:
        top_celebs_rows.append(tuple(celeb.values))
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                "INSERT INTO celebs_raw (id, name, imdb_id, roles, gender, known_for, rank, ingest_at) VALUES %s",
                top_celebs_rows
            )
        conn.close()


def load_celeb_movies(celeb_movie_ids):
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN", schema="public")

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                "INSERT INTO celeb_movies (id, movie_id) VALUES %s ON CONFLICT (id, movie_id) DO NOTHING",
                celeb_movie_ids
            )
        conn.close()


def load_celeb_series(celeb_series_ids):
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN", schema="public")

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                "INSERT INTO celeb_series (id, series_id) VALUES %s ON CONFLICT (id, series_id) DO NOTHING",
                celeb_series_ids
            )
        conn.close()
