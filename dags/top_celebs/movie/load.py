from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_movies(movies_info):
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN", schema="public")

    movies_rows = []
    for movie in movies_info:
        movies_rows.append(tuple(movie.values))

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO movies (id, title, genres, origin, release_date, status, score) VALUES %s 
                ON CONFLICT (id) DO UPDATE
                SET 
                    release_date = EXCLUDED.release_date,
                    status = EXCLUDED.status,
                    score = EXCLUDED.score
                """,
                movies_rows
            )
        conn.close()