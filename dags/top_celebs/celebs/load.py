from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def load_top_celebs(ti):
    top_celebs_info = ti.xcom_pull(key='top_celebs_info', task_ids='request_top_celebs')
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN")
    
    top_celebs_rows = []
    for celeb in top_celebs_info:
        top_celebs_rows.append(tuple(celeb.values()))
    
    print(len(top_celebs_rows))
    pg_hook.insert_rows("celebs_raw", top_celebs_rows)
    # with pg_hook.get_conn() as conn: 
    #     with conn.cursor() as cursor:
    #         execute_values(
    #             cursor,
    #             "INSERT INTO celebs_raw (id, name, imdb_id, roles, gender, known_for, rank, ingest_at) VALUES %s",
    #             top_celebs_rows
    #         )
    #     conn.close()


def load_celeb_movies(ti):
    celeb_movie_ids = ti.xcom_pull(key='celeb_movie_ids', task_ids='request_movies_celebs')
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN")

    celeb_movie_ids = [tuple(celeb_movie_id) for celeb_movie_id in celeb_movie_ids]
    print(len(celeb_movie_ids))
    pg_hook.insert_rows("celeb_movies", celeb_movie_ids, replace=False)
    # with pg_hook.get_cursor() as cursor:
    #     execute_values(
    #         cursor,
    #         "INSERT INTO celeb_movies (id, movie_id) VALUES %s ON CONFLICT (id, movie_id) DO NOTHING",
    #         celeb_movie_ids
    #     )


def load_celeb_series(ti):
    celeb_series_ids = ti.xcom_pull(key='celeb_series_ids', task_ids='request_series_celebs')
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN")
    
    celeb_series_ids = [tuple(celeb_series_id) for celeb_series_id in celeb_series_ids]
    print(len(celeb_series_ids))
    pg_hook.insert_rows("celeb_series", celeb_series_ids, replace=False)
    # with pg_hook.get_cursor() as cursor:
    #     execute_values(
    #         cursor,
    #         "INSERT INTO celeb_series (id, series_id) VALUES %s ON CONFLICT (id, series_id) DO NOTHING",
    #         celeb_series_ids
    #     )
