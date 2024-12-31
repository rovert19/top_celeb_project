from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_movies(ti):
    movies_info = ti.xcom_pull(key="movies_info", task_ids="request_movies")
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN")

    movies_rows = []
    for movie in movies_info:
        movies_rows.append(tuple(movie.values()))

    print(len(movies_rows))
    pg_hook.insert_rows(
        "movies", 
        movies_rows, 
        replace=True, 
        replace_index="id",
        target_fields=["id", "title", "genres", "origin", "release_date", "status", "score"]
    )
    # with pg_hook.get_cursor() as cursor:
    #     execute_values(
    #         cursor,
    #         """
    #         INSERT INTO movies (id, title, genres, origin, release_date, status, score) VALUES %s 
    #         ON CONFLICT (id) DO UPDATE
    #         SET 
    #             release_date = EXCLUDED.release_date,
    #             status = EXCLUDED.status,
    #             score = EXCLUDED.score
    #         """,
    #         movies_rows
    #     )