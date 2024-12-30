from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_series(ti):
    all_series_info = ti.xcom_pull(key="all_series_info", task_ids="request_series")
    pg_hook = PostgresHook(postgres_conn_id="PG_CONN")

    series_rows = []
    for series in all_series_info:
        series_rows.append(tuple(series.values()))

    with pg_hook.get_cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO series (id, title, genres, origin, release_date, seasons, status, score) VALUES %s 
            ON CONFLICT (id) DO UPDATE
            SET
                release_date = EXCLUDED.release_date,
                seasons = EXCLUDED.seasons,
                status = EXCLUDED.status,
                score = EXCLUDED.score
            """,
            series_rows
        )