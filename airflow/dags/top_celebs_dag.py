from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, date
from dags.top_celebs.scrapping.extract import extract_celebs
from dags.top_celebs.celebs.fetch_data import batch_request_top_celebs_info, batch_request_movies_celeb, batch_request_series_celeb
from dags.top_celebs.celebs.load import load_top_celebs, load_celeb_series, load_celeb_movies
from dags.top_celebs.movie.fetch_data import batch_request_movies_info
from dags.top_celebs.movie.load import load_movies
from dags.top_celebs.series.fetch_data import batch_request_series_info
from dags.top_celebs.series.load import load_series


@dag(
    dag_id="etl_top_celebs",
    start_date=datetime(2024, 12, 31),
    schedule="0 5 * * MON",
    template_searchpath= ['/usr/local/airflow/include/sql'],
    catchup=False
)
def etl_top_celebs():
    extract_top_celebs = PythonOperator(
        task_id="extract_top_celebs",
        python_callable=extract_celebs
    )

    request_top_celebs = PythonOperator(
        task_id="request_top_celebs", 
        python_callable=batch_request_top_celebs_info
    )

    request_movies_celebs = PythonOperator(
        task_id="request_movies_celebs", 
        python_callable=batch_request_movies_celeb
    )
    request_series_celebs = PythonOperator(
        task_id="request_series_celebs", 
        python_callable=batch_request_series_celeb
    )

    insert_top_celebs = PythonOperator(
        task_id="load_top_celebs", 
        python_callable=load_top_celebs
    )
    insert_celeb_series = PythonOperator(
        task_id="load_celeb_series", 
        python_callable=load_celeb_series
    )
    insert_celeb_movies = PythonOperator(
        task_id="load_celeb_movies", 
        python_callable=load_celeb_movies
    )

    request_series = PythonOperator(
        task_id="request_series", 
        python_callable=batch_request_series_info
    )
    insert_series = PythonOperator(
        task_id="load_series", 
        python_callable=load_series
    )

    request_movies = PythonOperator(
        task_id="request_movies", 
        python_callable=batch_request_movies_info
    )
    insert_movies = PythonOperator(
        task_id="load_movies", 
        python_callable=load_movies
    )

    celeb_cumulative_data = SQLExecuteQueryOperator(
        task_id="celeb_cumulative_data",
        conn_id="PG_CONN",
        # parameters= { "current_date": date.today() },
        sql="/celeb_cum_query.sql"
    )

    celeb_top_10_week_data = SQLExecuteQueryOperator(
        task_id="celeb_top_10_week",
        conn_id="PG_CONN",
        # parameters= { "current_date": date.today() },
        sql="/celeb_top_10_week.sql"
    )

    celeb_leader_week_data = SQLExecuteQueryOperator(
        task_id="celeb_leader_week",
        conn_id="PG_CONN",
        # parameters= { "current_date": date.today() },
        sql="/celeb_leader_week.sql"
    )

    celeb_most_top_weeks_data = SQLExecuteQueryOperator(
        task_id="celeb_most_top_weeks",
        conn_id="PG_CONN",
        # parameters= { "current_date": date.today() },
        sql="/celeb_total_weeks.sql"
    )

    extract_top_celebs >> request_top_celebs >> [insert_top_celebs, request_movies_celebs, request_series_celebs]
    insert_top_celebs >> celeb_cumulative_data
    request_movies_celebs >> [ insert_celeb_movies, request_movies ]
    request_movies >> insert_movies
    request_series_celebs >> [ insert_celeb_series, request_series ]
    request_series >> insert_series

    celeb_cumulative_data >> [celeb_top_10_week_data, celeb_most_top_weeks_data]
    [celeb_cumulative_data, insert_celeb_movies, insert_celeb_series, insert_movies, insert_series] >> celeb_leader_week_data

etl_top_celebs()

#
#
#                               --------------> insert_top_celebs
#                               |
#  extract_celeb ----> request_top_celebs ----> request_movies_celebs ----> request_movies ----> insert_movies
#                               |                         |
#                               |                         ----------------> insert_celeb_movies
#                               |
#                               --------------> request_series_celebs ----> request_series ----> insert_series
#                                                         |
#                                                         ----------------> insert_celeb_series