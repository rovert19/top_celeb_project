from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook
from datetime import datetime
import asyncio
import aiohttp

from include.constants import HEADERS, URL_MOVIE_CELEB, URL_SEARCH_CELEB, URL_SERIES_CELEB

def get_celeb_info(data): 
    celeb_data = data["results"][0]
    celeb_info = {
        "id": celeb_data["id"],
        "name": celeb_data["name"],
        "gender": int(celeb_data["gender"]),
        "known_for": [
            [str(film["id"]), film["media_type"]] for film in celeb_data["known_for"]],
    }
    return celeb_info 


async def request_movies_of_celeb(celeb_id):
    url_movie_celeb = URL_MOVIE_CELEB.format(str(celeb_id))

    async with aiohttp.ClientSession() as session:
        async with session.get(url_movie_celeb, headers=HEADERS) as response:
            await asyncio.sleep(1)
            celeb_data = await response.json()
            movies = celeb_data["cast"]
            movies_ids = [movie_info["id"] for movie_info in movies]
            return movies_ids


async def request_series_of_celeb(celeb_id):
    url_series_celeb = URL_SERIES_CELEB.format(str(celeb_id))

    async with aiohttp.ClientSession() as session:
        async with session.get(url_series_celeb, headers=HEADERS) as response:
            await asyncio.sleep(1)
            celeb_data = await response.json()
            series = celeb_data["cast"]
            series_ids = [series_info["id"] for series_info in series]
            return series_ids


async def request_celeb_info(celeb):
    url_celeb = URL_SEARCH_CELEB.format(celeb[1])

    async with aiohttp.ClientSession() as session:
        async with session.get(url_celeb, headers=HEADERS) as response:
            await asyncio.sleep(1)
            celeb_data = await response.json()
            celeb_info = get_celeb_info(celeb_data)

            return {
                "id": celeb_info["id"],
                "name": celeb_info["name"],
                "imdb_id": celeb[0],
                "roles": celeb[2],
                "gender": celeb_info["gender"],
                "known_for": celeb_info["known_for"],
                "rank": celeb[3],
                "ingest_at": datetime(2024, 12, 23).isoformat()
            }


def batch_request_top_celebs_info(ti):
    top_celebs = ti.xcom_pull(key="celebs", task_ids="extract_top_celebs")
    loop = asyncio.get_event_loop()

    tasks = [ request_celeb_info(celeb) for celeb in top_celebs ]
    top_celebs_info = loop.run_until_complete(asyncio.gather(*tasks))

    print(len(top_celebs_info))
    ti.xcom_push(key='top_celebs_info', value = top_celebs_info)


def batch_request_movies_celeb(ti):
    top_celebs_info = ti.xcom_pull(key="top_celebs_info", task_ids="request_top_celebs")
    loop = asyncio.get_event_loop()

    tasks = [ request_movies_of_celeb(celeb["id"]) for celeb in top_celebs_info ]
    movies_per_celeb = loop.run_until_complete(asyncio.gather(*tasks))    

    celeb_movie_ids = []
    movies_ids = []

    print(len(movies_per_celeb))
    for index, cast_celeb in enumerate(movies_per_celeb):
        celeb_id = top_celebs_info[index]["id"]

        celeb_movie_pairs = list(zip([celeb_id] * len(cast_celeb), cast_celeb))
        celeb_movie_ids.extend(celeb_movie_pairs)
        movies_ids.extend(cast_celeb)

    ti.xcom_push(key='celeb_movie_ids', value= celeb_movie_ids)
    ti.xcom_push(key='movies_ids', value= set(movies_ids))


def batch_request_series_celeb(ti):
    top_celebs_info = ti.xcom_pull(key="top_celebs_info", task_ids="request_top_celebs")
    loop = asyncio.get_event_loop()

    tasks = [ request_series_of_celeb(celeb["id"]) for celeb in top_celebs_info ]
    series_per_celeb = loop.run_until_complete(asyncio.gather(*tasks))    

    celeb_series_ids = []
    series_ids = []

    print(len(series_per_celeb))
    for index, cast_celeb in enumerate(series_per_celeb):
        celeb_id = top_celebs_info[index]["id"]

        celeb_series_pairs = list(zip([celeb_id] * len(cast_celeb), cast_celeb))
        celeb_series_ids.extend(celeb_series_pairs)
        series_ids.extend(cast_celeb)

    ti.xcom_push(key='celeb_series_ids', value= celeb_series_ids)
    ti.xcom_push(key='series_ids', value= set(series_ids))

