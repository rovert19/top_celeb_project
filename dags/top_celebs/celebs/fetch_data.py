from airflow.providers.http.hooks.http import HttpAsyncHook
from datetime import datetime
import asyncio

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


async def request_movies_of_celeb(async_hook: HttpAsyncHook, id_celeb):
    url_movie_celeb = URL_MOVIE_CELEB.format(str(id_celeb))
    await asyncio.sleep(1)
    response = await async_hook.run(url_movie_celeb, headers=HEADERS)
    celeb_data = await response.json()
    movies = celeb_data["cast"]
    movies_ids = [movie_info["id"] for movie_info in movies]
    return movies_ids


async def request_series_of_celeb(async_hook: HttpAsyncHook, id_celeb):
    url_series_celeb = URL_SERIES_CELEB.format(str(id_celeb))
    await asyncio.sleep(1)
    response = await async_hook.run(url_series_celeb, headers=HEADERS)
    celeb_data = await response.json()
    series = celeb_data["cast"]
    series_ids = [series_info["id"] for series_info in series]
    return series_ids


async def request_celeb_info(async_hook: HttpAsyncHook, celeb):
    url_celeb = URL_SEARCH_CELEB.format(celeb[1])
    await asyncio.sleep(1)
    response = await async_hook.run(url_celeb, headers=HEADERS)
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


def batch_request_top_celebs_info(top_celebs):
    async_hook = HttpAsyncHook(method="GET", http_conn_id="TMDB_API")
    loop = asyncio.get_event_loop()

    tasks = [request_celeb_info(async_hook, celeb) for celeb in top_celebs]
    top_celebs_info = loop.run_until_complete(asyncio.gather(*tasks))

    return top_celebs_info


def batch_request_movies_celeb(top_celebs_info):
    async_hook = HttpAsyncHook(method="GET", http_conn_id="TMDB_API")
    loop = asyncio.get_event_loop()

    tasks = [request_movies_of_celeb(async_hook, celeb["id"]) for celeb in top_celebs_info]
    movies_per_celeb = loop.run_until_complete(asyncio.gather(*tasks))

    celeb_movie_ids = []
    movies_ids = []
    for index, cast_celeb in enumerate(movies_per_celeb):
        celeb_id = top_celebs_info[index]["id"]

        celeb_movie_pairs = tuple(zip(celeb_id * len(cast_celeb[0]), cast_celeb[0]))
        celeb_movie_ids.append(celeb_movie_pairs)

        movies_ids.extend(cast_celeb[0])
    
    return celeb_movie_ids, movies_ids


def batch_request_series_celeb(top_celebs_info):
    async_hook = HttpAsyncHook(method="GET", http_conn_id="TMDB_API")
    loop = asyncio.get_event_loop()

    tasks = [request_series_of_celeb(async_hook, celeb["id"]) for celeb in top_celebs_info]
    series_per_celeb = loop.run_until_complete(asyncio.gather(*tasks))

    celeb_series_ids = []
    series_ids = []
    
    for index, cast_celeb in enumerate(series_per_celeb):
        celeb_id = top_celebs_info[index]["id"]

        celeb_series_pairs = tuple(zip(celeb_id * len(cast_celeb[1]), cast_celeb[1]))
        celeb_series_ids.append(celeb_series_pairs)

        series_ids.extend(cast_celeb[1])

    return celeb_series_ids, series_ids

