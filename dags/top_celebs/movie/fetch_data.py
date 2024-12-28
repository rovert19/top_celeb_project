from airflow.providers.http.hooks.http import HttpAsyncHook
import asyncio

from include.constants import HEADERS, URL_MOVIE_DETAILS


def get_movie_info(result):
    if result["release_date"] == "":
        release_date = None
    else:
        release_date = result["release_date"]

    movie_info = {
        "id": result["id"],
        "title": result["title"],
        "genres": [genre_dict["name"] for genre_dict in result["genres"]],
        "origin": result["origin_country"][0],
        "release_date": release_date,
        "status": result["status"],
        "score": result["vote_average"]
    }
    return movie_info


async def request_movie_info(async_hook: HttpAsyncHook, movie_id):
    url_movie_details = URL_MOVIE_DETAILS.format(str(movie_id))
    await asyncio.sleep(1)
    response = await async_hook.run(url_movie_details, headers=HEADERS)
    movie_data = await response.json()
    movie_info = get_movie_info(movie_data)
    return movie_info


def batch_request_movies_info(movie_ids):
    async_hook = HttpAsyncHook(method="GET", http_conn_id="TMDB_API")
    loop = asyncio.get_event_loop()

    tasks = [request_movie_info(async_hook, id) for id in movie_ids]
    movies_info = loop.run_until_complete(asyncio.gather(*tasks))

    return movies_info