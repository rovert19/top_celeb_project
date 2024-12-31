import asyncio
import aiohttp
from itertools import batched

from include.constants import HEADERS, URL_MOVIE_DETAILS

def get_movie_info(result):
    if len(result["origin_country"]) == 0:
        origin = None
    else:
        origin = result["origin_country"][0]

    if result["release_date"] == "":
        release_date = None
    else:
        release_date = result["release_date"]

    movie_info = {
        "id": result["id"],
        "title": result["title"],
        "genres": [genre_dict["name"] for genre_dict in result["genres"]],
        "origin": origin,
        "release_date": release_date,
        "status": result["status"],
        "score": result["vote_average"]
    }
    return movie_info


async def request_movie_info(movie_id):
    url_movie_details = URL_MOVIE_DETAILS.format(movie_id)
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url_movie_details, headers=HEADERS) as response:
            await asyncio.sleep(1)
            movie_data = await response.json()
            print(movie_data)
            movie_info = get_movie_info(movie_data)
            return movie_info


def batch_request_movies_info(ti):
    movies_ids = ti.xcom_pull(key='movies_ids', task_ids='request_movies_celebs')
    loop = asyncio.get_event_loop()

    tasks = [request_movie_info(id) for id in movies_ids]
    movies_info = []

    for batch in batched(tasks, 100):
        movies_info_batch = loop.run_until_complete(asyncio.gather(*batch))
        movies_info.extend(movies_info_batch)

    print(len(movies_info))
    ti.xcom_push(key="movies_info", value= movies_info)