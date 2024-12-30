import asyncio
import aiohttp
from itertools import batched

from include.constants import HEADERS, URL_SERIES_DETAILS

def get_series_info(result):
    if len(result["origin_country"]) == 0:
        origin = None
    else:
        origin = result["origin_country"][0]

    if result["first_air_date"] == "":
        release_date = None
    else:
        release_date = result["first_air_date"]

    series_info = {
        "id": result["id"],
        "title": result["name"],
        "genres": [genre_dict["name"] for genre_dict in result["genres"]],
        "origin": origin,
        "release_date": release_date,
        "seasons": int(result["number_of_seasons"]),
        "status": result["status"],
        "score": result["vote_average"]
    }
    return series_info


async def request_series_info(series_id):
    url_series_details = URL_SERIES_DETAILS.format(series_id)

    async with aiohttp.ClientSession() as session:
        async with session.get(url_series_details, headers=HEADERS) as response:
            await asyncio.sleep(1)
            series_data = await response.json()
            print(series_data)
            series_info = get_series_info(series_data)

            return series_info


def batch_request_series_info(ti):
    series_ids = ti.xcom_pull(key='series_ids', task_ids='request_series_celebs')
    loop = asyncio.get_event_loop()
    
    tasks = [request_series_info(id) for id in series_ids]
    all_series_info = []

    for batch in batched(tasks, 100):
        series_info_batch = loop.run_until_complete(asyncio.gather(*batch))
        all_series_info.extend(series_info_batch)

    print(len(all_series_info))
    ti.xcom_push(key="all_series_info", value= all_series_info)
    