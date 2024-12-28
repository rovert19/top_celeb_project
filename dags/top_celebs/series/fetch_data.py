from airflow.providers.http.hooks.http import HttpAsyncHook
import asyncio

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


async def request_series_info(async_hook: HttpAsyncHook, series_id):
    url_series_details = URL_SERIES_DETAILS.format(str(series_id))
    await asyncio.sleep(1)
    response = await async_hook.run(url_series_details, headers=HEADERS)
    series_data = await response.json()
    series_info = get_series_info(series_data)
    return series_info


def batch_request_series_info(series_ids):
    async_hook = HttpAsyncHook(method="GET", http_conn_id="TMDB_API")
    loop = asyncio.get_event_loop()

    tasks = [request_series_info(async_hook, id) for id in series_ids]
    all_series_info = loop.run_until_complete(asyncio.gather(*tasks))

    return all_series_info