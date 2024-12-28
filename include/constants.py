from airflow.hooks.base import BaseHook

URL_SEARCH_CELEB = "search/person?query={}&include_adult=false&language=en-US&page=1"
URL_MOVIE_CELEB = "person/{}/movie_credits?language=en-US"
URL_SERIES_CELEB = "person/{}/tv_credits?language=en-US"

URL_MOVIE_DETAILS = "movie/{movie_id}"
URL_SERIES_DETAILS = "tv/{series_id}"

HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {BaseHook.get_connection("TOKEN_TMDB")}"
}