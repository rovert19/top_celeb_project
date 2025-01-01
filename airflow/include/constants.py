from airflow.models import Variable
import os

URL_SEARCH_CELEB = "https://api.themoviedb.org/3/search/person?query={}&include_adult=false&language=en-US&page=1"
URL_MOVIE_CELEB = "https://api.themoviedb.org/3/person/{}/movie_credits?language=en-US"
URL_SERIES_CELEB = "https://api.themoviedb.org/3/person/{}/tv_credits?language=en-US"

URL_MOVIE_DETAILS = "https://api.themoviedb.org/3/movie/{}?language=en-US"
URL_SERIES_DETAILS = "https://api.themoviedb.org/3/tv/{}?language=en-US"

TOKEN_TMDB = os.getenv("TOKEN_TMDB")

HEADERS = {
    "accept": "application/json",
    "Authorization": f'Bearer {TOKEN_TMDB}'
}
