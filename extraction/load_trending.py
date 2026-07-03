import json
import requests
import os
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime, date

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_ACCESS_TOKEN = os.getenv("TMDB_ACCESS_TOKEN")

time_window = "day" # O TMDB aceita "day" ou "week" aqui, não a data atual

TRENDING_MOVIES_URL = f"https://api.themoviedb.org/3/trending/movie/{time_window}"
POPULAR_MOVIE_URL = "https://api.themoviedb.org/3/movie/popular"
PLAYING_MOVIES = "https://api.themoviedb.org/3/movie/now_playing"


def load_urls(endpoint, folder_name):
    page = 1 #define 1 pois vai começar na primeira página
    results = [] # lista vazia para acumular todos os filmes em uma só lista

    while True:
        headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}"
        }
        # Adiciona o parâmetro de página na requisição!
        response = requests.get(endpoint, params={"page": page}, headers=headers, timeout = 30)
        data = response.json()
        results.extend(data["results"]) #retorna o campos results do response em cada loop
        if page >= 50 or page > data["total_pages"]: #capa em 5 paginas ou se ultrapassar o toal de páginas (tem um campo total_pages no response)
            break
        page += 1

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    # Monta o nome do arquivo (ex: raw/trending/2026-07-02.json)
    today_str = date.today().isoformat() # isoformat retorna no formato YYYY-MM-DD
    blob_name = f"raw/{folder_name}/{today_str}.json"
    blob = bucket.blob(blob_name)
    
    # Converte a lista para NDJSON (uma linha por filme)
    ndjson_content = "\n".join(json.dumps(record) for record in results)
    blob.upload_from_string(ndjson_content, content_type="application/json")
    print(f"Salvo no GCS: gs://{GCS_BUCKET_NAME}/{blob_name} com {len(results)} filmes.")

def main():
    load_urls(TRENDING_MOVIES_URL, "trending")
    load_urls(POPULAR_MOVIE_URL, "popular")
    load_urls(PLAYING_MOVIES, "now_playing")

if __name__ == "__main__":
    main()



