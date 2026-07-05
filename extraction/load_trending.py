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

time_window = "day" # TMDB accepts "day" or "week" here, not the current date

TRENDING_MOVIES_URL = f"https://api.themoviedb.org/3/trending/movie/{time_window}"
POPULAR_MOVIE_URL = "https://api.themoviedb.org/3/movie/popular"
PLAYING_MOVIES = "https://api.themoviedb.org/3/movie/now_playing"


def load_urls(endpoint, folder_name):
    page = 1 # Set to 1 to start at the first page
    results = [] # Empty list to accumulate all movies

    while True:
        headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}"
        }
        # Add the page parameter to the request
        response = requests.get(endpoint, params={"page": page}, headers=headers, timeout = 30)
        data = response.json()
        results.extend(data["results"]) # Returns the 'results' field from the response in each loop
        if page >= 50 or page > data["total_pages"]: # Cap at 50 pages or if it exceeds the total_pages field from the response
            break
        page += 1

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    # Build the filename (e.g. raw/trending/2026-07-02.json)
    today_str = date.today().isoformat() # isoformat returns the date in YYYY-MM-DD format
    blob_name = f"raw/{folder_name}/{today_str}.json"
    blob = bucket.blob(blob_name)
    
    # Convert the list to NDJSON (one line per movie)
    ndjson_content = "\n".join(json.dumps(record) for record in results)
    blob.upload_from_string(ndjson_content, content_type="application/json")
    print(f"Saved to GCS: gs://{GCS_BUCKET_NAME}/{blob_name} with {len(results)} movies.")

def main():
    load_urls(TRENDING_MOVIES_URL, "trending")
    load_urls(POPULAR_MOVIE_URL, "popular")
    load_urls(PLAYING_MOVIES, "now_playing")

if __name__ == "__main__":
    main()



