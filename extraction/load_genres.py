import os
import json
import requests
from dotenv import load_dotenv
from google.cloud import storage
from tenacity import retry, wait_exponential, stop_after_attempt

# Load environment variables
load_dotenv()

TMDB_ACCESS_TOKEN = os.getenv("TMDB_ACCESS_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

GENRES_URL = "https://api.themoviedb.org/3/genre/movie/list"

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def fetch_genres() -> dict:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}"
    }
    params = {
        "language": "en" # keeping in English like the others
    }
    print("Fetching genres from TMDB API...")
    response = requests.get(GENRES_URL, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def upload_to_gcs(data: dict, bucket_name: str, blob_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Extract the genres list and save as NDJSON (one per line) for seamless BigQuery ingestion
    genres_list = data.get("genres", [])
    ndjson_content = "\n".join(json.dumps(record) for record in genres_list)
    blob.upload_from_string(ndjson_content, content_type="application/json", timeout=120)
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")

def main():
    genres_data = fetch_genres()
    print(f"Fetched {len(genres_data.get('genres', []))} genres.")
    
    blob_name = "raw/genres/genres.json"
    upload_to_gcs(genres_data, GCS_BUCKET_NAME, blob_name)

if __name__ == "__main__":
    main()
