import json
import os
import time
from datetime import date

import requests
from dotenv import load_dotenv
from google.cloud import storage
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

load_dotenv()

TMDB_TOKEN = os.getenv("TMDB_ACCESS_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BASE_URL = "https://api.themoviedb.org/3"
TODAY = date.today().isoformat()

# Prefix where IDs are saved
MOVIE_IDS_PREFIX = "raw/movie_ids/"

POPULARITY_THRESHOLD = 10.0
SLEEP_BETWEEN_REQUESTS = 0.05  # 20 movies/sec, safe within the 40 req/s limit

# GCS

def get_latest_movie_ids_blob_name(bucket_name: str, prefix: str) -> str:
    """Lists blobs in the prefix and returns the name of the most recent one."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise ValueError(f"No files found at gs://{bucket_name}/{prefix}")
    
    # Since the name format is YYYY_MM_DD, sorting alphabetically brings the most recent one to the end
    blobs.sort(key=lambda b: b.name)
    latest_blob = blobs[-1]
    print(f"Latest movie IDs file found: {latest_blob.name}")
    return latest_blob.name


def read_filtered_ids_from_gcs(bucket_name: str, blob_name: str, min_popularity: float) -> list[dict]:
    """Streams the NDJSON file from GCS and returns only filtered records to save RAM."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    filtered_ids = []
    # blob.open("r") allows reading the remote file line by line (streaming) without blowing up memory
    with blob.open("r") as f:
        for line in f:
            if line.strip():
                item = json.loads(line)
                if item.get("popularity", 0) >= min_popularity:
                    filtered_ids.append(item)
    return filtered_ids

def upload_to_gcs(data: dict, bucket_name: str, blob_name: str) -> None:
    """Uploads a single JSON record to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type="application/json")

# TMDB API

@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
)
def fetch_movie_details(movie_id: int) -> dict:
    """Calls /movie/{id} and returns the full details JSON."""
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_TOKEN}"
    }
    response = requests.get(
        f"{BASE_URL}/movie/{movie_id}",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()

@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
)
def fetch_movie_credits(movie_id: int) -> dict:
    """Calls /movie/{id}/credits and returns the credits JSON."""
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_TOKEN}"
    }
    response = requests.get(
        f"{BASE_URL}/movie/{movie_id}/credits",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()

# Main

def main():
    print("Reading movie IDs from GCS...")
    latest_blob_name = get_latest_movie_ids_blob_name(GCS_BUCKET_NAME, MOVIE_IDS_PREFIX)
    
    filtered_ids = read_filtered_ids_from_gcs(GCS_BUCKET_NAME, latest_blob_name, POPULARITY_THRESHOLD)
    print(f"After filter: {len(filtered_ids)} movies to process")
    for i, item in enumerate(filtered_ids):
        movie_id = item["id"]
        try:
            details = fetch_movie_details(movie_id)
            upload_to_gcs(
                details,
                GCS_BUCKET_NAME,
                f"raw/movies/{TODAY}/{movie_id}.json",
            )
            credits = fetch_movie_credits(movie_id)
            upload_to_gcs(
                credits,
                GCS_BUCKET_NAME,
                f"raw/credits/{TODAY}/{movie_id}.json",
            )
        except Exception as e:
            print(f"  Skipping movie {movie_id}: {e}")
            continue
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        if (i + 1) % 500 == 0:
            print(f"  Progress: {i + 1}/{len(filtered_ids)}")
    print("Done.")


if __name__ == "__main__":
    main()