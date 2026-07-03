import gzip
import io
import json
import os
import requests

from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
# pyrefly: ignore [missing-import]
from google.cloud import storage

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_DESTINATION_FOLDER = "raw/movie_ids"

yesterday = datetime.now() - timedelta(days=1)
month = yesterday.strftime("%m")
day = yesterday.strftime("%d")
year = yesterday.strftime("%Y")

TMDB_EXPORT_URL = (
    f"https://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz"
)
GCS_BLOB_NAME = f"{GCS_DESTINATION_FOLDER}/movie_ids_{year}_{month}_{day}.json"

def download_and_parse_movie_ids(url: str) -> list[dict]:
    """Downloads the TMDB .gz export and returns a list of movie ID dicts."""
    print(f"Downloading: {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()  # raises HTTPError for 4xx/5xx
    movie_ids = []
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
        for raw_line in gz_file:
            line = raw_line.decode("utf-8").strip()
            if line:
                movie_ids.append(json.loads(line))
    print(f"Parsed {len(movie_ids):,} movie IDs.")
    return movie_ids

# Upload to GCS
def upload_to_gcs(data: list[dict], bucket_name: str, blob_name: str) -> None:
    """Serializes the movie ID list as newline-delimited JSON and uploads to GCS."""
    client = storage.Client()  # authenticates via GOOGLE_APPLICATION_CREDENTIALS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    ndjson_content = "\n".join(json.dumps(record) for record in data)
    blob.upload_from_string(ndjson_content, content_type="application/json", timeout=600)
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")


# Main that will run both functions
def main():
    movie_ids = download_and_parse_movie_ids(TMDB_EXPORT_URL)
    upload_to_gcs(movie_ids, GCS_BUCKET_NAME, GCS_BLOB_NAME)

if __name__ == "__main__":
    main()