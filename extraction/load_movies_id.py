import gzip
import os
import requests
import tempfile
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

def download_and_upload_movie_ids(url: str, bucket_name: str, blob_name: str):
    """Downloads the TMDB .gz export, extracts it to a local temp file, and uploads to GCS."""
    print(f"Downloading stream from: {url}")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Usando um arquivo temporário físico para não explodir a memória (RAM) do Docker
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_out:
        temp_filename = temp_out.name
        
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            
            # Descompacta o GZIP "on-the-fly" e escreve no disco sem lotar a memória
            with gzip.GzipFile(fileobj=response.raw) as gz_file:
                for line in gz_file:
                    temp_out.write(line)

    print(f"Arquivo salvo temporariamente em {temp_filename}. Fazendo upload pro GCS...")
    blob.upload_from_filename(temp_filename, content_type="application/json", timeout=600)
    print(f"Sucesso: Uploaded to gs://{bucket_name}/{blob_name}")
    
    # Limpa o arquivo temporário
    os.remove(temp_filename)

def main():
    download_and_upload_movie_ids(TMDB_EXPORT_URL, GCS_BUCKET_NAME, GCS_BLOB_NAME)

if __name__ == "__main__":
    main()