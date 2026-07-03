import requests
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
import os

# Test script with fight club movie to test connection with TMDB APIs 

load_dotenv()

movie_id = 550
TMDB_TOKEN = os.getenv("TMDB_ACCESS_TOKEN")
URL = f"https://api.themoviedb.org/3/movie/{movie_id}"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}

response = requests.get(URL, headers=headers)
print(response.json())
