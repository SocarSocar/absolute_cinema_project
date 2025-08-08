import requests
from dotenv import load_dotenv
from pprint import pprint
import os

# Charge les variables d'environnement du fichier .env
load_dotenv()

# Récupère la clé API Bearer
API_KEY = os.getenv("TMDB_BEARER")
movie_id = 559969  # Test avec El Camino

# Endpoint de l'API
url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"

# En-têtes de la requête
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# Requête
response = requests.get(url, headers=headers)

# Résultat brut
print(response.status_code)
pprint(response.json())