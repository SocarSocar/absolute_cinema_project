#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les critiques (reviews) d’un film via /movie/{movie_id}/reviews.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher


class MovieReviewsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les critiques des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",                 # fichier source avec les IDs de films
            output_file="movie_reviews.ndjson",            # fichier NDJSON de sortie
            log_file="movie_reviews.log",                  # fichier de log
            entity_type="movie reviews",                   # type d’entité
            window_days=30,                                # rafraîchir si besoin (30 jours)
            date_field="release_date",                     # champ date de référence
            id_field="id",                                 # champ identifiant film
            extra_params={}                                # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les critiques d’un film"""
        return f"/movie/{entity_id}/reviews"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux critiques.
        La réponse TMDB contient :
        {
          "id": 550,
          "page": 1,
          "results": [
            {
              "id": "5a1e1c8bc3a3680f6f0035c9",
              "author": "John Doe",
              "content": "Super film !",
              "created_at": "2017-11-29T16:34:35.000Z",
              "url": "https://www.themoviedb.org/review/5a1e1c8bc3a3680f6f0035c9"
            }
          ]
        }
        On garde uniquement id (review_id), author, content, created_at, url
        """
        reviews = []
        for entry in data.get("results", []):
            reviews.append({
                "review_id": entry.get("id"),
                "author": entry.get("author"),
                "content": entry.get("content"),
                "created_at": entry.get("created_at"),
                "url": entry.get("url")
            })
        
        return {
            "id": data.get("id"),    # ID du film
            "reviews": reviews
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieReviewsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
