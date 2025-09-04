#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les détails de films via /movie/{movie_id}.
Utilise le module tmdb_base pour toute la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer tmdb_base
sys.path.insert(0, str(Path(__file__).parent))

from tmdb_base import TMDBFetcher, select_list_of_dicts


class MovieDetailsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les détails de films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",
            output_file="movie_details.ndjson",
            log_file="movie_details.log",
            entity_type="movie details",
            window_days=30,  # Fenêtre de refresh pour les films récents
            date_field="release_date",
            id_field="id",
            extra_params={}  # ou {"language": "en-US"} si nécessaire
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Endpoint pour récupérer les détails d'un film"""
        return f"/movie/{entity_id}"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux movie details.
        Ne garde QUE les champs suivants dans l'ordre défini :
        budget, genres, id, imdb_id, original_language, original_title,
        overview, popularity, production_companies, production_countries,
        release_date, revenue, runtime, spoken_languages, status, tagline,
        title, vote_average, vote_count
        """
        return {
            "budget": data.get("budget"),
            "genres": select_list_of_dicts(data.get("genres"), ["id", "name"]),
            "id": data.get("id"),
            "imdb_id": data.get("imdb_id"),
            "original_language": data.get("original_language"),
            "original_title": data.get("original_title"),
            "overview": data.get("overview"),
            "popularity": data.get("popularity"),
            "production_companies": select_list_of_dicts(
                data.get("production_companies"), 
                ["id", "name", "origin_country"]
            ),
            "production_countries": select_list_of_dicts(
                data.get("production_countries"), 
                ["iso_3166_1", "name"]
            ),
            "release_date": data.get("release_date"),
            "revenue": data.get("revenue"),
            "runtime": data.get("runtime"),
            "spoken_languages": select_list_of_dicts(
                data.get("spoken_languages"), 
                ["english_name", "iso_639_1", "name"]
            ),
            "status": data.get("status"),
            "tagline": data.get("tagline"),
            "title": data.get("title"),
            "vote_average": data.get("vote_average"),
            "vote_count": data.get("vote_count"),
        }


def main():
    """Point d'entrée principal"""
    fetcher = MovieDetailsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()