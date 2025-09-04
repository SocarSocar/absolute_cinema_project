#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les dates de sortie d’un film via /movie/{movie_id}/release_dates.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher


class MovieReleaseDatesFetcher(TMDBFetcher):
    """Fetcher spécifique pour les dates de sortie des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",                   # fichier source avec les IDs de films
            output_file="movie_release_dates.ndjson",        # fichier NDJSON de sortie
            log_file="movie_release_dates.log",              # fichier de log
            entity_type="movie release dates",               # type d’entité
            window_days=30,                                  # rafraîchir si besoin (30 jours)
            date_field="release_date",                       # champ date de référence
            id_field="id",                                   # champ identifiant film
            extra_params={}                                  # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les dates de sortie d’un film"""
        return f"/movie/{entity_id}/release_dates"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux dates de sortie.
        La réponse TMDB contient :
        {
          "id": 550,
          "results": [
            {
              "iso_3166_1": "US",
              "release_dates": [
                {"certification": "R", "release_date": "1999-10-15T00:00:00.000Z", "type": 3}
              ]
            }
          ]
        }
        On aplatit en une liste [{iso_3166_1, release_date, type, certification}]
        """
        flattened = []
        for entry in data.get("results", []):
            country = entry.get("iso_3166_1")
            for release in entry.get("release_dates", []):
                flattened.append({
                    "iso_3166_1": country,
                    "release_date": release.get("release_date"),
                    "type": release.get("type"),  # release_type
                    "certification": release.get("certification")
                })
        
        return {
            "id": data.get("id"),   # ID du film
            "release_dates": flattened
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieReleaseDatesFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()


