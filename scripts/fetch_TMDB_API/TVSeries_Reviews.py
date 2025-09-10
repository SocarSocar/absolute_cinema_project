#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les reviews d’une série TV via /tv/{series_id}/reviews.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class TVSeriesReviewsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les reviews des séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",            # fichier source avec les IDs de séries
            output_file="tv_series_reviews.ndjson",# fichier NDJSON de sortie
            log_file="tv_series_reviews.log",      # fichier de log
            entity_type="tv series reviews",       # type d’entité
            window_days=30,                        # rafraîchir si besoin
            date_field="first_air_date",           # champ date de référence
            id_field="id",                         # champ identifiant série
            extra_params={}                        # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les reviews d’une série TV"""
        return f"/tv/{entity_id}/reviews"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux reviews.
        La réponse TMDB contient :
        {
          "id": 1399,
          "results": [
            {
              "id": "5a1b2c3d4e",
              "author": "John Doe",
              "content": "Super série !",
              "created_at": "2023-09-01T12:34:56.000Z",
              "url": "https://www.themoviedb.org/review/5a1b2c3d4e"
            }
          ]
        }
        """
        return {
            "id": data.get("id"),  # ID de la série
            "reviews": select_list_of_dicts(
                data.get("results"),
                ["id", "author", "content", "created_at", "url"]
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesReviewsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

