#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les classifications (content ratings) d’une série TV via /tv/{series_id}/content_ratings.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class TVSeriesContentRatingsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les classifications des séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",                        # fichier source avec les IDs de séries
            output_file="tv_series_content_ratings.ndjson",    # fichier NDJSON de sortie
            log_file="tv_series_content_ratings.log",          # fichier de log
            entity_type="tv series content ratings",           # type d’entité
            window_days=30,                                    # rafraîchir si besoin
            date_field="first_air_date",                        # champ date de référence
            id_field="id",                                      # champ identifiant série
            extra_params={}                                     # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les classifications d’une série TV"""
        return f"/tv/{entity_id}/content_ratings"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux content ratings.
        La réponse TMDB contient :
        {
          "id": 1399,
          "results": [
            {"iso_3166_1": "US", "rating": "TV-MA"},
            {"iso_3166_1": "FR", "rating": "16"}
          ]
        }
        """
        return {
            "id": data.get("id"),  # ID de la série
            "content_ratings": select_list_of_dicts(
                data.get("results"),
                ["iso_3166_1", "rating"]
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesContentRatingsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
