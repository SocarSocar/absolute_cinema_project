#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les titres alternatifs d’une série TV via /tv/{series_id}/alternative_titles.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class TVSeriesAlternativeTitlesFetcher(TMDBFetcher):
    """Fetcher spécifique pour les titres alternatifs des séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",                        # fichier source avec les IDs de séries
            output_file="tv_series_alternative_titles.ndjson", # fichier NDJSON de sortie
            log_file="tv_series_alternative_titles.log",       # fichier de log
            entity_type="tv series alternative titles",       # type d’entité
            window_days=30,                                    # rafraîchir si besoin
            date_field="first_air_date",                        # champ date de référence
            id_field="id",                                      # champ identifiant série
            extra_params={}                                     # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les titres alternatifs d’une série TV"""
        return f"/tv/{entity_id}/alternative_titles"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux titres alternatifs.
        La réponse TMDB contient :
        {
          "id": 1399,
          "results": [
            {"iso_3166_1": "US", "title": "Game of Thrones"},
            {"iso_3166_1": "FR", "title": "Le Trône de Fer"}
          ]
        }
        """
        return {
            "id": data.get("id"),  # ID de la série
            "alternative_titles": select_list_of_dicts(
                data.get("results"),
                ["iso_3166_1", "title"]
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesAlternativeTitlesFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

