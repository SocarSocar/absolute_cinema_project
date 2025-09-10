#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les traductions d’une série TV via /tv/{series_id}/translations.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class TVSeriesTranslationsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les traductions des séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",                   # fichier source avec les IDs de séries
            output_file="tv_series_translations.ndjson",  # fichier NDJSON de sortie
            log_file="tv_series_translations.log",        # fichier de log
            entity_type="tv series translations",         # type d’entité
            window_days=30,                               # rafraîchir si besoin
            date_field="first_air_date",                  # champ date de référence
            id_field="id",                                # champ identifiant série
            extra_params={}                               # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les traductions d’une série TV"""
        return f"/tv/{entity_id}/translations"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux traductions.
        La réponse TMDB contient :
        {
          "id": 1399,
          "translations": [
            {
              "iso_639_1": "fr",
              "iso_3166_1": "FR",
              "name": "Le Trône de Fer",
              "overview": "Résumé en français...",
              "tagline": "Une tagline française"
            }
          ]
        }
        """
        return {
            "id": data.get("id"),  # ID de la série
            "translations": select_list_of_dicts(
                data.get("translations"),
                ["iso_639_1", "iso_3166_1", "name", "overview", "tagline"]
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesTranslationsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

