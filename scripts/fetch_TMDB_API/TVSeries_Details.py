#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les détails d’une série TV via /tv/{series_id}.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class TVSeriesDetailsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les détails de séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_dumps.json",                   # fichier source avec les IDs de séries
            output_file="tv_series_details.ndjson",       # fichier NDJSON de sortie
            log_file="tv_series_details.log",            # fichier de log
            entity_type="tv series details",             # type d’entité
            window_days=30,                               # rafraîchir si besoin (30 jours)
            date_field="first_air_date",                  # champ date de référence
            id_field="id",                                # champ identifiant série
            extra_params={}                               # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les détails d’une série TV"""
        return f"/tv/{entity_id}"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux séries TV.
        Champs principaux et listes aplaties.
        """
        return {
            "content_id": data.get("id"),
            "original_name": data.get("original_name"),
            "name": data.get("name"),
            "original_language": data.get("original_language"),
            "overview": data.get("overview"),
            "homepage": data.get("homepage"),
            "poster_path": data.get("poster_path"),
            "backdrop_path": data.get("backdrop_path"),
            "status": data.get("status"),
            "tagline": data.get("tagline"),
            "popularity": data.get("popularity"),
            "vote_average": data.get("vote_average"),
            "vote_count": data.get("vote_count"),
            "type": data.get("type"),
            "first_air_date": data.get("first_air_date"),
            "last_air_date": data.get("last_air_date"),
            "number_of_seasons": data.get("number_of_seasons"),
            "number_of_episodes": data.get("number_of_episodes"),
            "in_production": data.get("in_production"),
            "origin_country": data.get("origin_country"),  # liste de codes pays ISO_3166_1
            "genres": select_list_of_dicts(
                data.get("genres"),
                ["id", "name"]
            ),
            "production_companies": select_list_of_dicts(
                data.get("production_companies"),
                ["id", "name", "origin_country"]
            ),
            "production_countries": select_list_of_dicts(
                data.get("production_countries"),
                ["iso_3166_1"]
            ),
            "spoken_languages": select_list_of_dicts(
                data.get("spoken_languages"),
                ["iso_639_1"]
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesDetailsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

