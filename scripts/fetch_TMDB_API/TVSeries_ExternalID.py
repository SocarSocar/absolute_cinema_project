#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les IDs externes d’une série TV via /tv/{series_id}/external_ids.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher


class TVSeriesExternalIDsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les IDs externes des séries TV"""
    
    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",               # fichier source avec les IDs de séries
            output_file="tv_series_external_ids.ndjson", # fichier NDJSON de sortie
            log_file="tv_series_external_ids.log",    # fichier de log
            entity_type="tv series external ids",     # type d’entité
            window_days=30,                           # rafraîchir si besoin
            date_field="first_air_date",              # champ date de référence
            id_field="id",                            # champ identifiant série
            extra_params={}                           # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les IDs externes d’une série TV"""
        return f"/tv/{entity_id}/external_ids"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux IDs externes.
        TMDB renvoie un objet type :
        {
          "id": 1399,
          "imdb_id": "tt0944947",
          "tvdb_id": 121361,
          ...
        }
        """
        return {
            "id": data.get("id"),        # ID TMDB de la série
            "imdb_id": data.get("imdb_id")  # ID IMDB
        }


def main():
    """Point d’entrée principal"""
    fetcher = TVSeriesExternalIDsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
