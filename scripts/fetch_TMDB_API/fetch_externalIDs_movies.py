#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère l'identifiant IMDB d’un film via /movie/{movie_id}/external_ids.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher


class MovieExternalIDsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les identifiants externes des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",                 # fichier source avec les IDs de films
            output_file="movie_external_ids.ndjson",       # fichier NDJSON de sortie
            log_file="movie_external_ids.log",             # fichier de log
            entity_type="movie external ids",              # type d’entité
            window_days=30,                                # rafraîchir si besoin (30 jours)
            date_field="release_date",                     # champ date de référence
            id_field="id",                                 # champ identifiant film
            extra_params={}                                # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les identifiants externes d’un film"""
        return f"/movie/{entity_id}/external_ids"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux identifiants externes.
        La réponse TMDB contient :
        {
          "id": 550,
          "imdb_id": "tt0137523",
          "facebook_id": "...",
          ...
        }
        On garde uniquement imdb_id
        """
        return {
            "id": data.get("id"),        # ID du film
            "imdb_id": data.get("imdb_id")
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieExternalIDsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

