#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les mots-clés associés à un film via /movie/{movie_id}/keywords.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class MovieKeywordsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les mots-clés des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",         # fichier source avec les IDs de films
            output_file="movie_keywords.ndjson",   # fichier NDJSON de sortie
            log_file="movie_keywords.log",         # fichier de log
            entity_type="movie keywords",          # type d’entité
            window_days=30,                        # rafraîchir si besoin (30 jours)
            date_field="release_date",             # champ date de référence
            id_field="id",                         # champ identifiant film
            extra_params={}                        # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les mots-clés d’un film"""
        return f"/movie/{entity_id}/keywords"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux mots-clés.
        La réponse TMDB contient un champ "keywords" qui est une liste de {id, name}.
        """
        return {
            "id": data.get("id"),  # ID du film
            "keywords": select_list_of_dicts(
                data.get("keywords"),
                ["id", "name"]  # chaque mot-clé = id + nom
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieKeywordsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

