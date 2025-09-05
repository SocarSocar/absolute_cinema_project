#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les crédits d’un film via /movie/{movie_id}/credits.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class MovieCreditsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les crédits de films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",        # fichier source avec les IDs de films
            output_file="movie_credits.ndjson",   # fichier NDJSON de sortie
            log_file="movie_credits.log",         # fichier de log
            entity_type="movie credits",          # type d’entité
            window_days=30,                       # rafraîchir sur 30 jours si besoin
            date_field="release_date",            # champ date pour filtrer
            id_field="id",                        # champ identifiant film
            extra_params={}                       # paramètres supplémentaires (ex: {"language": "en-US"})
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les crédits d’un film"""
        return f"/movie/{entity_id}/credits"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux crédits de films.
        On récupère les deux sous-parties de la réponse TMDB :
        - cast (liste d’acteurs avec leurs rôles)
        - crew (liste des membres de l’équipe technique)
        """
        return {
            "id": data.get("id"),  # ID du film
            "cast": select_list_of_dicts(
                data.get("cast"),
                ["credit_id", "id", "character", "order"]  # acteurs + rôle + ordre
            ),
            "crew": select_list_of_dicts(
                data.get("crew"),
                ["credit_id", "id", "department", "job"]   # équipe technique
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieCreditsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
