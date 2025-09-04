#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les titres alternatifs d’un film via /movie/{movie_id}/alternative_titles.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher, select_list_of_dicts


class MovieAlternativeTitlesFetcher(TMDBFetcher):
    """Fetcher spécifique pour les titres alternatifs des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",                   # fichier source avec les IDs de films
            output_file="movie_alternative_titles.ndjson",   # fichier NDJSON de sortie
            log_file="movie_alternative_titles.log",         # fichier de log
            entity_type="movie alternative titles",          # type d’entité
            window_days=30,                                  # rafraîchir si besoin (30 jours)
            date_field="release_date",                       # champ date de référence
            id_field="id",                                   # champ identifiant film
            extra_params={}                                  # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les titres alternatifs d’un film"""
        return f"/movie/{entity_id}/alternative_titles"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux titres alternatifs.
        La réponse TMDB contient un champ "titles" qui est une liste de {iso_3166_1, title}.
        """
        return {
            "id": data.get("id"),  # ID du film
            "titles": select_list_of_dicts(
                data.get("titles"),
                ["iso_3166_1", "title"]  # on garde seulement le code pays et le titre alternatif
            )
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieAlternativeTitlesFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()

