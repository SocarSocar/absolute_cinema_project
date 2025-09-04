#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les traductions d’un film via /movie/{movie_id}/translations.
Utilise le module fetch_API_TMDB pour la logique commune.
"""

import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer fetch_API_TMDB
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import TMDBFetcher


class MovieTranslationsFetcher(TMDBFetcher):
    """Fetcher spécifique pour les traductions des films"""
    
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",                   # fichier source avec les IDs de films
            output_file="movie_translations.ndjson",         # fichier NDJSON de sortie
            log_file="movie_translations.log",               # fichier de log
            entity_type="movie translations",                # type d’entité
            window_days=30,                                  # rafraîchir si besoin (30 jours)
            date_field="release_date",                       # champ date de référence
            id_field="id",                                   # champ identifiant film
            extra_params={}                                  # paramètres supplémentaires
        )
    
    def get_endpoint(self, entity_id: int) -> str:
        """Construit l’endpoint pour récupérer les traductions d’un film"""
        return f"/movie/{entity_id}/translations"
    
    def project_fields(self, data: dict) -> dict:
        """
        Projection des champs spécifiques aux traductions.
        La réponse TMDB contient :
        {
          "id": 550,
          "translations": [
            {
              "iso_639_1": "fr",
              "iso_3166_1": "FR",
              "data": {
                "title": "Fight Club",
                "overview": "Un homme...",
                "tagline": "Mêlez-vous-en"
              }
            }
          ]
        }
        On garde uniquement iso_639_1, iso_3166_1, data.title, data.overview, data.tagline
        """
        translations = []
        for entry in data.get("translations", []):
            translations.append({
                "iso_639_1": entry.get("iso_639_1"),
                "iso_3166_1": entry.get("iso_3166_1"),
                "title": entry.get("data", {}).get("title"),
                "overview": entry.get("data", {}).get("overview"),
                "tagline": entry.get("data", {}).get("tagline"),
            })
        
        return {
            "id": data.get("id"),   # ID du film
            "translations": translations
        }


def main():
    """Point d’entrée principal"""
    fetcher = MovieTranslationsFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()


