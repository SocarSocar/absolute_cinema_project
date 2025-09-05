#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script maître pour exécuter tous les scripts de fetch_TMDB_API
dans un ordre défini en respectant les dépendances.
"""

import subprocess
import os

# Dossier où se trouvent tes scripts
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__)) + "/fetch_TMDB_API/"

# Liste ordonnée des scripts à exécuter
# ⚠️ Tu peux ajouter/enlever des fichiers si besoin
EXECUTION_ORDER = [
    # Les dépendances "languages" doivent passer avant genres
    "Configuration_languages.py",

    # Genres (ont besoin de ref_languages)
    "Genre_series.py",
    "Genre_movies.py",

    # Movies
    "Movies_Details.py",
    "Movies_AlternativeTitles.py",
    "Movies_Credits.py",
    "Movies_ExternalIDs.py",
    "Movies_Keywords.py",
    "Movies_ReleaseDates.py",
    "Movies_Reviews.py",
    "Movies_Translations.py",
    "Movies_WatchProviders.py",

    # Séries TV (ordre important)
    "TVSeason_details.py",    # doit être lancé avant TVEpisode
    "TVEpisode_details.py",
    "TVSeries_Content.py",
    "TVSeries_AlternativeTitles.py",
    "TVSeries_Credits.py",
    "TVSeries_ExternalID.py",
    "TVSeries_Keywords.py",
    "TVSeries_Reviews.py",
    "TVSeries_Translations.py",

    # Autres entités
    "Company_details.py",
    "Configuration_countries.py",
    "Network_details.py",
    "People_details.py",   # si tu veux les gens
    "TV_watch_providers.py",
    "Certifications_series.py",
]

def run_script(script_name):
    """Exécute un script Python avec subprocess"""
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"\n🚀 Lancement de {script_name} ...")
    try:
        subprocess.run(["python", script_path], check=True)
        print(f"✅ Terminé : {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur dans {script_name} : {e}")
        # Si tu veux arrêter à la première erreur, tu mets un exit ici
        # exit(1)

def main():
    for script in EXECUTION_ORDER:
        run_script(script)

if __name__ == "__main__":
    main()