#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
from pathlib import Path

# Ce fichier est dans: projet_absolute_cinema/scripts/fetch_TMDB_API
BASE_DIR    = Path(__file__).resolve().parent                 # .../scripts/fetch_TMDB_API
SCRIPTS_DIR = BASE_DIR                                        # .../scripts/fetch_TMDB_API
DUMPS_DIR   = BASE_DIR.parent / "dumps_daily"                 # .../scripts/dumps_daily

EXECUTION_ORDER = [
    "Configuration_languages.py",
    "Genre_series.py",
    "Genre_movies.py",
    "Movies_Details.py",
    "Movies_AlternativeTitles.py",
    "Movies_Certifications.py",
    "Movies_Credits.py",
    "Movies_ExternalIDs.py",
    "Movies_Keywords.py",
    "Movies_ReleaseDates.py",
    "Movies_Reviews.py",
    "Movies_Translations.py",
    "TVSeason_details.py",
    "TVEpisode_details.py",
    "TVSeries_Content.py",
    "TVSeries_AlternativeTitles.py",
    "TVSeries_Credits.py",
    "TVSeries_details.py",
    "TVSeries_ExternalID.py",
    "TVSeries_Keywords.py",
    "TVSeries_Reviews.py",
    "TVSeries_Translations.py",
    "Company_details.py",
    "Configuration_countries.py",
    "Network_details.py",
    "People_details.py",
    "Certifications_series.py",
]

def assert_paths():
    if not SCRIPTS_DIR.is_dir():
        print(f"ECHEC: dossier manquant {SCRIPTS_DIR}")
        sys.exit(2)
    if not DUMPS_DIR.is_dir():
        print(f"ECHEC: dossier manquant {DUMPS_DIR}")
        sys.exit(2)
    sh = DUMPS_DIR / "fetch_dumps_daily.sh"
    if not sh.is_file():
        print(f"ECHEC: script manquant {sh}")
        sys.exit(2)

def run_fetch_dumps():
    sh = DUMPS_DIR / "fetch_dumps_daily.sh"
    print("\nLancement: fetch_dumps_daily.sh")
    subprocess.run(["bash", str(sh)], check=True)

def run_script(script_name: str):
    script_path = SCRIPTS_DIR / script_name
    print(f"\nLancement: {script_name}")
    subprocess.run([sys.executable, str(script_path)], check=True)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline TMDB — lancement complet ou reprise à partir d’un script précis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-from", "-s",
        metavar="SCRIPT",
        help="Nom exact (ou insensible à la casse) d’un script dans l’ordre d’exécution pour reprendre à partir de celui-ci.",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="Affiche l’ordre d’exécution des scripts et quitte.",
    )
    parser.add_argument(
        "--skip-dumps",
        action="store_true",
        help="Ne pas lancer fetch_dumps_daily.sh (reprise pure sur les scripts Python).",
    )
    return parser.parse_args()

def resolve_start_index(start_from: str) -> int:
    if not start_from:
        return 0
    # Résolution insensible à la casse
    lower_map = {name.lower(): name for name in EXECUTION_ORDER}
    key = start_from.lower()
    if key in lower_map:
        canonical = lower_map[key]
        idx = EXECUTION_ORDER.index(canonical)
        return idx
    # Tente aussi une correspondance par sous-chaîne unique (ex: "tvepisode" -> "TVEpisode_details.py")
    candidates = [i for i, name in enumerate(EXECUTION_ORDER) if key in name.lower()]
    if len(candidates) == 1:
        return candidates[0]
    print("ECHEC: script introuvable ou ambigu pour --start-from:", start_from)
    print("Scripts disponibles (ordre):")
    for n in EXECUTION_ORDER:
        print(" -", n)
    sys.exit(2)

def main():
    assert_paths()
    args = parse_args()

    if args.list:
        print("Ordre d’exécution:")
        for i, name in enumerate(EXECUTION_ORDER, 1):
            print(f"{i:02d}. {name}")
        return

    start_idx = resolve_start_index(args.start_from)

    try:
        if not args.skip_dumps and start_idx == 0:
            run_fetch_dumps()
        elif not args.skip_dumps and start_idx > 0:
            print("\nInfo: reprise à partir d’un script Python — fetch_dumps_daily.sh non relancé.")
        for script in EXECUTION_ORDER[start_idx:]:
            run_script(script)
        print("\nOK: pipeline terminée")
    except subprocess.CalledProcessError as e:
        print(f"\nECHEC: {e}. Code retour: {e.returncode}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\nInterrompu par l’utilisateur.")
        sys.exit(130)

if __name__ == "__main__":
    main()
