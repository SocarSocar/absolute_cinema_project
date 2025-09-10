#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import snowflake.connector

# ---- Connexion Snowflake ----
conn = snowflake.connector.connect(
    user="NICOLAS.BOUTTIER",
    password="Nicolas070899@",
    account="nusqxoe-jk70019",
    warehouse="COMPUTE_WH",
    database="TMDB_TEST_ETL",
    schema="BUCKET",
)
cs = conn.cursor()

# ---- Répertoires ----
BASE_DIR = Path(__file__).resolve().parent                    # dossier de ce script
PROJECT_DIR = BASE_DIR.parent.parent                          # .../projet_absolute_cinema
OUT_DIR = PROJECT_DIR / "data" / "out"                        # .../projet_absolute_cinema/data/out

# ---- Fichiers requis (1 fichier = 1 stage @ST_<UPPER_BASE>) ----
REQUIRED_FILES = [
    "certification_movies.ndjson",
    "certification_series.ndjson",
    "company_details.ndjson",
    "movie_alternative_titles.ndjson",
    "movie_credits.ndjson",
    "movie_details.ndjson",
    "movie_external_ids.ndjson",
    "movie_keywords.ndjson",
    "movie_release_dates.ndjson",
    "movie_reviews.ndjson",
    "movie_translations.ndjson",
    "people_details.ndjson",
    "ref_countries.ndjson",
    "ref_genre_movies.ndjson",
    "ref_genre_series.ndjson",
    "ref_languages.ndjson",
    "tv_episodes_details.ndjson",
    "tv_networks_details.ndjson",
    "tv_seasons_details.ndjson",
    "tv_series_alternative_titles.ndjson",
    "tv_series_content_ratings.ndjson",
    "tv_series_credits.ndjson",
    "tv_series_details.ndjson",
    "tv_series_external_ids.ndjson",
    "tv_series_keywords.ndjson",
    "tv_series_reviews.ndjson",
    "tv_series_translations.ndjson",
    "watch_providers_movies.ndjson",
    "watch_providers_series.ndjson",
]

ALLOWED_EXTS = {".ndjson", ".json"}  # filtre strict (ignore .tmp, .txt, etc.)

try:
    # Sécurité répertoire
    if not OUT_DIR.is_dir():
        raise FileNotFoundError(f"Dossier introuvable: {OUT_DIR}")

    # Contexte BDD/Schéma (déjà fixé à la connexion, ré-assertion défensive)
    cs.execute("USE DATABASE TMDB_TEST_ETL")
    cs.execute("USE SCHEMA BUCKET")

    missing = []
    uploaded = []

    for fname in REQUIRED_FILES:
        ext = Path(fname).suffix.lower()
        if ext not in ALLOWED_EXTS:
            # Ne traite pas les extensions non autorisées
            continue

        file_path = OUT_DIR / fname
        stage = f"@ST_{Path(fname).stem.upper()}"  # ex: movie_details -> @ST_MOVIE_DETAILS

        if not file_path.is_file():
            missing.append(fname)
            print(f"[MISSING] {fname}")
            continue

        # Nettoie le stage pour ne conserver qu'une seule version "latest"
        try:
            cs.execute(f"REMOVE {stage}")
        except Exception:
            # Si le stage est vide ou absent, ignore silencieusement
            pass

        # PUT vers le stage (garde guillemets autour des chemins)
        put_sql = f"PUT 'file://{file_path.as_posix()}' {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cs.execute(put_sql)

        uploaded.append(fname)
        print(f"[UPLOADED] {fname} -> {stage}")

    # Résumé
    print("\n--- Résumé upload stages ---")
    print(f"OK : {len(uploaded)}")
    if uploaded:
        print("  " + "\n  ".join(uploaded))
    print(f"MANQUANTS : {len(missing)}")
    if missing:
        print("  " + "\n  ".join(missing))

finally:
    cs.close()
    conn.close()
