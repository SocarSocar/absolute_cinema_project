#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from typing import List
from contextlib import closing

# Optional but recommended: pip install python-dotenv
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import snowflake.connector

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent              # .../scripts/Load_Snowflake
PROJECT_DIR = BASE_DIR.parent.parent                    # .../projet_absolute_cinema
OUT_DIR = PROJECT_DIR / "data" / "out"                  # .../data/out
ENV_PATH = PROJECT_DIR / ".env"                         # secrets live here

# ---------- Env helpers ----------
def _get_env(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise RuntimeError(f"Missing required env var: {var}")
    return val

if load_dotenv is not None and ENV_PATH.exists():
    load_dotenv(ENV_PATH)

SNOWFLAKE_USER      = _get_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = _get_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT   = _get_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = _get_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = _get_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = _get_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")  # optional

# ---------- Data files ----------
REQUIRED_FILES: List[str] = [
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
ALLOWED_EXTS = {".ndjson", ".json"}

def main() -> None:
    if not OUT_DIR.is_dir():
        raise FileNotFoundError(f"Dossier introuvable: {OUT_DIR}")

    conn_kwargs = dict(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    if SNOWFLAKE_ROLE:
        conn_kwargs["role"] = SNOWFLAKE_ROLE

    with closing(snowflake.connector.connect(**conn_kwargs)) as conn, closing(conn.cursor()) as cs:
        cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        missing, uploaded = [], []

        for fname in REQUIRED_FILES:
            ext = Path(fname).suffix.lower()
            if ext not in ALLOWED_EXTS:
                continue

            file_path = OUT_DIR / fname
            stage = f"@ST_{Path(fname).stem.upper()}"

            if not file_path.is_file():
                missing.append(fname)
                print(f"[MISSING] {fname}")
                continue

            try:
                cs.execute(f"REMOVE {stage}")
            except Exception:
                pass

            put_sql = f"PUT 'file://{file_path.as_posix()}' {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            cs.execute(put_sql)

            uploaded.append(fname)
            print(f"[UPLOADED] {fname} -> {stage}")

        print("\n--- Résumé upload stages ---")
        print(f"OK : {len(uploaded)}")
        if uploaded:
            print("  " + "\n  ".join(uploaded))
        print(f"MANQUANTS : {len(missing)}")
        if missing:
            print("  " + "\n  ".join(missing))

if __name__ == "__main__":
    main()
