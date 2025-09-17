# 2) SCRIPT PYTHON — upload horodaté *plat* (.ndjson.gz à la racine du stage), TRUNCATE RAW, REFRESH, cleanup
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uploader → Internal Stages (fichiers plats horodatés .ndjson.gz) → TRUNCATE RAW → Snowpipe REFRESH → Cleanup
- Aucun sous-répertoire dans les stages: on renomme localement avant PUT.
- Pas d’append: TRUNCATE des *_RAW* avant chargement.
- Cleanup: suppression des fichiers LOADED du stage.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Set, Optional

from dotenv import load_dotenv
import snowflake.connector


# --- Chemins
BASE_DIR = Path(__file__).resolve().parent                  # .../scripts/Load_Snowflake
PROJECT_DIR = BASE_DIR.parent.parent                        # .../projet_absolute_cinema
OUT_DIR = PROJECT_DIR / "data" / "out"                      # dumps NDJSON
load_dotenv(PROJECT_DIR / ".env")

# --- Vars d'env
def _get_env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"ENV manquante: {name}")
    return v

SNOWFLAKE_USER      = _get_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = _get_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT   = _get_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = _get_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = _get_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = _get_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")

# --- Fichiers attendus
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

# stem -> (stage, pipe, raw_table, stage_stem)
MAP: Dict[str, Tuple[str, str, str, str]] = {
    "certification_movies":       ("BUCKET.ST_CERTIFICATION_MOVIES",    "BUCKET.PIPE_CERTIFICATION_MOVIES",      "BUCKET.CERTIFICATION_MOVIES_RAW",      "certification_movies"),
    "certification_series":       ("BUCKET.ST_CERTIFICATION_SERIES",    "BUCKET.PIPE_CERTIFICATION_SERIES",      "BUCKET.CERTIFICATION_SERIES_RAW",      "certification_series"),
    "company_details":            ("BUCKET.ST_COMPANY_DETAILS",         "BUCKET.PIPE_COMPANY_DETAILS",           "BUCKET.COMPANY_DETAILS_RAW",           "company_details"),
    "movie_alternative_titles":   ("BUCKET.ST_MOVIE_ALTERNATIVE_TITLES","BUCKET.PIPE_MOVIE_ALTERNATIVE_TITLES", "BUCKET.MOVIE_ALTERNATIVE_TITLES_RAW",  "movie_alternative_titles"),
    "movie_credits":              ("BUCKET.ST_MOVIE_CREDITS",           "BUCKET.PIPE_MOVIE_CREDITS",             "BUCKET.MOVIE_CREDITS_RAW",             "movie_credits"),
    "movie_details":              ("BUCKET.ST_MOVIE_DETAILS",           "BUCKET.PIPE_MOVIE_DETAILS",             "BUCKET.MOVIE_DETAILS_RAW",             "movie_details"),
    "movie_external_ids":         ("BUCKET.ST_MOVIE_EXTERNAL_IDS",      "BUCKET.PIPE_MOVIE_EXTERNAL_IDS",        "BUCKET.MOVIE_EXTERNAL_IDS_RAW",        "movie_external_ids"),
    "movie_keywords":             ("BUCKET.ST_MOVIE_KEYWORDS",          "BUCKET.PIPE_MOVIE_KEYWORDS",            "BUCKET.MOVIE_KEYWORDS_RAW",            "movie_keywords"),
    "movie_release_dates":        ("BUCKET.ST_MOVIE_RELEASE_DATES",     "BUCKET.PIPE_MOVIE_RELEASE_DATES",       "BUCKET.MOVIE_RELEASE_DATES_RAW",       "movie_release_dates"),
    "movie_reviews":              ("BUCKET.ST_MOVIE_REVIEWS",           "BUCKET.PIPE_MOVIE_REVIEWS",             "BUCKET.MOVIE_REVIEWS_RAW",             "movie_reviews"),
    "movie_translations":         ("BUCKET.ST_MOVIE_TRANSLATIONS",      "BUCKET.PIPE_MOVIE_TRANSLATIONS",        "BUCKET.MOVIE_TRANSLATIONS_RAW",        "movie_translations"),
    "people_details":             ("BUCKET.ST_PEOPLE_DETAILS",          "BUCKET.PIPE_PEOPLE_DETAILS",            "BUCKET.PEOPLE_DETAILS_RAW",            "people_details"),
    "ref_countries":              ("BUCKET.ST_REF_COUNTRIES",           "BUCKET.PIPE_REF_COUNTRIES",             "BUCKET.REF_COUNTRIES_RAW",             "ref_countries"),
    "ref_genre_movies":           ("BUCKET.ST_REF_GENRE_MOVIES",        "BUCKET.PIPE_REF_GENRE_MOVIES",          "BUCKET.REF_GENRE_MOVIES_RAW",          "ref_genre_movies"),
    "ref_genre_series":           ("BUCKET.ST_REF_GENRE_SERIES",        "BUCKET.PIPE_REF_GENRE_SERIES",          "BUCKET.REF_GENRE_SERIES_RAW",          "ref_genre_series"),
    "ref_languages":              ("BUCKET.ST_REF_LANGUAGES",           "BUCKET.PIPE_REF_LANGUAGES",             "BUCKET.REF_LANGUAGES_RAW",             "ref_languages"),
    "tv_episodes_details":        ("BUCKET.ST_TV_EPISODES_DETAILS",     "BUCKET.PIPE_TV_EPISODES_DETAILS",       "BUCKET.TV_EPISODES_DETAILS_RAW",       "tv_episodes_details"),
    "tv_networks_details":        ("BUCKET.ST_TV_NETWORKS_DETAILS",     "BUCKET.PIPE_TV_NETWORKS_DETAILS",       "BUCKET.TV_NETWORKS_DETAILS_RAW",       "tv_networks_details"),
    "tv_seasons_details":         ("BUCKET.ST_TV_SEASONS_DETAILS",      "BUCKET.PIPE_TV_SEASONS_DETAILS",        "BUCKET.TV_SEASONS_DETAILS_RAW",        "tv_seasons_details"),
    "tv_series_alternative_titles":("BUCKET.ST_TV_SERIES_ALTERNATIVE_TITLES","BUCKET.PIPE_TV_SERIES_ALTERNATIVE_TITLES","BUCKET.TV_SERIES_ALTERNATIVE_TITLES_RAW","tv_series_alternative_titles"),
    "tv_series_content_ratings":  ("BUCKET.ST_TV_SERIES_CONTENT_RATINGS","BUCKET.PIPE_TV_SERIES_CONTENT_RATINGS","BUCKET.TV_SERIES_CONTENT_RATINGS_RAW","tv_series_content_ratings"),
    "tv_series_credits":          ("BUCKET.ST_TV_SERIES_CREDITS",       "BUCKET.PIPE_TV_SERIES_CREDITS",         "BUCKET.TV_SERIES_CREDITS_RAW",         "tv_series_credits"),
    "tv_series_details":          ("BUCKET.ST_TV_SERIES_DETAILS",       "BUCKET.PIPE_TV_SERIES_DETAILS",         "BUCKET.TV_SERIES_DETAILS_RAW",         "tv_series_details"),
    "tv_series_external_ids":     ("BUCKET.ST_TV_SERIES_EXTERNAL_IDS",  "BUCKET.PIPE_TV_SERIES_EXTERNAL_IDS",    "BUCKET.TV_SERIES_EXTERNAL_IDS_RAW",    "tv_series_external_ids"),
    "tv_series_keywords":         ("BUCKET.ST_TV_SERIES_KEYWORDS",      "BUCKET.PIPE_TV_SERIES_KEYWORDS",        "BUCKET.TV_SERIES_KEYWORDS_RAW",        "tv_series_keywords"),
    "tv_series_reviews":          ("BUCKET.ST_TV_SERIES_REVIEWS",       "BUCKET.PIPE_TV_SERIES_REVIEWS",         "BUCKET.TV_SERIES_REVIEWS_RAW",         "tv_series_reviews"),
    "tv_series_translations":     ("BUCKET.ST_TV_SERIES_TRANSLATIONS",  "BUCKET.PIPE_TV_SERIES_TRANSLATIONS",    "BUCKET.TV_SERIES_TRANSLATIONS_RAW",    "tv_series_translations"),
    "watch_providers_movies":     ("BUCKET.ST_WATCH_PROVIDERS_MOVIES",  "BUCKET.PIPE_WATCH_PROVIDERS_MOVIES",    "BUCKET.WATCH_PROVIDERS_MOVIES_RAW",    "watch_providers_movies"),
    "watch_providers_series":     ("BUCKET.ST_WATCH_PROVIDERS_SERIES",  "BUCKET.PIPE_WATCH_PROVIDERS_SERIES",    "BUCKET.WATCH_PROVIDERS_SERIES_RAW",    "watch_providers_series"),
}

def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def ensure_context(cs) -> None:
    cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

def put_flat_with_timestamp(cs, local_file: Path, stage_name: str, stage_stem: str, ts: str) -> str:
    """
    Renomme localement -> PUT à la racine du stage -> retourne le nom final sur le stage (.gz)
    Ex: local movie_details.ndjson => stage movie_details_YYYYMMDDTHHMMSSZ.ndjson.gz
    """
    tmp_dir = local_file.parent / "_tmp_upload"
    tmp_dir.mkdir(exist_ok=True)
    tmp_src = tmp_dir / f"{stage_stem}_{ts}{local_file.suffix}"
    shutil.copy2(local_file, tmp_src)

    put_sql = (
        f"PUT 'file://{tmp_src.as_posix()}' "
        f"@{stage_name} "
        f"AUTO_COMPRESS=TRUE OVERWRITE=FALSE"
    )
    cs.execute(put_sql)

    staged_name = f"{tmp_src.name}.gz"  # Auto-compress ajoute .gz
    tmp_src.unlink(missing_ok=True)
    return staged_name

def refresh_pipe(cs, pipe_name: str) -> None:
    cs.execute(f"ALTER PIPE {pipe_name} REFRESH")

def loaded_files_for_table(cs, raw_table: str, stem: str, hours_back: int = 12) -> List[str]:
    q = f"""
    SELECT DISTINCT FILE_NAME
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => '{raw_table}',
        START_TIME => DATEADD('hour', -{int(hours_back)}, CURRENT_TIMESTAMP())
    ))
    WHERE STATUS = 'LOADED'
      AND FILE_NAME ILIKE '{stem}_%'
    """
    cs.execute(q)
    return [r[0] for r in cs.fetchall()]

def list_stage_files(cs, stage_name: str) -> List[str]:
    rows = cs.execute(f"LIST @{stage_name}").fetchall()
    return [Path(r[0]).name for r in rows]

def remove_from_stage(cs, stage_name: str, file_name: str) -> None:
    cs.execute(f"REMOVE @{stage_name}/{file_name}")

def main() -> None:
    if not OUT_DIR.is_dir():
        raise FileNotFoundError(f"Dossier introuvable: {OUT_DIR}")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE if SNOWFLAKE_ROLE else None,
        client_session_keep_alive=True,
    )
    cs = conn.cursor()

    try:
        ensure_context(cs)
        ts = utc_ts()

        touched: List[Tuple[str, str, str, str]] = []  # (stage, pipe, raw, stem)
        uploaded: List[str] = []
        missing: List[str] = []

        # 1) Upload plat horodaté
        for fname in REQUIRED_FILES:
            p = OUT_DIR / fname
            if p.suffix.lower() not in ALLOWED_EXTS:
                print(f"[SKIP_EXT] {fname}")
                continue
            if not p.is_file():
                print(f"[MISSING] {fname}")
                missing.append(fname)
                continue

            stem = p.stem
            if stem not in MAP:
                print(f"[SKIP_MAP] {fname}")
                continue

            stage_name, pipe_name, raw_table, stage_stem = MAP[stem]
            staged_name = put_flat_with_timestamp(cs, p, stage_name, stage_stem, ts)
            print(f"[UPLOADED] {fname} -> @{stage_name}/{staged_name}")
            touched.append((stage_name, pipe_name, raw_table, stage_stem))
            uploaded.append(fname)

        # 2) Garde-fou: vérifier qu’on voit bien les fichiers horodatés avant de TRUNCATE
        for stage_name, _, raw_table, stage_stem in touched:
            names = list_stage_files(cs, stage_name)
            expected_prefix = f"{stage_stem}_{ts}"
            if not any(n.startswith(expected_prefix) and n.endswith(".ndjson.gz") for n in names):
                raise RuntimeError(f"Aucun fichier horodaté détecté dans {stage_name} (prefix={expected_prefix}). Abandon.")

        # 3) TRUNCATE RAW
        raw_to_truncate = sorted({raw for _, _, raw, _ in touched})
        for raw in raw_to_truncate:
            cs.execute(f"TRUNCATE TABLE {raw}")
            print(f"[TRUNCATE] {raw}")

        # 4) REFRESH des pipes
        pipes = sorted({pipe for _, pipe, _, _ in touched})
        for pipe in pipes:
            refresh_pipe(cs, pipe)
            print(f"[REFRESH] {pipe}")

        # 5) Cleanup des stages après LOADED
        for stage_name, _, raw_table, stage_stem in touched:
            try:
                loaded = loaded_files_for_table(cs, raw_table, stage_stem, hours_back=12)
                for fname in loaded:
                    remove_from_stage(cs, stage_name, fname)
                    print(f"[REMOVE] @{stage_name}/{fname}")
            except Exception as e:
                print(f"[CLEANUP_WARN] stage={stage_name} table={raw_table} err={e}")

        # 6) Résumé
        print("\n--- Résumé ---")
        print(f"Uploads: {len(uploaded)}")
        print(f"Manquants: {len(missing)}")
        print(f"RAW truncatés: {len(raw_to_truncate)}")
        print(f"Pipes rafraîchies: {len(pipes)}")

    finally:
        try:
            cs.close()
        finally:
            conn.close()


if __name__ == "__main__":
    main()
