#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from datetime import datetime
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
OUT_DIR = PROJECT_DIR / "data" / "ML"                  # .../data/out
ENV_PATH = PROJECT_DIR / ".env"                         # secrets live here

# ---------- Env helpers ----------
def _get_env(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise RuntimeError(f"Missing required env var: {var}")
    return val

if load_dotenv is not None and ENV_PATH.exists():
    load_dotenv(ENV_PATH)

SNOWFLAKE_USER         = _get_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD     = _get_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT      = _get_env("SNOWFLAKE_ACCOUNT")          # e.g. orgname-account or account.region
SNOWFLAKE_WAREHOUSE    = _get_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE     = _get_env("SNOWFLAKE_DATABASE")         # TMDB_ETL
SNOWFLAKE_SCHEMA_ML    = _get_env("SNOWFLAKE_SCHEMA_ML")        # e.g. GOLD (spécifique ML)
SNOWFLAKE_ROLE         = os.getenv("SNOWFLAKE_ROLE")            # optional
# Optional: limit de sécurité pour tests (ex: "100000"); vide = pas de LIMIT
ROW_LIMIT              = os.getenv("ROW_LIMIT", "").strip()

TABLE_NAME             = "GLD_FACT_MOVIE_ML_NUMERIC"

# ---------- Snowflake connection ----------
def connect_snowflake():
    params = {
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "account": SNOWFLAKE_ACCOUNT,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA_ML,
    }
    if SNOWFLAKE_ROLE:
        params["role"] = SNOWFLAKE_ROLE
    return snowflake.connector.connect(**params)

# ---------- Extraction ----------
def build_query() -> str:
    base = f'SELECT * FROM "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA_ML}"."{TABLE_NAME}"'
    if ROW_LIMIT.isdigit():
        return f"{base} LIMIT {ROW_LIMIT}"
    return base

# ---------- Extraction ----------
def export_to_local_files():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / "GLD_FACT_MOVIE_ML_NUMERIC.csv"
    parquet_path = OUT_DIR / "GLD_FACT_MOVIE_ML_NUMERIC.parquet"

    sql = build_query()

    with closing(connect_snowflake()) as conn, closing(conn.cursor()) as cur:
        cur.execute(sql)
        df = cur.fetch_pandas_all()

    # Sauvegardes locales (elles écrasent si déjà existantes)
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception:
        parquet_path = None

    return str(csv_path), (str(parquet_path) if parquet_path else None), len(df)

# ---------- Main ----------
def main():
    csv_path, parquet_path, n_rows = export_to_local_files()
    print(f"[OK] {n_rows} lignes extraites depuis {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_ML}.{TABLE_NAME}")
    print(f"[OK] CSV : {csv_path}")
    if parquet_path:
        print(f"[OK] Parquet : {parquet_path}")

if __name__ == "__main__":
    main()
