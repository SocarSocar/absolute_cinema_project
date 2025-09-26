# app/api.py
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import snowflake.connector

from datetime import date, datetime

API_VERSION = "v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

# --------- Config (depuis .env injecté par compose) ----------
def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val or ""

SNOW_USER = _getenv("SNOWFLAKE_USER", required=True)
SNOW_PASSWORD = _getenv("SNOWFLAKE_PASSWORD", required=True)
SNOW_ACCOUNT = _getenv("SNOWFLAKE_ACCOUNT", required=True)
SNOW_WAREHOUSE = _getenv("SNOWFLAKE_WAREHOUSE", required=True)
SNOW_DATABASE = _getenv("SNOWFLAKE_DATABASE", required=True)

# Schéma GOLD résolu prudemment: priorité à *_SCHEMA_GOLD, sinon *_SCHEMA_ML, sinon *_SCHEMA, sinon "GOLD"
SCHEMA_GOLD = (
    os.getenv("SNOWFLAKE_SCHEMA_GOLD")
    or os.getenv("SNOWFLAKE_SCHEMA_ML")
    or os.getenv("SNOWFLAKE_SCHEMA")
    or "GOLD"
)

SNOW_ROLE = os.getenv("SNOWFLAKE_ROLE")  # optionnel

# Table mapping — ajuste ici si vos noms diffèrent
TABLES = {
    "CONTENT": "FCT_CONTENT",
    "GENRE": "DIM_GENRE",
    "BR_CONTENT_GENRE": "BRIDGE_CONTENT_GENRE",
    "COMPANY": "DIM_COMPANY",
    "BR_CONTENT_COMPANY": "BRIDGE_CONTENT_COMPANY",
    "PERSON": "DIM_PERSON",
    "PERSON_ALIAS": "DIM_PERSON_ALIAS",
    "CREDIT": "FACT_CREDIT",
    "REVIEW": "FACT_REVIEW",
    "DATE": "DIM_DATE",
    "FINANCE": "FACT_REVENUE_BUDGET",
    "WATCH_PROVIDER": "DIM_WATCH_PROVIDER",
    "BR_CONTENT_PROVIDER": "BRIDGE_CONTENT_PROVIDER_MARKET",
    "MOVIE_FEATURES": "GLD_FACT_MOVIE_ML_NUMERIC",  # pour /features
}

def Q(name: str) -> str:
    """Retourne le nom pleinement qualifié DB.SCHEMA.TABLE"""
    return f'{SNOW_DATABASE}.{SCHEMA_GOLD}.{TABLES[name]}'

# --------- Connexion / exécution ----------
def get_conn():
    kwargs = dict(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        warehouse=SNOW_WAREHOUSE,
        database=SNOW_DATABASE,
        schema=SCHEMA_GOLD,
    )
    if SNOW_ROLE:
        kwargs["role"] = SNOW_ROLE
    return snowflake.connector.connect(**kwargs)

def run_query(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0].lower() for c in cur.description] if cur.description else []
            rows = cur.fetchall()
    if not cols:
        return []
    return [dict(zip(cols, r)) for r in rows]

def run_query_one(sql: str, params: Tuple[Any, ...]) -> Dict[str, Any]:
    out = run_query(sql, params)
    if not out:
        raise HTTPException(status_code=404, detail="Not found")
    return out[0]

def clamp_limit(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    return max(1, min(MAX_LIMIT, int(limit)))

def clamp_offset(offset: Optional[int]) -> int:
    if offset is None:
        return 0
    return max(0, int(offset))

# --------- Modèles (simples) ----------
class Genre(BaseModel):
    genre_id: Optional[int] = None
    name: Optional[str] = None

class Company(BaseModel):
    company_id: Optional[int] = None
    name: Optional[str] = None
    origin_country: Optional[str] = None
    headquarters: Optional[str] = None

class Provider(BaseModel):
    provider_id: Optional[int] = None
    name: Optional[str] = None
    type: Optional[str] = None

class Credit(BaseModel):
    person_id: Optional[int] = None
    name: Optional[str] = None
    job: Optional[str] = None
    character: Optional[str] = None
    order: Optional[int] = None

class Finance(BaseModel):
    budget: Optional[float] = None
    revenue: Optional[float] = None
    currency: Optional[str] = None

class Review(BaseModel):
    review_id: Optional[int] = None
    author: Optional[str] = None
    source: Optional[str] = None
    language: Optional[str] = None
    rating: Optional[float] = None
    title: Optional[str] = None
    content: Optional[str] = None
    date_id: Optional[Any] = None  # peut être int/date/datetime/str selon GOLD

class Content(BaseModel):
    id_content: str
    content_type: Optional[str] = None
    native_id: Optional[int] = None
    movie_id: Optional[int] = None
    series_id: Optional[int] = None
    season_id: Optional[int] = None
    episode_id: Optional[int] = None
    title: Optional[str] = None
    original_language: Optional[str] = None
    content_date: Optional[date] = None   # <= ICI: était Optional[str]
    popularity: Optional[float] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    runtime: Optional[int] = None
    number_of_seasons: Optional[int] = None
    number_of_episodes: Optional[int] = None
    genres: Optional[List[Genre]] = None
    companies: Optional[List[Company]] = None
    providers: Optional[List[Provider]] = None
    cast: Optional[List[Credit]] = None


class PageMeta(BaseModel):
    limit: int
    offset: int
    returned: int

# Pages concrètes (évite les génériques Pydantic v2 pour l’OpenAPI)
class ContentPage(BaseModel):
    meta: PageMeta
    items: List[Content]

class ReviewPage(BaseModel):
    meta: PageMeta
    items: List[Review]

class DictPage(BaseModel):
    meta: PageMeta
    items: List[Dict[str, Any]]

# --------- App ----------
logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Absolute Cinema API", version=API_VERSION)

# --------- Helpers d’enrichissement ----------
def fetch_genres(id_content: str) -> List[Genre]:
    sql = f"""
        SELECT g.genre_id, g.name
        FROM {Q('BR_CONTENT_GENRE')} bg
        JOIN {Q('GENRE')} g USING(genre_id)
        WHERE bg.id_content = %s
        ORDER BY g.name
    """
    return [Genre(**r) for r in run_query(sql, (id_content,))]

def fetch_companies(id_content: str) -> List[Company]:
    sql = f"""
        SELECT c.company_id, c.name, c.origin_country, c.headquarters
        FROM {Q('BR_CONTENT_COMPANY')} bc
        JOIN {Q('COMPANY')} c USING(company_id)
        WHERE bc.id_content = %s
        ORDER BY c.name
    """
    return [Company(**r) for r in run_query(sql, (id_content,))]

def fetch_providers(id_content: str) -> List[Provider]:
    # Colonnes réelles fréquentes: BRIDGE: watch_provider_id / DIM: watch_provider_id, name, provider_type
    sql = f"""
        SELECT
            wp.watch_provider_id AS provider_id,
            wp.name AS name,
            wp.provider_type AS type
        FROM {Q('BR_CONTENT_PROVIDER')} bp
        JOIN {Q('WATCH_PROVIDER')} wp
          ON wp.watch_provider_id = bp.watch_provider_id
        WHERE bp.id_content = %s
        ORDER BY wp.name
    """
    try:
        return [Provider(**r) for r in run_query(sql, (id_content,))]
    except Exception as e:
        # Fallback: certaines implémentations utilisent 'type' au lieu de 'provider_type'
        sql_alt = f"""
            SELECT
                wp.watch_provider_id AS provider_id,
                wp.name AS name,
                wp.type AS type
            FROM {Q('BR_CONTENT_PROVIDER')} bp
            JOIN {Q('WATCH_PROVIDER')} wp
              ON wp.watch_provider_id = bp.watch_provider_id
            WHERE bp.id_content = %s
            ORDER BY wp.name
        """
        return [Provider(**r) for r in run_query(sql_alt, (id_content,))]

def fetch_cast(id_content: str, limit: int = 20) -> List[Credit]:
    sql = f"""
        SELECT fc.person_id, p.name, fc.job, fc.character, fc."order"
        FROM {Q('CREDIT')} fc
        JOIN {Q('PERSON')} p USING(person_id)
        WHERE fc.id_content = %s
        ORDER BY fc."order" ASC NULLS LAST
        LIMIT %s
    """
    return [Credit(**r) for r in run_query(sql, (id_content, limit))]

# --------- Endpoints: CONTENT ----------
@app.get(f"/{API_VERSION}/content/by-title", response_model=ContentPage)
def content_by_title(
    q: str = Query(..., min_length=1),
    content_type: Optional[str] = Query(None, pattern="^(movie|series)$"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)
    clauses = ["title ILIKE %s"]
    params: List[Any] = [f"%{q}%"]
    if content_type:
        clauses.append("content_type = %s")
        params.append(content_type)
    where = " AND ".join(clauses)
    sql = f"""
        SELECT id_content, content_type, native_id, movie_id, series_id, season_id, episode_id,
               title, original_language, content_date, popularity, vote_average, vote_count,
               runtime, number_of_seasons, number_of_episodes
        FROM {Q('CONTENT')}
        WHERE {where}
        ORDER BY popularity DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    rows = run_query(sql, tuple(params))
    items = [Content(**r) for r in rows]
    return ContentPage(meta=PageMeta(limit=limit, offset=offset, returned=len(items)), items=items)

@app.get(f"/{API_VERSION}/content/{{id_content}}", response_model=Content)
def content_detail(id_content: str):
    sql = f"""
        SELECT id_content, content_type, native_id, movie_id, series_id, season_id, episode_id,
               title, original_language, content_date, popularity, vote_average, vote_count,
               runtime, number_of_seasons, number_of_episodes
        FROM {Q('CONTENT')}
        WHERE id_content = %s
        LIMIT 1
    """
    base = run_query_one(sql, (id_content,))
    c = Content(**base)

    # Enrichissements tolérants: ne jamais casser la route si une jointure échoue
    try:
        c.genres = fetch_genres(id_content)
    except Exception:
        c.genres = []

    try:
        c.companies = fetch_companies(id_content)
    except Exception:
        c.companies = []

    try:
        c.providers = fetch_providers(id_content)
    except Exception:
        c.providers = []

    try:
        c.cast = fetch_cast(id_content, 20)
    except Exception:
        c.cast = []

    return c

# --------- Endpoints: REVIEWS / FINANCE / PROVIDERS ----------
@app.get(f"/{API_VERSION}/content/{{id_content}}/reviews", response_model=ReviewPage)
def content_reviews(
    id_content: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)

    # Variantes de schéma courantes pour FACT_REVIEW
    selects = [
        # v1: colonnes "simples"
        f"""
        SELECT review_id, author, source, language, rating, title, content, date_id
        FROM {Q('REVIEW')}
        WHERE id_content = %s
        ORDER BY date_id DESC
        LIMIT %s OFFSET %s
        """,
        # v2: source renommé en review_source
        f"""
        SELECT review_id, author, review_source AS source, language, rating, title, content, date_id
        FROM {Q('REVIEW')}
        WHERE id_content = %s
        ORDER BY date_id DESC
        LIMIT %s OFFSET %s
        """,
        # v3: source = site ; titre/texte préfixés
        f"""
        SELECT review_id, author, site AS source, language, rating, review_title AS title, review_text AS content, date_id
        FROM {Q('REVIEW')}
        WHERE id_content = %s
        ORDER BY date_id DESC
        LIMIT %s OFFSET %s
        """,
        # v4: variations lang/score/date
        f"""
        SELECT review_id, author, site AS source, lang AS language, score AS rating, review_title AS title, review_text AS content, review_date AS date_id
        FROM {Q('REVIEW')}
        WHERE id_content = %s
        ORDER BY review_date DESC
        LIMIT %s OFFSET %s
        """,
    ]

    last_err = None
    for sql in selects:
        try:
            rows = run_query(sql, (id_content, limit, offset))
            items = [Review(**r) for r in rows]
            return ReviewPage(
                meta=PageMeta(limit=limit, offset=offset, returned=len(items)),
                items=items,
            )
        except Exception as e:
            last_err = e
            continue

    # Si toutes les variantes échouent, on renvoie une 500 explicite avec le message Snowflake
    raise HTTPException(status_code=500, detail=f"FACT_REVIEW schema mismatch: {last_err}")


@app.get(f"/{API_VERSION}/content/{{id_content}}/finance", response_model=Finance)
def content_finance(id_content: str):
    sql = f"""
        SELECT budget, revenue, currency
        FROM {Q('FINANCE')}
        WHERE id_content = %s
        ORDER BY /* au cas où */ 1
        LIMIT 1
    """
    row = run_query(sql, (id_content,))
    if not row:
        return Finance()  # vide si pas de finance
    return Finance(**row[0])

@app.get(f"/{API_VERSION}/content/{{id_content}}/providers", response_model=List[Provider])
def content_providers(id_content: str):
    return fetch_providers(id_content)

# --------- Endpoints: FEATURES (ex-ML num) ----------
@app.get(f"/{API_VERSION}/content/{{movie_id}}/features")
def movie_features(movie_id: int):
    sql = f"""
        SELECT *
        FROM {Q('MOVIE_FEATURES')}
        WHERE movie_id = %s
        LIMIT 1
    """
    row = run_query(sql, (movie_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Features not found")
    # Retour brut: dict colonnes -> valeurs numériques
    return row[0]

# --------- Endpoints: PERSON ----------
@app.get(f"/{API_VERSION}/person/by-name", response_model=DictPage)
def person_by_name(
    q: str = Query(..., min_length=1),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)
    # Match sur nom et alias
    sql = f"""
        WITH matches AS (
            SELECT p.person_id, p.name
            FROM {Q('PERSON')} p
            WHERE p.name ILIKE %s
            UNION
            SELECT p.person_id, p.name
            FROM {Q('PERSON_ALIAS')} a
            JOIN {Q('PERSON')} p USING(person_id)
            WHERE a.alias ILIKE %s
        )
        SELECT DISTINCT person_id, name
        FROM matches
        ORDER BY name
        LIMIT %s OFFSET %s
    """
    rows = run_query(sql, (f"%{q}%", f"%{q}%", limit, offset))
    return DictPage(meta=PageMeta(limit=limit, offset=offset, returned=len(rows)), items=rows)

@app.get(f"/{API_VERSION}/person/{{person_id}}", response_model=Dict[str, Any])
def person_detail(person_id: int):
    base = run_query_one(
        f"SELECT person_id, name FROM {Q('PERSON')} WHERE person_id = %s LIMIT 1",
        (person_id,),
    )
    credits = run_query(
        f"""
        SELECT fc.id_content, fc.job, fc.character, fc."order",
               c.title, c.content_type, c.popularity, c.vote_average, c.vote_count, c.content_date
        FROM {Q('CREDIT')} fc
        JOIN {Q('CONTENT')} c USING(id_content)
        WHERE fc.person_id = %s
        ORDER BY c.popularity DESC NULLS LAST, fc."order" ASC NULLS LAST
        """,
        (person_id,),
    )
    base["credits"] = credits
    return base

# --------- Endpoints: STATS ----------
@app.get(f"/{API_VERSION}/stats/genre", response_model=List[Dict[str, Any]])
def stats_genre(
    metric: str = Query("vote_average", pattern="^(vote_average|popularity)$"),
    top: int = Query(20, ge=1, le=200),
):
    if metric == "vote_average":
        sql = f"""
            SELECT g.genre_id, g.name,
                   AVG(c.vote_average) AS vote_average,
                   AVG(c.popularity)  AS popularity,
                   COUNT(*) AS n
            FROM {Q('CONTENT')} c
            JOIN {Q('BR_CONTENT_GENRE')} bg ON bg.id_content = c.id_content
            JOIN {Q('GENRE')} g ON g.genre_id = bg.genre_id
            GROUP BY 1,2
            HAVING COUNT(*) > 0
            ORDER BY vote_average DESC NULLS LAST
            LIMIT %s
        """
        return run_query(sql, (top,))
    else:
        sql = f"""
            SELECT g.genre_id, g.name,
                   AVG(c.vote_average) AS vote_average,
                   AVG(c.popularity)  AS popularity,
                   COUNT(*) AS n
            FROM {Q('CONTENT')} c
            JOIN {Q('BR_CONTENT_GENRE')} bg ON bg.id_content = c.id_content
            JOIN {Q('GENRE')} g ON g.genre_id = bg.genre_id
            GROUP BY 1,2
            HAVING COUNT(*) > 0
            ORDER BY popularity DESC NULLS LAST
            LIMIT %s
        """
        return run_query(sql, (top,))
