# app/api.py
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import snowflake.connector


# ===========================
# API METADATA / OPENAPI DOCS
# ===========================

API_VERSION = "v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

TAGS_METADATA = [
    {
        "name": "content",
        "description": (
            "Endpoints centrés sur les œuvres (films/séries/saisons/épisodes). "
            "Incluent recherche par titre, fiche détaillée, reviews, finance et providers."
        ),
    },
    {
        "name": "person",
        "description": (
            "Endpoints centrés sur les personnes (acteurs, réalisateurs, etc.). "
            "Incluent recherche par nom et fiche détaillée avec filmographie."
        ),
    },
    {
        "name": "features",
        "description": (
            "Accès brut aux colonnes numériques normalisées liées aux films "
            "pour exploitation analytique. "
            "Aucune logique ML ici : simple exposition des *features*."
        ),
    },
    {
        "name": "stats",
        "description": (
            "Statistiques agrégées prêtes à l’emploi (ex: agrégations par genre)."
        ),
    },
]

APP_DESCRIPTION = """
API de lecture **directe** sur Snowflake (couche GOLD).  
Aucun import local. **Toutes** les requêtes exécutent du SQL côté Snowflake et renvoient du JSON propre.

### Principes d’usage
- **Filtrage & pagination** systématiques quand utile (`limit`/`offset`).
- **Paramétrage SQL** sécurisé (binds `%s`, pas de concaténation de valeurs).
- **Schéma GOLD** détecté automatiquement via variables d’environnement.

### Schéma attendu (extraits)
- Pivot: `FCT_CONTENT`  
- Genres: `BRIDGE_CONTENT_GENRE` + `DIM_GENRE`  
- Comptes/Studios: `BRIDGE_CONTENT_COMPANY` + `DIM_COMPANY`  
- Providers: `BRIDGE_CONTENT_PROVIDER_MARKET` + `DIM_WATCH_PROVIDER`  
- Cast/Crew: `FACT_CREDIT` + `DIM_PERSON` (+ `DIM_PERSON_ALIAS`)  
- Reviews: `FACT_REVIEW`  
- Finance: `FACT_REVENUE_BUDGET`  
- Features numériques: `GLD_FACT_MOVIE_ML_NUMERIC`

### Authentification
Aucune. L’API sert uniquement l’interrogation lecture.

### Limites
- `limit` borné à {MAX_LIMIT}.
- Les agrégations sont volontairement simples pour garder les latences faibles.
""".replace("{MAX_LIMIT}", str(MAX_LIMIT))


# ===============
# CONFIG ENV VARS
# ===============

def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    """Lecture stricte des variables d’environnement. Lève si `required=True` et vide."""
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val or ""

SNOW_USER = _getenv("SNOWFLAKE_USER", required=True)
SNOW_PASSWORD = _getenv("SNOWFLAKE_PASSWORD", required=True)
SNOW_ACCOUNT = _getenv("SNOWFLAKE_ACCOUNT", required=True)
SNOW_WAREHOUSE = _getenv("SNOWFLAKE_WAREHOUSE_API", required=True)
SNOW_DATABASE = _getenv("SNOWFLAKE_DATABASE", required=True)

# Résolution prudente du schéma GOLD
SCHEMA_GOLD = (
    os.getenv("SNOWFLAKE_SCHEMA_GOLD")
    or os.getenv("SNOWFLAKE_SCHEMA_ML")
    or os.getenv("SNOWFLAKE_SCHEMA")
    or "GOLD"
)
SNOW_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Mapping des tables (adapter ici si vos noms diffèrent)
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
    "BR_CONTENT_PROVIDER": "BRIDGE_CONTENT_PROVIDER_MARKET",  # + country_code
    "MOVIE_FEATURES": "GLD_FACT_MOVIE_ML_NUMERIC",
}

def Q(name: str) -> str:
    """Nom pleinement qualifié DB.SCHEMA.TABLE pour Snowflake."""
    return f'{SNOW_DATABASE}.{SCHEMA_GOLD}.{TABLES[name]}'


# ============================
# CONNEXION / EXÉCUTION SQL
# ============================

def get_conn():
    """
    Ouvre une connexion Snowflake avec le warehouse / database / schema définis.
    Fermeture automatique via context manager.
    """
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
    """
    Exécute une requête **SELECT** paramétrée et renvoie une liste de dicts (snake_case).
    Utilise `cursor.description` pour récupérer les noms de colonnes.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0].lower() for c in cur.description] if cur.description else []
            rows = cur.fetchall()
    if not cols:
        return []
    return [dict(zip(cols, r)) for r in rows]

def run_query_one(sql: str, params: Tuple[Any, ...]) -> Dict[str, Any]:
    """
    Exécute une requête **SELECT** et renvoie **un seul** enregistrement. 404 si vide.
    """
    out = run_query(sql, params)
    if not out:
        raise HTTPException(status_code=404, detail="Not found")
    return out[0]

def clamp_limit(limit: Optional[int]) -> int:
    """Borne `limit` entre 1 et `MAX_LIMIT`, défaut `DEFAULT_LIMIT`."""
    if limit is None:
        return DEFAULT_LIMIT
    return max(1, min(MAX_LIMIT, int(limit)))

def clamp_offset(offset: Optional[int]) -> int:
    """Borne `offset` à min 0, défaut 0."""
    if offset is None:
        return 0
    return max(0, int(offset))


# =====================
# Pydantic: SCHEMAS DOC
# =====================

class Genre(BaseModel):
    genre_id: Optional[int] = Field(None, description="Identifiant genre")
    name: Optional[str] = Field(None, description="Libellé du genre")

class Company(BaseModel):
    company_id: Optional[int] = Field(None, description="Identifiant société")
    name: Optional[str] = Field(None, description="Nom")
    origin_country: Optional[str] = Field(None, description="Pays d’origine (code ISO)")
    headquarters: Optional[str] = Field(None, description="Siège social")

class Provider(BaseModel):
    provider_id: Optional[int] = Field(None, description="Identifiant provider")
    provider_name: Optional[str] = Field(None, description="Nom commercial du provider")
    country_code: Optional[str] = Field(None, description="Code pays marché (ex: FR, US)")

class Credit(BaseModel):
    person_id: Optional[int] = Field(None, description="Identifiant personne")
    name: Optional[str] = Field(None, description="Nom de la personne")
    department: Optional[str] = Field(None, description="Département (ex: Acting, Directing)")
    job: Optional[str] = Field(None, description="Métier (ex: Director, Writer)")
    character: Optional[str] = Field(None, description="Nom du personnage (pour Acting)")
    order_idx: Optional[int] = Field(None, description="Ordre d’affichage du casting")

class Finance(BaseModel):
    budget: Optional[float] = Field(None, description="Budget estimé")
    revenue: Optional[float] = Field(None, description="Revenue estimé")

class Review(BaseModel):
    review_id: Optional[str] = Field(None, description="Identifiant review (chaîne)")
    author: Optional[str] = Field(None, description="Auteur de la review")
    content: Optional[str] = Field(None, description="Texte brut de la review")
    created_at: Optional[datetime] = Field(None, description="Horodatage création")
    url: Optional[str] = Field(None, description="URL source publique")

class Content(BaseModel):
    id_content: str = Field(..., description="Clé pivot interne (ex: `60059_serie`)")
    content_type: Optional[str] = Field(None, description="movie|series|season|episode")
    native_id: Optional[int] = Field(None, description="ID natif source (TMDB, etc.)")
    movie_id: Optional[int] = Field(None, description="ID film (si applicable)")
    series_id: Optional[int] = Field(None, description="ID série (si applicable)")
    season_id: Optional[int] = Field(None, description="ID saison (si applicable)")
    episode_id: Optional[int] = Field(None, description="ID épisode (si applicable)")
    title: Optional[str] = Field(None, description="Titre principal")
    original_language: Optional[str] = Field(None, description="Langue originale (code)")
    content_date: Optional[date] = Field(None, description="Date de sortie/diffusion")
    popularity: Optional[float] = Field(None, description="Score popularité")
    vote_average: Optional[float] = Field(None, description="Moyenne votes")
    vote_count: Optional[int] = Field(None, description="Volume votes")
    runtime: Optional[int] = Field(None, description="Durée (min) si pertinent")
    number_of_seasons: Optional[int] = Field(None, description="Nb saisons si série")
    number_of_episodes: Optional[int] = Field(None, description="Nb épisodes si série")
    genres: Optional[List[Genre]] = Field(None, description="Liste des genres")
    companies: Optional[List[Company]] = Field(None, description="Liste des sociétés liées")
    providers: Optional[List[Provider]] = Field(None, description="Liste des providers dispos")
    cast: Optional[List[Credit]] = Field(None, description="Liste casting/crew principaux")

class PageMeta(BaseModel):
    limit: int = Field(..., description="Taille page retournée")
    offset: int = Field(..., description="Décalage de début")
    returned: int = Field(..., description="Nombre d’items renvoyés")

class ContentPage(BaseModel):
    meta: PageMeta
    items: List[Content]

class ReviewPage(BaseModel):
    meta: PageMeta
    items: List[Review]

class DictPage(BaseModel):
    meta: PageMeta
    items: List[Dict[str, Any]]


# ==========
# APP INIT
# ==========

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Absolute Cinema API",
    version=API_VERSION,
    summary="API de lecture Snowflake (GOLD) pour contenus, personnes, reviews, finance, providers, features et stats.",
    description=APP_DESCRIPTION,
    contact={
        "name": "Absolute Cinema",
    },
    license_info={"name": "Propriétaire"},
    openapi_tags=TAGS_METADATA,
)


# ============================
# HELPERS ENRICHISSEMENTS DOC
# ============================

def fetch_genres(id_content: str) -> List[Genre]:
    """
    Genres associés à un `id_content`.

    Requêtes:
      - `BRIDGE_CONTENT_GENRE` → `DIM_GENRE`

    Ordre: alphabétique par nom de genre.
    """
    sql = f"""
        SELECT g.genre_id, g.name
        FROM {Q('BR_CONTENT_GENRE')} bg
        JOIN {Q('GENRE')} g USING(genre_id)
        WHERE bg.id_content = %s
        ORDER BY g.name
    """
    return [Genre(**r) for r in run_query(sql, (id_content,))]

def fetch_companies(id_content: str) -> List[Company]:
    """
    Sociétés associées à un `id_content`.

    Requêtes:
      - `BRIDGE_CONTENT_COMPANY` → `DIM_COMPANY`

    Ordre: alphabétique.
    """
    sql = f"""
        SELECT c.company_id, c.name, c.origin_country, c.headquarters
        FROM {Q('BR_CONTENT_COMPANY')} bc
        JOIN {Q('COMPANY')} c USING(company_id)
        WHERE bc.id_content = %s
        ORDER BY c.name
    """
    return [Company(**r) for r in run_query(sql, (id_content,))]

def fetch_providers(id_content: str) -> List[Provider]:
    """
    Providers disponibles pour un `id_content` par marché (country_code).

    Requêtes:
      - `BRIDGE_CONTENT_PROVIDER_MARKET` → `DIM_WATCH_PROVIDER`

    Ordre: alphabétique par `provider_name`.
    """
    sql = f"""
        SELECT
            wp.provider_id,
            wp.provider_name,
            bp.country_code
        FROM {Q('BR_CONTENT_PROVIDER')} bp
        JOIN {Q('WATCH_PROVIDER')} wp USING(provider_id)
        WHERE bp.id_content = %s
        ORDER BY wp.provider_name
    """
    return [Provider(**r) for r in run_query(sql, (id_content,))]

def fetch_cast(id_content: str, limit: int = 20) -> List[Credit]:
    """
    Casting/équipe principale d’un `id_content`.

    Requêtes:
      - `FACT_CREDIT` → `DIM_PERSON`

    Ordonne par `order_idx` croissant (nulls en fin).
    """
    sql = f"""
        SELECT
            fc.person_id,
            p.name,
            fc.department,
            fc.job,
            fc.character,
            fc.order_idx
        FROM {Q('CREDIT')} fc
        JOIN {Q('PERSON')} p USING(person_id)
        WHERE fc.id_content = %s
        ORDER BY COALESCE(fc.order_idx, 999999)
        LIMIT %s
    """
    return [Credit(**r) for r in run_query(sql, (id_content, limit))]


# =========================
# ENDPOINTS: CONTENT
# =========================

@app.get(
    f"/{API_VERSION}/content/by-title",
    response_model=ContentPage,
    tags=["content"],
    summary="Recherche de contenus par titre",
    description=(
        "Recherche *full-text* simple sur `title` avec filtre optionnel `content_type` "
        "(`movie|series|season|episode`). Retour paginé et trié par popularité décroissante."
    ),
    responses={
        200: {
            "description": "Liste paginée de contenus.",
            "content": {
                "application/json": {
                    "example": {
                        "meta": {"limit": 5, "offset": 0, "returned": 2},
                        "items": [
                            {
                                "id_content": "603_movie",
                                "content_type": "movie",
                                "title": "The Matrix",
                                "original_language": "en",
                                "content_date": "1999-03-31",
                                "popularity": 120.5,
                                "vote_average": 8.2,
                                "vote_count": 21000,
                            },
                            {
                                "id_content": "604_movie",
                                "content_type": "movie",
                                "title": "The Matrix Reloaded",
                                "original_language": "en",
                                "content_date": "2003-05-15",
                                "popularity": 85.2,
                                "vote_average": 7.0,
                                "vote_count": 15000,
                            },
                        ],
                    }
                }
            },
        }
    },
)
def content_by_title(
    content_name: str = Query(
        ...,
        min_length=1,
        description="Sous-chaîne insensible à la casse recherchée dans `title`.",
        examples={"ex": {"summary": "Exemple", "value": "Matrix"}},
    ),
    content_type: Optional[str] = Query(
        None,
        pattern="^(movie|series|season|episode)$",
        description="Filtre optionnel sur le type de contenu.",
    ),
    limit: int = Query(
        DEFAULT_LIMIT, ge=1, le=MAX_LIMIT,
        description=f"Nombre max d’items à renvoyer (≤ {MAX_LIMIT})."
    ),
    offset: int = Query(0, ge=0, description="Décalage de départ des résultats."),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)
    clauses = ["title ILIKE %s"]
    params: List[Any] = [f"%{content_name}%"]
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


@app.get(
    f"/{API_VERSION}/content/{{id_content}}",
    response_model=Content,
    tags=["content"],
    summary="Fiche détaillée d’un contenu",
    description=(
        "Retourne la fiche pivot enrichie (genres, sociétés, providers, cast) "
        "à partir de `id_content`."
    ),
    responses={
        200: {"description": "Objet `Content` enrichi."},
        404: {"description": "`id_content` introuvable."},
    },
)
def content_detail(id_content: str):
    sql = f"""
        SELECT id_content, content_type, native_id, movie_id, series_id, season_id, episode_id,
               title, original_language, content_date, popularity, vote_average, vote_count,
               runtime, number_of_seasons, number_of_episodes
        FROM {Q('CONTENT')}
        WHERE id_content = %s
        ORDER BY popularity DESC NULLS LAST
        LIMIT 1
    """
    base = run_query_one(sql, (id_content,))
    c = Content(**base)
    try: c.genres = fetch_genres(id_content)
    except Exception: c.genres = []
    try: c.companies = fetch_companies(id_content)
    except Exception: c.companies = []
    try: c.providers = fetch_providers(id_content)
    except Exception: c.providers = []
    try: c.cast = fetch_cast(id_content, 20)
    except Exception: c.cast = []
    return c


# ===============================
# ENDPOINTS: REVIEWS / FINANCE / PROVIDERS
# ===============================

@app.get(
    f"/{API_VERSION}/content/{{id_content}}/reviews",
    response_model=ReviewPage,
    tags=["content"],
    summary="Reviews d’un contenu",
    description="Liste paginée des reviews attachées à `id_content`, triées par `created_at` décroissant.",
    responses={200: {"description": "Page de reviews."}},
)
def content_reviews(
    id_content: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Taille page."),
    offset: int = Query(0, ge=0, description="Décalage."),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)
    sql = f"""
        SELECT review_id, author, content, created_at, url
        FROM {Q('REVIEW')}
        WHERE id_content = %s
        ORDER BY created_at DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    rows = run_query(sql, (id_content, limit, offset))
    items = [Review(**r) for r in rows]
    return ReviewPage(meta=PageMeta(limit=limit, offset=offset, returned=len(items)), items=items)


@app.get(
    f"/{API_VERSION}/content/{{id_content}}/finance",
    response_model=Finance,
    tags=["content"],
    summary="Budget/Revenue d’un contenu",
    description="Retourne budget et revenue agrégés attachés à `id_content` si disponibles.",
    responses={200: {"description": "Objet `Finance` (vide si non trouvé)."}},
)
def content_finance(id_content: str):
    sql = f"""
        SELECT budget, revenue
        FROM {Q('FINANCE')}
        WHERE id_content = %s
        ORDER BY 1
        LIMIT 1
    """
    row = run_query(sql, (id_content,))
    if not row:
        return Finance()
    return Finance(**row[0])


@app.get(
    f"/{API_VERSION}/content/{{id_content}}/providers",
    response_model=List[Provider],
    tags=["content"],
    summary="Providers par marché d’un contenu",
    description="Liste des providers disponibles pour `id_content`, avec `country_code`.",
    responses={200: {"description": "Liste de providers."}},
)
def content_providers(id_content: str):
    return fetch_providers(id_content)


# ===============================
# ENDPOINTS: FEATURES (NUMÉRIQUES)
# ===============================

@app.get(
    f"/{API_VERSION}/content/{{movie_id}}/features",
    tags=["features"],
    summary="Features numériques associées à un film",
    description=(
        "Retour brut d’une ligne de features numériques normalisées pour `movie_id` "
        "(table `GLD_FACT_MOVIE_ML_NUMERIC`). Pas de transformation supplémentaire."
    ),
    responses={
        200: {
            "description": "Dict colonnes → valeurs numériques.",
            "content": {"application/json": {"example": {"movie_id": 603, "feat_pop_norm": 0.81, "feat_vote_norm": 0.92}}},
        },
        404: {"description": "Aucune feature trouvée pour `movie_id`."},
    },
)
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
    return row[0]


# =========================
# ENDPOINTS: PERSON
# =========================

@app.get(
    f"/{API_VERSION}/person/by-name",
    response_model=DictPage,
    tags=["person"],
    summary="Recherche personnes par nom (avec alias)",
    description=(
        "Recherche insensible à la casse sur `DIM_PERSON.name` et `DIM_PERSON_ALIAS.aka`. "
        "Retour distinct des personnes matchées."
    ),
    responses={200: {"description": "Liste paginée de personnes (id, name)."}},
)
def person_by_name(
    q: str = Query(
        ...,
        min_length=1,
        description="Sous-chaîne recherchée sur nom et alias.",
        examples={"ex": {"summary": "Exemple", "value": "keanu"}},
    ),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Taille page."),
    offset: int = Query(0, ge=0, description="Décalage."),
):
    limit = clamp_limit(limit)
    offset = clamp_offset(offset)
    sql = f"""
        WITH matches AS (
            SELECT p.person_id, p.name
            FROM {Q('PERSON')} p
            WHERE p.name ILIKE %s
            UNION
            SELECT p.person_id, p.name
            FROM {Q('PERSON_ALIAS')} a
            JOIN {Q('PERSON')} p USING(person_id)
            WHERE a.aka ILIKE %s
        )
        SELECT DISTINCT person_id, name
        FROM matches
        ORDER BY name
        LIMIT %s OFFSET %s
    """
    rows = run_query(sql, (f"%{q}%", f"%{q}%", limit, offset))
    return DictPage(meta=PageMeta(limit=limit, offset=offset, returned=len(rows)), items=rows)


@app.get(
    f"/{API_VERSION}/person/{{person_id}}",
    response_model=Dict[str, Any],
    tags=["person"],
    summary="Fiche détaillée personne + crédits",
    description=(
        "Retourne l’en-tête personne et l’ensemble des crédits reliés "
        "(jointure `FACT_CREDIT` ↔ `FCT_CONTENT`). Tri: popularité contenu, puis `order_idx`."
    ),
    responses={
        200: {"description": "Détail personne + crédits."},
        404: {"description": "`person_id` introuvable."},
    },
)
def person_detail(person_id: int):
    base = run_query_one(
        f"SELECT person_id, name FROM {Q('PERSON')} WHERE person_id = %s LIMIT 1",
        (person_id,),
    )
    credits = run_query(
        f"""
        SELECT
            fc.id_content,
            fc.department,
            fc.job,
            fc.character,
            fc.order_idx,
            c.title,
            c.content_type,
            c.popularity,
            c.vote_average,
            c.vote_count,
            c.content_date
        FROM {Q('CREDIT')} fc
        JOIN {Q('CONTENT')} c USING(id_content)
        WHERE fc.person_id = %s
        ORDER BY c.popularity DESC NULLS LAST, COALESCE(fc.order_idx, 999999)
        """,
        (person_id,),
    )
    base["credits"] = credits
    return base


# =========================
# ENDPOINTS: STATS
# =========================

@app.get(
    f"/{API_VERSION}/stats/genre",
    response_model=List[Dict[str, Any]],
    tags=["stats"],
    summary="Agrégations par genre",
    description=(
        "Renvoie, par genre, les moyennes `vote_average` et `popularity`, et un volume de contenus. "
        "Tri par nom de genre. Utilise `FCT_CONTENT` ↔ `BRIDGE_CONTENT_GENRE` ↔ `DIM_GENRE`."
    ),
    responses={
        200: {
            "description": "Liste d’objets agrégés par genre.",
            "content": {
                "application/json": {
                    "example": [
                        {"genre_id": 28, "name": "Action", "vote_average": 6.8, "popularity": 45.2, "content_volume": 12034},
                        {"genre_id": 18, "name": "Drama", "vote_average": 6.9, "popularity": 37.1, "content_volume": 15402},
                    ]
                }
            },
        }
    },
)
def stats_genre(
    metric: str = Query(
        "vote_average",
        pattern="^(vote_average|popularity)$",
        description="Indicateur de référence. Conservé pour compatibilité, non utilisé dans l’ordre de tri actuel.",
    ),
    top: int = Query(20, ge=1, le=200, description="Limite haute de lignes renvoyées."),
):
    sql = f"""
        SELECT
            g.genre_id,
            MIN(g.name)                                        AS name,
            AVG(TRY_TO_DOUBLE(c.vote_average))                 AS vote_average,
            AVG(TRY_TO_DOUBLE(c.popularity))                   AS popularity,
            COUNT(*)                                           AS content_volume
        FROM {Q('CONTENT')} c
        JOIN {Q('BR_CONTENT_GENRE')} bg ON bg.id_content = c.id_content
        JOIN {Q('GENRE')} g            ON g.genre_id     = bg.genre_id
        GROUP BY g.genre_id
        HAVING COUNT(*) > 0
        ORDER BY name ASC
        LIMIT %s
    """
    return run_query(sql, (top,))
