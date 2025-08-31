#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
But : récupérer les détails de films via /movie/{movie_id}, puis écrire un NDJSON
(1 film par ligne) ne contenant QUE les champs suivants, dans cet ordre :

- budget (int)
- genres (list[obj]: id, name)
- id (int)
- imdb_id (str)
- original_language (str)
- original_title (str)
- overview (str)
- popularity (float)
- production_companies (list[obj]: id, name, origin_country)
- production_countries (list[obj]: iso_3166_1, name)
- release_date (str)
- revenue (int)
- runtime (int)
- spoken_languages (list[obj]: english_name, iso_639_1, name)
- status (str)
- tagline (str)
- title (str)
- vote_average (float)
- vote_count (int)

Caractéristiques :
- NDJSON : projet_absolute_cinema/data/out/movie_details.ndjson
- Lecture des IDs depuis : projet_absolute_cinema/data/out/movie_dumps.json (NDJSON)
- Auth Bearer dans : projet_absolute_cinema/.env (clé TMDB_bearer ou TMDB_BEARER)
- Limitation de débit ~40 RPS (token bucket), concurrency threads
- Gestion 429 + backoff + retries
- Log de synthèse ajouté à logs/app.log : "DD/MM/YYYY : added X movie details, N error 404, ..."
- Aucune dépendance externe (stdlib)
"""

import json
import sys
import time
import random
import threading
from collections import Counter, deque
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# ========= Chemins fixes =========
ROOT = Path(__file__).resolve().parents[1]  # -> projet_absolute_cinema/
ENV_PATH = ROOT / ".env"
DATA_DIR = ROOT / "data" / "out"
LOGS_DIR = ROOT / "logs"
INPUT_DUMPS = DATA_DIR / "movie_dumps.json"         # NDJSON : 1 objet JSON par ligne
OUTPUT_NDJSON = DATA_DIR / "movie_details.ndjson"   # 1 film par ligne
TMP_OUTPUT = DATA_DIR / "movie_details.tmp.ndjson"  # écriture atomique
APP_LOG = LOGS_DIR / "app.log"

# ========= Config TMDB =========
TMDB_API_HOST = "https://api.themoviedb.org/3"
# Pas d'append_to_response : inutile pour les champs ciblés → charge réseau réduite
EXTRA_PARAMS = {}  # ex: {"language": "en-US"} si souhaité, laissé vide pour éviter tout filtre

# ========= Concurrence / Rate limit =========
TARGET_RPS = 50                     # objectif ~40 requêtes/seconde
MAX_WORKERS = 64                    # threads réseau concurrentiels
MAX_IN_FLIGHT = MAX_WORKERS * 4     # bornage des futures soumises
MAX_RETRIES_PER_ID = 6              # erreurs transitoires max
MAX_BACKOFF_SECONDS = 60.0

# ========= RateLimiter (token bucket basique) =========
class RateLimiter:
    def __init__(self, rate: int, per: float = 1.0):
        self.rate = rate
        self.per = per
        self._dq = deque()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._dq and (now - self._dq[0]) >= self.per:
                    self._dq.popleft()
                if len(self._dq) < self.rate:
                    self._dq.append(now)
                    return
                sleep_for = self.per - (now - self._dq[0])
            time.sleep(sleep_for if sleep_for > 0 else 0.001)

# ========= Utilitaires =========
def load_bearer_from_env_file(env_path: Path) -> str:
    if not env_path.exists():
        sys.stderr.write(f"[ERREUR] .env introuvable: {env_path}\n")
        sys.exit(1)
    bearer = None
    with env_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("\"'")
            if k in ("TMDB_bearer", "TMDB_BEARER"):
                bearer = v
                break
    if not bearer:
        sys.stderr.write("[ERREUR] TMDB_bearer manquant dans .env\n")
        sys.exit(1)
    return bearer


def iter_unique_movie_ids(dumps_path: Path):
    if not dumps_path.exists():
        sys.stderr.write(f"[ERREUR] Fichier d'input introuvable: {dumps_path}\n")
        sys.exit(1)
    seen = set()
    parse_errors = 0
    invalid_lines = 0
    with dumps_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            mid = obj.get("id", None)
            if isinstance(mid, int):
                if mid not in seen:
                    seen.add(mid)
                    yield mid
            else:
                invalid_lines += 1
    if parse_errors or invalid_lines:
        sys.stderr.write(f"[WARN] movie_dumps.json: {parse_errors} lignes JSON invalides, {invalid_lines} sans id exploitable\n")


# Compteurs d'erreurs thread-safe
_error_counter = Counter()
_err_lock = threading.Lock()
def _inc_error(key: str):
    with _err_lock:
        _error_counter[key] += 1


def tmdb_request_movie_details(movie_id: int, bearer: str, limiter: RateLimiter):
    """
    Requête concurrente avec rate limiting + retries/backoff.
    Retourne dict JSON ou None si non récupérable.
    """
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/json",
        "User-Agent": "absolute-cinema/etl"
    }
    query = urlencode(EXTRA_PARAMS) if EXTRA_PARAMS else ""
    url = f"{TMDB_API_HOST}/movie/{movie_id}" + (f"?{query}" if query else "")

    backoff = 0.2
    attempts = 0

    while True:
        attempts += 1
        limiter.acquire()  # verrou de débit global (~40 RPS)
        req = Request(url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=45) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8"))
        except HTTPError as e:
            if e.code == 404:
                _inc_error("404")
                return None
            if e.code == 401:
                sys.stderr.write("[ERREUR] 401 Unauthorized. Vérifie TMDB_bearer dans .env\n")
                sys.exit(1)
            if e.code == 429:
                retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                if retry_after is not None:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = backoff
                else:
                    wait_s = backoff
                time.sleep(min(wait_s, MAX_BACKOFF_SECONDS))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * (1.5 + random.random() * 0.5))
                if attempts < MAX_RETRIES_PER_ID:
                    continue
                _inc_error("HTTP_429_exceeded_retries")
                return None
            _inc_error(f"HTTP_{e.code}")
            return None
        except URLError:
            if attempts < MAX_RETRIES_PER_ID:
                time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * (1.5 + random.random() * 0.5))
                continue
            _inc_error("URLError")
            return None
        except Exception:
            if attempts < MAX_RETRIES_PER_ID:
                time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * (1.5 + random.random() * 0.5))
                continue
            _inc_error("Exception")
            return None


# ========= Projection (sélection stricte des champs) =========
def _select_list_of_dicts(lst, keys):
    out = []
    if isinstance(lst, list):
        for it in lst:
            if isinstance(it, dict):
                out.append({k: it.get(k) for k in keys})
    return out

def project_movie_fields(d: dict) -> dict:
    return {
        "budget": d.get("budget"),
        "genres": _select_list_of_dicts(d.get("genres"), ["id", "name"]),
        "id": d.get("id"),
        "imdb_id": d.get("imdb_id"),
        "original_language": d.get("original_language"),
        "original_title": d.get("original_title"),
        "overview": d.get("overview"),
        "popularity": d.get("popularity"),
        "production_companies": _select_list_of_dicts(d.get("production_companies"), ["id", "name", "origin_country"]),
        "production_countries": _select_list_of_dicts(d.get("production_countries"), ["iso_3166_1", "name"]),
        "release_date": d.get("release_date"),
        "revenue": d.get("revenue"),
        "runtime": d.get("runtime"),
        "spoken_languages": _select_list_of_dicts(d.get("spoken_languages"), ["english_name", "iso_639_1", "name"]),
        "status": d.get("status"),
        "tagline": d.get("tagline"),
        "title": d.get("title"),
        "vote_average": d.get("vote_average"),
        "vote_count": d.get("vote_count"),
    }


def append_summary_log(date_str: str, added: int):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    parts = [f"{date_str} : added {added} movie details"]
    for key, val in sorted(_error_counter.items()):
        if val > 0:
            parts.append(f"{val} error {key}")
    line = ", ".join(parts) + "\n"
    with APP_LOG.open("a", encoding="utf-8") as f:
        f.write(line)


def main():
    bearer = load_bearer_from_env_file(ENV_PATH)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ids = list(iter_unique_movie_ids(INPUT_DUMPS))
    total = len(ids)
    if total == 0:
        sys.stderr.write("[ERREUR] Aucun ID trouvé dans movie_dumps.json\n")
        sys.exit(1)

    limiter = RateLimiter(TARGET_RPS, per=1.0)
    added = 0
    processed = 0

    def worker(mid: int):
        return tmdb_request_movie_details(mid, bearer, limiter)

    with TMP_OUTPUT.open("w", encoding="utf-8") as out, ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        it = iter(ids)
        futures = set()

        for _ in range(min(MAX_IN_FLIGHT, total)):
            try:
                futures.add(ex.submit(worker, next(it)))
            except StopIteration:
                break

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for fut in done:
                data = fut.result()
                processed += 1
                if data is not None:
                    proj = project_movie_fields(data)
                    json.dump(proj, out, ensure_ascii=False, separators=(",", ":"))
                    out.write("\n")
                    added += 1
                if processed % 500 == 0:
                    sys.stderr.write(f"[INFO] {processed}/{total} traités | {added} ajoutés\n")
                try:
                    futures.add(ex.submit(worker, next(it)))
                except StopIteration:
                    pass

    TMP_OUTPUT.replace(OUTPUT_NDJSON)

    date_str = time.strftime("%d/%m/%Y")
    append_summary_log(date_str, added)
    sys.stderr.write(f"[OK] NDJSON écrit : {OUTPUT_NDJSON} | {added}/{total} ajoutés\n")


if __name__ == "__main__":
    main()
