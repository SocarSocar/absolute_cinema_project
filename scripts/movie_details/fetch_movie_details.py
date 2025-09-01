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
- Limitation de débit ~50 RPS (token bucket), concurrency threads
- Gestion 429 + backoff + retries
- Log de synthèse ajouté à logs/app.log :
  "DD/MM/YYYY : added X movie details / updated Y movie details / errors <compte-détaillé> / total : N"
- Aucune dépendance externe (stdlib)

Ajouts :
- Incrémentalité sans index externe :
  - Ne requiert que les IDs nouveaux (absents du NDJSON) ET ceux dont la release_date est ≤ 30 jours avant la date du run.
  - Copie streaming de l’existant vers un .tmp en excluant les IDs à rafraîchir, puis append des nouvelles/MAJ → écriture atomique.
- Progression terminal : incrément 1 par 1 sur le total à traiter, via une ligne réécrite en place.
"""

import json
import sys
import time
import random
import threading
from collections import Counter, deque
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# ========= Chemins fixes =========
ROOT = Path(__file__).resolve().parents[2]  # -> projet_absolute_cinema/
ENV_PATH = ROOT / ".env"
DATA_DIR = ROOT / "data" / "out"
LOGS_DIR = ROOT / "logs"
INPUT_DUMPS = DATA_DIR / "movie_dumps.json"         # NDJSON : 1 objet JSON par ligne
OUTPUT_NDJSON = DATA_DIR / "movie_details.ndjson"   # 1 film par ligne
TMP_OUTPUT = DATA_DIR / "movie_details.tmp.ndjson"  # écriture atomique
APP_LOG = LOGS_DIR / "movie_details.log"

# ========= Config TMDB =========
TMDB_API_HOST = "https://api.themoviedb.org/3"
EXTRA_PARAMS = {}  # ex: {"language": "en-US"} si souhaité

# ========= Concurrence / Rate limit =========
TARGET_RPS = 50
MAX_WORKERS = 64
MAX_IN_FLIGHT = MAX_WORKERS * 4
MAX_RETRIES_PER_ID = 6
MAX_BACKOFF_SECONDS = 60.0

# ========= Fenêtre de rafraîchissement =========
DAYS_WINDOW = 30  # release_date ≤ now - 0 et ≥ now-30j → à rafraîchir


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


def parse_date_safe(iso_str: str):
    if not iso_str or not isinstance(iso_str, str):
        return None
    try:
        # TMDB format 'YYYY-MM-DD'
        return datetime.strptime(iso_str, "%Y-%m-%d").date()
    except Exception:
        return None


def scan_existing_details(path: Path, window_days: int):
    """
    Retourne:
      - all_ids: set de tous les IDs présents dans OUTPUT_NDJSON
      - refresh_ids: set des IDs dont release_date ∈ [today-30j ; today]
      - kept_lines_count: nombre de lignes valides lues (stat)
    """
    all_ids = set()
    refresh_ids = set()
    kept_lines = 0
    if not path.exists():
        return all_ids, refresh_ids, kept_lines

    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=window_days)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            mid = obj.get("id")
            if not isinstance(mid, int):
                continue
            all_ids.add(mid)
            kept_lines += 1
            rd = parse_date_safe(obj.get("release_date"))
            if rd is not None and cutoff <= rd <= today:
                refresh_ids.add(mid)
    return all_ids, refresh_ids, kept_lines


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
        limiter.acquire()
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


def append_summary_log(date_str: str, added: int, updated: int, total_lines: int):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    # erreurs détaillées
    total_errors = sum(_error_counter.values())
    if total_errors > 0:
        errors_detail = " ; ".join(f"{k}={v}" for k, v in sorted(_error_counter.items()))
        errors_part = f"errors {total_errors} / {errors_detail}"
    else:
        errors_part = "errors 0"

    line = f"{date_str} : added {added} movie details / updated {updated} movie details / {errors_part} / total : {total_lines}\n"
    with APP_LOG.open("a", encoding="utf-8") as f:
        f.write(line)


# ========= Affichage progressif =========
_progress_lock = threading.Lock()
_progress_state = {"processed": 0, "ok": 0, "total": 0, "added": 0, "updated": 0, "errors": 0}

def _print_progress():
    with _progress_lock:
        proc = _progress_state["processed"]
        tot = _progress_state["total"]
        ok = _progress_state["ok"]
        add = _progress_state["added"]
        upd = _progress_state["updated"]
        err = _progress_state["errors"]
        sys.stderr.write(f"\r[PROGRESS] {proc}/{tot} | ok={ok} | added={add} | updated={upd} | errors={err}")
        sys.stderr.flush()


def main():
    bearer = load_bearer_from_env_file(ENV_PATH)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Lire les IDs dumps
    ids = list(iter_unique_movie_ids(INPUT_DUMPS))
    if not ids:
        sys.stderr.write("[ERREUR] Aucun ID trouvé dans movie_dumps.json\n")
        sys.exit(1)

    # 2) Scanner l'existant pour décider de l'incrémental sans index externe
    existing_ids, refresh_ids, kept_lines = scan_existing_details(OUTPUT_NDJSON, DAYS_WINDOW)

    # 3) Déterminer la cible : nouveaux OU à rafraîchir (release ≤ 30 jours)
    targets = []
    will_add = 0
    will_update = 0
    existing_ids_set = existing_ids  # alias

    # Ajout : tous les IDs absents de l'existant
    for mid in ids:
        if mid not in existing_ids_set:
            targets.append(mid)
            will_add += 1

    # Update : tous les IDs marqués à rafraîchir
    for mid in (refresh_ids):
        targets.append(mid)
        will_update += 1

    # Dédupliquer en préservant l’ordre d’origine (priorité aux "add" qui sont déjà dans l’ordre des dumps)
    seen_target = set()
    dedup_targets = []
    for mid in targets:
        if mid not in seen_target:
            seen_target.add(mid)
            dedup_targets.append(mid)
    targets = dedup_targets

    total_to_process = len(targets)
    _progress_state["total"] = total_to_process
    _progress_state["added"] = will_add
    _progress_state["updated"] = will_update

    if total_to_process == 0:
        # Rien à faire : recopier tel quel vers tmp pour garantir atomicité si besoin
        if OUTPUT_NDJSON.exists():
            OUTPUT_NDJSON.replace(TMP_OUTPUT)
            TMP_OUTPUT.replace(OUTPUT_NDJSON)
        date_str = time.strftime("%d/%m/%Y")
        append_summary_log(date_str, 0, 0, kept_lines)
        sys.stderr.write("\n[OK] Aucun ID à traiter. Fichier inchangé.\n")
        return

    limiter = RateLimiter(TARGET_RPS, per=1.0)

    # 4) Préparer écriture : on recopie l'existant SAUF les IDs à rafraîchir
    #    → évite de charger l'existant en mémoire, et garantit que les MAJ écraseront proprement
    targets_set = set(targets)
    copied_existing = 0
    if OUTPUT_NDJSON.exists():
        with OUTPUT_NDJSON.open("r", encoding="utf-8") as src, TMP_OUTPUT.open("w", encoding="utf-8") as dst:
            for line in src:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                mid = obj.get("id")
                if isinstance(mid, int) and mid in targets_set:
                    # cet ID sera réécrit par la nouvelle version → skip la copie
                    continue
                # sinon, garder la ligne telle quelle
                dst.write(line)
                copied_existing += 1
    else:
        # créer le tmp vide si pas d'existant
        TMP_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        TMP_OUTPUT.write_text("", encoding="utf-8")

    # 5) Téléchargement concurrent des cibles + écriture append
    added = 0
    updated = 0
    ok = 0

    write_lock = threading.Lock()
    progress_err_count = 0

    def worker(mid: int):
        return tmdb_request_movie_details(mid, bearer, limiter), mid

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, TMP_OUTPUT.open("a", encoding="utf-8") as out:
        it = iter(targets)
        futures = set()

        # amorçage
        for _ in range(min(MAX_IN_FLIGHT, total_to_process)):
            try:
                futures.add(ex.submit(worker, next(it)))
            except StopIteration:
                break

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for fut in done:
                data, mid = fut.result()
                with _progress_lock:
                    _progress_state["processed"] += 1

                if data is not None:
                    proj = project_movie_fields(data)
                    line = json.dumps(proj, ensure_ascii=False, separators=(",", ":")) + "\n"
                    with write_lock:
                        out.write(line)
                    ok += 1
                    if mid in existing_ids_set:
                        updated += 1
                    else:
                        added += 1
                else:
                    with _err_lock:
                        progress_err_count = sum(_error_counter.values())
                    with _progress_lock:
                        _progress_state["errors"] = progress_err_count

                with _progress_lock:
                    _progress_state["ok"] = ok
                _print_progress()

                try:
                    futures.add(ex.submit(worker, next(it)))
                except StopIteration:
                    pass

    # 6) Remplacement atomique
    TMP_OUTPUT.replace(OUTPUT_NDJSON)

    # 7) Total final = lignes recopiées + ok écrits
    total_lines = copied_existing + ok

    # 8) Log synthèse
    date_str = time.strftime("%d/%m/%Y")
    append_summary_log(date_str, added, updated, total_lines)

    # 9) Fin
    sys.stderr.write(f"\n[OK] NDJSON écrit : {OUTPUT_NDJSON} | added={added} | updated={updated} | kept={copied_existing} | total={total_lines}\n")


if __name__ == "__main__":
    main()
