#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module de base réutilisable pour tous les scripts d'interrogation TMDB.
Contient toutes les fonctionnalités communes : rate limiting, retry logic,
gestion des erreurs, logging, etc.
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
from typing import Optional, Iterator, Set, Tuple, Dict, Any, List, Callable


# ========= Chemins fixes =========
ROOT = Path(__file__).resolve().parents[2]  # -> projet_absolute_cinema/
ENV_PATH = ROOT / ".env"
DATA_DIR = ROOT / "data" / "out"
LOGS_DIR = ROOT / "logs" / "fetch_TMDB_API"

# ========= Config TMDB =========
TMDB_API_HOST = "https://api.themoviedb.org/3"

# ========= Concurrence / Rate limit =========
TARGET_RPS = 50
MAX_WORKERS = 64
MAX_IN_FLIGHT = MAX_WORKERS * 4
MAX_RETRIES_PER_ID = 6
MAX_BACKOFF_SECONDS = 60.0


# ========= RateLimiter (token bucket) =========
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


# ========= Compteurs d'erreurs thread-safe =========
class ErrorCounter:
    def __init__(self):
        self._counter = Counter()
        self._lock = threading.Lock()
    
    def inc(self, key: str):
        with self._lock:
            self._counter[key] += 1
    
    def get_all(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counter)
    
    def total(self) -> int:
        with self._lock:
            return sum(self._counter.values())


# ========= Progress Tracker =========
class ProgressTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._state = {
            "processed": 0, 
            "ok": 0, 
            "total": 0, 
            "added": 0, 
            "updated": 0, 
            "errors": 0
        }
    
    def set(self, key: str, value: int):
        with self._lock:
            self._state[key] = value
    
    def inc(self, key: str):
        with self._lock:
            self._state[key] += 1
    
    def get(self, key: str) -> int:
        with self._lock:
            return self._state[key]
    
    def print_progress(self, custom_format: Optional[str] = None):
        with self._lock:
            if custom_format:
                sys.stderr.write(custom_format.format(**self._state))
            else:
                proc = self._state["processed"]
                tot = self._state["total"]
                ok = self._state["ok"]
                add = self._state["added"]
                upd = self._state["updated"]
                err = self._state["errors"]
                sys.stderr.write(f"\r[PROGRESS] {proc}/{tot} | ok={ok} | added={add} | updated={upd} | errors={err}")
            sys.stderr.flush()


# ========= Utilitaires génériques =========
def load_bearer_from_env_file(env_path: Path = ENV_PATH) -> str:
    """Charge le token TMDB depuis le fichier .env"""
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


def iter_ndjson_ids(path: Path, id_field: str = "id") -> Iterator[int]:
    """
    Itère sur les IDs uniques depuis un fichier NDJSON.
    
    Args:
        path: Chemin vers le fichier NDJSON
        id_field: Nom du champ contenant l'ID (par défaut "id")
    """
    if not path.exists():
        sys.stderr.write(f"[ERREUR] Fichier d'input introuvable: {path}\n")
        sys.exit(1)
    seen = set()
    parse_errors = 0
    invalid_lines = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            mid = obj.get(id_field)
            if isinstance(mid, int):
                if mid not in seen:
                    seen.add(mid)
                    yield mid
            else:
                invalid_lines += 1
    if parse_errors or invalid_lines:
        sys.stderr.write(f"[WARN] {path.name}: {parse_errors} lignes JSON invalides, {invalid_lines} sans {id_field} exploitable\n")


def parse_date_safe(iso_str: str) -> Optional[datetime]:
    """Parse une date ISO en gérant les erreurs"""
    if not iso_str or not isinstance(iso_str, str):
        return None
    try:
        return datetime.strptime(iso_str, "%Y-%m-%d").date()
    except Exception:
        return None


def scan_existing_ndjson(
    path: Path, 
    window_days: Optional[int] = None, 
    date_field: Optional[str] = None,
    id_field: str = "id"
) -> Tuple[Set[int], Set[int], int]:
    """
    Scanne un fichier NDJSON existant.
    
    Args:
        path: Chemin du fichier
        window_days: Fenêtre de rafraîchissement en jours (None = pas de refresh basé sur date)
        date_field: Nom du champ de date pour le refresh (ex: "release_date")
        id_field: Nom du champ ID
    
    Returns:
        (all_ids, refresh_ids, kept_lines_count)
    """
    all_ids = set()
    refresh_ids = set()
    kept_lines = 0
    
    if not path.exists():
        return all_ids, refresh_ids, kept_lines

    should_check_date = window_days is not None and date_field is not None
    if should_check_date:
        today = datetime.utcnow().date()
        cutoff = today - timedelta(days=window_days)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            mid = obj.get(id_field)
            if not isinstance(mid, int):
                continue
            all_ids.add(mid)
            kept_lines += 1
            
            if should_check_date:
                dt = parse_date_safe(obj.get(date_field))
                if dt is not None and cutoff <= dt <= today:
                    refresh_ids.add(mid)
    
    return all_ids, refresh_ids, kept_lines


def tmdb_request(
    endpoint: str,
    bearer: str,
    limiter: RateLimiter,
    error_counter: ErrorCounter,
    extra_params: Optional[Dict[str, Any]] = None,
    timeout: int = 45
) -> Optional[Dict[str, Any]]:
    """
    Requête TMDB générique avec rate limiting et retry.
    
    Args:
        endpoint: Endpoint API (ex: "/movie/123")
        bearer: Token d'authentification
        limiter: Instance RateLimiter
        error_counter: Instance ErrorCounter
        extra_params: Paramètres URL additionnels
        timeout: Timeout de la requête en secondes
    
    Returns:
        Réponse JSON ou None si erreur
    """
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/json",
        "User-Agent": "absolute-cinema/etl"
    }
    
    params = extra_params or {}
    query = urlencode(params) if params else ""
    url = f"{TMDB_API_HOST}{endpoint}" + (f"?{query}" if query else "")

    backoff = 0.2
    attempts = 0

    while True:
        attempts += 1
        limiter.acquire()
        req = Request(url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8"))
        except HTTPError as e:
            if e.code == 404:
                error_counter.inc("404")
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
                error_counter.inc("HTTP_429_exceeded_retries")
                return None
            error_counter.inc(f"HTTP_{e.code}")
            return None
        except URLError:
            if attempts < MAX_RETRIES_PER_ID:
                time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * (1.5 + random.random() * 0.5))
                continue
            error_counter.inc("URLError")
            return None
        except Exception:
            if attempts < MAX_RETRIES_PER_ID:
                time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * (1.5 + random.random() * 0.5))
                continue
            error_counter.inc("Exception")
            return None


def select_list_of_dicts(lst: Any, keys: List[str]) -> List[Dict[str, Any]]:
    """Sélectionne certains champs dans une liste de dictionnaires"""
    out = []
    if isinstance(lst, list):
        for it in lst:
            if isinstance(it, dict):
                out.append({k: it.get(k) for k in keys})
    return out


def append_summary_log(
    log_path: Path,
    date_str: str,
    added: int,
    updated: int,
    total_lines: int,
    error_counter: ErrorCounter,
    entity_type: str = "movie details"
):
    """
    Ajoute une ligne de synthèse au log.
    
    Args:
        log_path: Chemin du fichier log
        date_str: Date formatée
        added: Nombre d'entités ajoutées
        updated: Nombre d'entités mises à jour
        total_lines: Nombre total de lignes
        error_counter: Instance ErrorCounter
        entity_type: Type d'entité (ex: "movie details", "series details")
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    errors = error_counter.get_all()
    total_errors = sum(errors.values())
    if total_errors > 0:
        errors_detail = " ; ".join(f"{k}={v}" for k, v in sorted(errors.items()))
        errors_part = f"errors {total_errors} / {errors_detail}"
    else:
        errors_part = "errors 0"

    line = f"{date_str} : added {added} {entity_type} / updated {updated} {entity_type} / {errors_part} / total : {total_lines}\n"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


class TMDBFetcher:
    """
    Classe de base pour tous les fetchers TMDB.
    À hériter dans chaque script spécifique.
    """
    
    def __init__(
        self,
        input_file: str,
        output_file: str,
        log_file: str,
        entity_type: str = "entities",
        window_days: Optional[int] = None,
        date_field: Optional[str] = None,
        id_field: str = "id",
        extra_params: Optional[Dict[str, Any]] = None
    ):
        self.input_path = DATA_DIR / input_file
        self.output_path = DATA_DIR / output_file
        self.tmp_path = DATA_DIR / f"{output_file}.tmp"
        self.log_path = LOGS_DIR / log_file
        self.entity_type = entity_type
        self.window_days = window_days
        self.date_field = date_field
        self.id_field = id_field
        self.extra_params = extra_params or {}
        
        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()
    
    def get_endpoint(self, entity_id: int) -> str:
        """À override dans les classes enfants"""
        raise NotImplementedError
    
    def project_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """À override dans les classes enfants pour filtrer les champs"""
        return data
    
    def fetch_entity(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Récupère une entité depuis l'API"""
        endpoint = self.get_endpoint(entity_id)
        return tmdb_request(
            endpoint, 
            self.bearer, 
            self.limiter, 
            self.error_counter,
            self.extra_params
        )
    
    def run(self):
        """Exécution principale du fetcher"""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # 1) Lire les IDs
        ids = list(iter_ndjson_ids(self.input_path, self.id_field))
        if not ids:
            sys.stderr.write(f"[ERREUR] Aucun ID trouvé dans {self.input_path.name}\n")
            sys.exit(1)
        
        # 2) Scanner l'existant
        existing_ids, refresh_ids, kept_lines = scan_existing_ndjson(
            self.output_path, 
            self.window_days, 
            self.date_field,
            self.id_field
        )
        
        # 3) Déterminer les cibles
        targets = []
        will_add = 0
        will_update = 0
        
        # Nouveaux IDs
        for mid in ids:
            if mid not in existing_ids:
                targets.append(mid)
                will_add += 1
        
        # IDs à rafraîchir (si applicable)
        if self.window_days is not None:
            for mid in refresh_ids:
                if mid not in targets:  # éviter les doublons
                    targets.append(mid)
                    will_update += 1
        
        # Dédupliquer
        seen_target = set()
        dedup_targets = []
        for mid in targets:
            if mid not in seen_target:
                seen_target.add(mid)
                dedup_targets.append(mid)
        targets = dedup_targets
        
        total_to_process = len(targets)
        self.progress.set("total", total_to_process)
        self.progress.set("added", will_add)
        self.progress.set("updated", will_update)
        
        if total_to_process == 0:
            # Rien à faire
            if self.output_path.exists():
                self.output_path.replace(self.tmp_path)
                self.tmp_path.replace(self.output_path)
            date_str = time.strftime("%d/%m/%Y")
            append_summary_log(self.log_path, date_str, 0, 0, kept_lines, self.error_counter, self.entity_type)
            sys.stderr.write("\n[OK] Aucun ID à traiter. Fichier inchangé.\n")
            return
        
        # 4) Copier l'existant sauf les IDs à rafraîchir
        targets_set = set(targets)
        copied_existing = 0
        if self.output_path.exists():
            with self.output_path.open("r", encoding="utf-8") as src, self.tmp_path.open("w", encoding="utf-8") as dst:
                for line in src:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    mid = obj.get(self.id_field)
                    if isinstance(mid, int) and mid in targets_set:
                        continue
                    dst.write(line)
                    copied_existing += 1
        else:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            self.tmp_path.write_text("", encoding="utf-8")
        
        # 5) Téléchargement concurrent
        added = 0
        updated = 0
        ok = 0
        write_lock = threading.Lock()
        
        def worker(mid: int):
            return self.fetch_entity(mid), mid
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(targets)
            futures = set()
            
            # Amorçage
            for _ in range(min(MAX_IN_FLIGHT, total_to_process)):
                try:
                    futures.add(ex.submit(worker, next(it)))
                except StopIteration:
                    break
            
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    data, mid = fut.result()
                    self.progress.inc("processed")
                    
                    if data is not None:
                        proj = self.project_fields(data)
                        line = json.dumps(proj, ensure_ascii=False, separators=(",", ":")) + "\n"
                        with write_lock:
                            out.write(line)
                        ok += 1
                        self.progress.set("ok", ok)
                        
                        if mid in existing_ids:
                            updated += 1
                            self.progress.set("updated", updated)
                        else:
                            added += 1
                            self.progress.set("added", added)
                    
                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()
                    
                    try:
                        futures.add(ex.submit(worker, next(it)))
                    except StopIteration:
                        pass
        
        # 6) Remplacement atomique
        self.tmp_path.replace(self.output_path)
        
        # 7) Total final
        total_lines = copied_existing + ok
        
        # 8) Log synthèse
        date_str = time.strftime("%d/%m/%Y")
        append_summary_log(self.log_path, date_str, added, updated, total_lines, self.error_counter, self.entity_type)
        
        # 9) Fin
        sys.stderr.write(f"\n[OK] NDJSON écrit : {self.output_path} | added={added} | updated={updated} | kept={copied_existing} | total={total_lines}\n")