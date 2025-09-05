#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TV SERIES DETAILS — /tv/{series_id}
Entrée :  data/out/tv_series_dumps.json (clé: id)
Sortie :  data/out/tv_series_details.ndjson
Log :     logs/fetch_TMDB_API/tv_series_details.log
entity_type : tv_series_details

Champs projetés (stricts, niveau série uniquement) :
- id, name, original_name, original_language, languages
- overview, tagline, type, status, in_production
- first_air_date, last_air_date
- number_of_seasons, number_of_episodes
- episode_run_time (array), origin_country (array)
- popularity, vote_average, vote_count
- genres (array<{id,name}>)
- spoken_languages (array<{english_name,iso_639_1,name}>)
- networks (array<{id,name,origin_country}>)
- production_companies (array<{id,name,origin_country}>)
- production_countries (array<{iso_3166_1,name}>)
- created_by (array<{id,name,original_name,gender,credit_id}>)
- seasons_index (array<{season_number,id}>)

Exclusions explicites : backdrop_path, poster_path, homepage, logo_path, last_episode_to_air, next_episode_to_air, images, videos.

Politique de refresh par status :
- Returning Series → refresh systématique (toujours)
- In Production    → fenêtre 30 jours (sur last_air_date)
- Pilot            → fenêtre 90 jours
- Planned          → fenêtre 90 jours
- Canceled         → fenêtre 365 jours
- Ended            → fenêtre 180 jours
- Autres/Inconnu   → fenêtre 60 jours
- Si last_air_date absente → ne pas refresh sauf Returning Series (toujours)
"""

import json
import sys
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# Import du module commun
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # noqa: E402
    DATA_DIR,
    LOGS_DIR,
    RateLimiter,
    ErrorCounter,
    ProgressTracker,
    load_bearer_from_env_file,
    tmdb_request,
    TARGET_RPS,
    MAX_WORKERS,
    MAX_IN_FLIGHT,
    append_summary_log,
)


def _parse_date_safe(s):
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


class TVSeriesDetailsFetcher:
    def __init__(self):
        self.input_path = DATA_DIR / "tv_series_dumps.json"
        self.output_path = DATA_DIR / "tv_series_details.ndjson"
        self.tmp_path = DATA_DIR / "tv_series_details.ndjson.tmp"
        self.log_path = LOGS_DIR / "tv_series_details.log"
        self.entity_type = "tv_series_details"

        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()

        self.max_workers = MAX_WORKERS
        self.max_in_flight = MAX_IN_FLIGHT

    # -------- Input IDs --------
    def _iter_ids(self):
        if not self.input_path.exists():
            sys.stderr.write(f"[ERREUR] Fichier d'input introuvable: {self.input_path}\n")
            sys.exit(1)

        seen = set()
        parse_errors = 0
        missing = 0

        with self.input_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue
                mid = obj.get("id")
                if not isinstance(mid, int):
                    missing += 1
                    continue
                if mid not in seen:
                    seen.add(mid)
                    yield mid

        if parse_errors or missing:
            sys.stderr.write(
                f"[WARN] {self.input_path.name}: {parse_errors} lignes JSON invalides, {missing} sans id exploitable\n"
            )

    # -------- Existing scan with status-based refresh policy --------
    def _scan_existing_custom_refresh(self):
        """
        Retourne:
          - existing_ids: set[int]
          - refresh_ids: set[int] décidés par la politique de status
          - kept_lines: int
        Politique:
          Returning Series -> always
          In Production    -> 30d
          Pilot            -> 90d
          Planned          -> 90d
          Canceled         -> 365d
          Ended            -> 180d
          Default          -> 60d
          last_air_date manquante -> pas de refresh, sauf Returning Series (always)
        """
        STATUS_REFRESH_POLICY = {
            "Returning Series": "always",
            "In Production": 30,
            "Pilot": 90,
            "Planned": 90,
            "Canceled": 365,
            "Ended": 180,
        }
        DEFAULT_WINDOW = 60

        existing_ids = set()
        refresh_ids = set()
        kept_lines = 0

        if not self.output_path.exists():
            return existing_ids, refresh_ids, kept_lines

        today = datetime.utcnow().date()

        def in_window(last_air_date_str: str, days: int) -> bool:
            if not last_air_date_str:
                return False
            try:
                lad = datetime.strptime(last_air_date_str, "%Y-%m-%d").date()
            except Exception:
                return False
            cutoff = today - timedelta(days=days)
            return cutoff <= lad <= today

        with self.output_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue

                sid = obj.get("id")
                if not isinstance(sid, int):
                    continue
                existing_ids.add(sid)
                kept_lines += 1

                status = obj.get("status") or ""
                lad = obj.get("last_air_date")

                policy = STATUS_REFRESH_POLICY.get(status, DEFAULT_WINDOW)

                if policy == "always":
                    refresh_ids.add(sid)
                elif isinstance(policy, int):
                    if in_window(lad, policy):
                        refresh_ids.add(sid)

        return existing_ids, refresh_ids, kept_lines

    # -------- Endpoint + projection --------
    def _endpoint(self, series_id: int) -> str:
        return f"/tv/{series_id}"

    def _project(self, d: dict) -> dict:
        def _sel_list(lst, keys):
            out = []
            if isinstance(lst, list):
                for it in lst:
                    if isinstance(it, dict):
                        out.append({k: it.get(k) for k in keys})
            return out

        created_keys = ["id", "name", "original_name", "gender", "credit_id"]

        seasons_idx = []
        seasons = d.get("seasons")
        if isinstance(seasons, list):
            for s in seasons:
                if isinstance(s, dict):
                    sn = s.get("season_number")
                    sid = s.get("id")
                    if isinstance(sn, int) and isinstance(sid, int):
                        seasons_idx.append({"season_number": sn, "id": sid})

        return {
            "id": d.get("id"),
            "name": d.get("name"),
            "original_name": d.get("original_name"),
            "original_language": d.get("original_language"),
            "languages": d.get("languages") if isinstance(d.get("languages"), list) else [],
            "overview": d.get("overview"),
            "tagline": d.get("tagline"),
            "type": d.get("type"),
            "status": d.get("status"),
            "in_production": d.get("in_production"),
            "first_air_date": d.get("first_air_date"),
            "last_air_date": d.get("last_air_date"),
            "number_of_seasons": d.get("number_of_seasons"),
            "number_of_episodes": d.get("number_of_episodes"),
            "episode_run_time": d.get("episode_run_time") if isinstance(d.get("episode_run_time"), list) else [],
            "origin_country": d.get("origin_country") if isinstance(d.get("origin_country"), list) else [],
            "popularity": d.get("popularity"),
            "vote_average": d.get("vote_average"),
            "vote_count": d.get("vote_count"),
            "genres": _sel_list(d.get("genres"), ["id", "name"]),
            "spoken_languages": _sel_list(d.get("spoken_languages"), ["english_name", "iso_639_1", "name"]),
            "networks": _sel_list(d.get("networks"), ["id", "name", "origin_country"]),
            "production_companies": _sel_list(d.get("production_companies"), ["id", "name", "origin_country"]),
            "production_countries": _sel_list(d.get("production_countries"), ["iso_3166_1", "name"]),
            "created_by": _sel_list(d.get("created_by"), created_keys),
            "seasons_index": seasons_idx,
        }

    # -------- Fetch one --------
    def _fetch_one(self, series_id: int):
        data = tmdb_request(
            endpoint=self._endpoint(series_id),
            bearer=self.bearer,
            limiter=self.limiter,
            error_counter=self.error_counter,
            extra_params=None,
        )
        return data, series_id

    # -------- Run --------
    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)

        # 1) IDs à traiter
        ids = list(self._iter_ids())
        if not ids:
            sys.stderr.write(f"[ERREUR] Aucun ID trouvé dans {self.input_path.name}\n")
            sys.exit(1)

        # 2) Scan existant + refresh custom
        existing_ids, refresh_ids, kept_lines = self._scan_existing_custom_refresh()

        # 3) Cibles = nouveaux + refresh
        targets = []
        will_add = 0
        will_update = 0

        for sid in ids:
            if sid not in existing_ids:
                targets.append(sid)
                will_add += 1

        for sid in refresh_ids:
            if sid not in targets:
                targets.append(sid)
                will_update += 1

        # Dédup
        seen = set()
        dedup_targets = []
        for sid in targets:
            if sid not in seen:
                seen.add(sid)
                dedup_targets.append(sid)
        targets = dedup_targets

        total_to_process = len(targets)
        self.progress.set("total", total_to_process)
        self.progress.set("added", will_add)
        self.progress.set("updated", will_update)

        if total_to_process == 0:
            if self.output_path.exists():
                self.output_path.replace(self.tmp_path)
                self.tmp_path.replace(self.output_path)
            date_str = time.strftime("%d/%m/%Y")
            append_summary_log(self.log_path, date_str, 0, 0, kept_lines, self.error_counter, self.entity_type)
            sys.stderr.write("\n[OK] Aucun ID à traiter. Fichier inchangé.\n")
            return

        # 4) Copier l'existant sauf ceux à refresh
        targets_set = set(targets)
        copied_existing = 0
        if self.output_path.exists():
            with self.output_path.open("r", encoding="utf-8") as src, self.tmp_path.open("w", encoding="utf-8") as dst:
                for line in src:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    sid = obj.get("id")
                    if isinstance(sid, int) and sid in targets_set:
                        continue
                    dst.write(line)
                    copied_existing += 1
        else:
            self.tmp_path.write_text("", encoding="utf-8")

        # 5) Téléchargements concurrents
        added = 0
        updated = 0
        ok = 0
        write_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(targets)
            futures = set()

            # Amorçage
            for _ in range(min(self.max_in_flight, total_to_process)):
                try:
                    futures.add(ex.submit(self._fetch_one, next(it)))
                except StopIteration:
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    data, sid = fut.result()
                    self.progress.inc("processed")

                    if data is not None:
                        proj = self._project(data)
                        out.write(json.dumps(proj, ensure_ascii=False, separators=(",", ":")) + "\n")
                        ok += 1
                        self.progress.set("ok", ok)

                        if sid in existing_ids:
                            updated += 1
                            self.progress.set("updated", updated)
                        else:
                            added += 1
                            self.progress.set("added", added)

                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()

                    try:
                        futures.add(ex.submit(self._fetch_one, next(it)))
                    except StopIteration:
                        pass

        # 6) Remplacement atomique
        self.tmp_path.replace(self.output_path)

        # 7) Total final et log
        total_lines = copied_existing + ok
        date_str = time.strftime("%d/%m/%Y")
        append_summary_log(
            self.log_path,
            date_str,
            added=added,
            updated=updated,
            total_lines=total_lines,
            error_counter=self.error_counter,
            entity_type=self.entity_type,
        )

        sys.stderr.write(
            f"\n[OK] NDJSON écrit : {self.output_path} | added={added} | updated={updated} | kept={copied_existing} | total={total_lines}\n"
        )


def main():
    TVSeriesDetailsFetcher().run()


if __name__ == "__main__":
    main()
