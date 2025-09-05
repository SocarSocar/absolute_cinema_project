#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TV SEASON DETAILS — /tv/{series_id}/season/{season_number}

Entrée :
  - data/out/tv_series_details.ndjson
    * Clés d’ID : (series_id=id) + seasons_index[].season_number

Sortie :
  - data/out/tv_seasons_details.ndjson
  - logs/fetch_TMDB_API/tv_seasons_details.log
  - entity_type : tv_seasons_details

Projection (exclusivement niveau saison ; aucun détail épisode écrit, pas d’images/liens) :
  - season_id (ex-id)
  - series_id
  - season_number
  - name
  - overview
  - air_date
  - vote_average
  - episode_count   # dérivé: len(episodes) si présent
  - _id             # identifiant interne TMDB de la saison

Politique de refresh :
  - Basée sur air_date
  - Fenêtre 60 jours
  - Si saison ancienne/close (heuristique: air_date < today - 365j) → fenêtre 180 jours

Particularités :
  - Pas de pagination
  - Dépend de tv_series_details.ndjson pour (series_id, season_number)
  - Cardinalité : 1 objet par (series_id, season_number)
"""

import json
import sys
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# Import module commun
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


class TVSeasonDetailsFetcher:
    def __init__(self):
        # IO
        self.series_details_path = DATA_DIR / "tv_series_details.ndjson"
        self.output_path = DATA_DIR / "tv_seasons_details.ndjson"
        self.tmp_path = DATA_DIR / "tv_seasons_details.ndjson.tmp"
        self.log_path = LOGS_DIR / "tv_seasons_details.log"
        self.entity_type = "tv_seasons_details"

        # Infra
        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()

        # Concurrence
        self.max_workers = MAX_WORKERS
        self.max_in_flight = MAX_IN_FLIGHT

    # -------- Lecture des couples (series_id, season_number) --------
    def _iter_series_seasons(self):
        """
        Source: tv_series_details.ndjson
        Utilise uniquement seasons_index: [{season_number, id}]
        Émet des tuples (series_id, season_number)
        """
        if not self.series_details_path.exists():
            sys.stderr.write(f"[ERREUR] Fichier introuvable: {self.series_details_path}\n")
            sys.exit(1)

        seen = set()
        parse_errors = 0
        missing = 0

        with self.series_details_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                sid = obj.get("id")
                seasons_index = obj.get("seasons_index")
                if not isinstance(sid, int) or not isinstance(seasons_index, list):
                    missing += 1
                    continue

                for s in seasons_index:
                    if not isinstance(s, dict):
                        continue
                    sn = s.get("season_number")
                    if isinstance(sn, int):
                        key = (sid, sn)
                        if key not in seen:
                            seen.add(key)
                            yield sid, sn

        if parse_errors or missing:
            sys.stderr.write(
                f"[WARN] {self.series_details_path.name}: {parse_errors} lignes JSON invalides, {missing} sans seasons_index exploitable\n"
            )

    # -------- Scan existant + refresh custom --------
    def _scan_existing_custom_refresh(self):
        """
        Fichier: tv_seasons_details.ndjson
        Décide le refresh via air_date:
          - air_date ∈ [today-60, today]           -> refresh
          - OU si air_date < today-365, fenêtre 180 -> air_date ∈ [today-180, today] -> refresh
        Retourne:
          existing_keys: set[(series_id, season_number)]
          refresh_keys : set[(series_id, season_number)]
          kept_lines   : int
        """
        existing_keys = set()
        refresh_keys = set()
        kept_lines = 0
        path = self.output_path
        if not path.exists():
            return existing_keys, refresh_keys, kept_lines

        today = datetime.utcnow().date()
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue

                sid = obj.get("series_id")
                sn = obj.get("season_number")
                if not isinstance(sid, int) or not isinstance(sn, int):
                    continue
                key = (sid, sn)
                existing_keys.add(key)
                kept_lines += 1

                air_date = _parse_date_safe(obj.get("air_date"))
                if air_date is None:
                    continue

                # Heuristique "ancienne/close": air_date < today - 365
                old_season = air_date < (today - timedelta(days=365))
                window = 180 if old_season else 60
                cutoff = today - timedelta(days=window)
                if cutoff <= air_date <= today:
                    refresh_keys.add(key)

        return existing_keys, refresh_keys, kept_lines

    # -------- Endpoint + projection --------
    def _endpoint(self, series_id: int, season_number: int) -> str:
        return f"/tv/{series_id}/season/{season_number}"

    def _project(self, d: dict, series_id: int) -> dict:
        # episode_count dérivé de la réponse (si episodes list fournie)
        episodes = d.get("episodes")
        episode_count = len(episodes) if isinstance(episodes, list) else d.get("episode_count")

        return {
            "season_id": d.get("id"),
            "series_id": series_id,
            "season_number": d.get("season_number"),
            "name": d.get("name"),
            "overview": d.get("overview"),
            "air_date": d.get("air_date"),
            "vote_average": d.get("vote_average"),
            "episode_count": episode_count,
            "_id": d.get("_id"),
        }

    # -------- Fetch --------
    def _fetch_one(self, series_id: int, season_number: int):
        data = tmdb_request(
            endpoint=self._endpoint(series_id, season_number),
            bearer=self.bearer,
            limiter=self.limiter,
            error_counter=self.error_counter,
            extra_params=None,
        )
        return data, (series_id, season_number)

    # -------- Run --------
    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)

        # 1) Couples à traiter
        pairs = list(self._iter_series_seasons())
        if not pairs:
            sys.stderr.write(f"[ERREUR] Aucun couple (series_id, season_number) trouvé dans {self.series_details_path.name}\n")
            sys.exit(1)

        # 2) Scan existant + refresh custom
        existing_keys, refresh_keys, kept_lines = self._scan_existing_custom_refresh()

        # 3) Cibles = nouveaux + refresh
        targets = []
        will_add = 0
        will_update = 0

        for key in pairs:
            if key not in existing_keys:
                targets.append(key)
                will_add += 1

        for key in refresh_keys:
            if key not in targets:
                targets.append(key)
                will_update += 1

        # Dédup
        seen = set()
        dedup_targets = []
        for key in targets:
            if key not in seen:
                seen.add(key)
                dedup_targets.append(key)
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
                    sid = obj.get("series_id")
                    sn = obj.get("season_number")
                    key = (sid, sn) if isinstance(sid, int) and isinstance(sn, int) else None
                    if key and key in targets_set:
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

        def worker(series_id: int, season_number: int):
            return self._fetch_one(series_id, season_number)

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(targets)
            futures = set()

            # Amorçage
            for _ in range(min(self.max_in_flight, total_to_process)):
                try:
                    s_id, s_num = next(it)
                    futures.add(ex.submit(worker, s_id, s_num))
                except StopIteration:
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    data, key = fut.result()
                    series_id, season_number = key
                    self.progress.inc("processed")

                    if data is not None:
                        proj = self._project(data, series_id)
                        with write_lock:
                            out.write(json.dumps(proj, ensure_ascii=False, separators=(",", ":")) + "\n")
                        ok += 1
                        self.progress.set("ok", ok)

                        if key in existing_keys:
                            updated += 1
                            self.progress.set("updated", updated)
                        else:
                            added += 1
                            self.progress.set("added", added)

                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()

                    try:
                        s_id, s_num = next(it)
                        futures.add(ex.submit(worker, s_id, s_num))
                    except StopIteration:
                        pass

        # 6) Remplacement atomique
        self.tmp_path.replace(self.output_path)

        # 7) Log final
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
    TVSeasonDetailsFetcher().run()


if __name__ == "__main__":
    main()
