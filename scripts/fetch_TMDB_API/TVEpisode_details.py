#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TV EPISODE DETAILS — /tv/{series_id}/season/{season_number}/episode/{episode_number}

Entrée :
  - data/out/tv_seasons_details.ndjson
    * Clés d’ID : (series_id) + (season_number)

Logique de dérivation :
  - Pour chaque (series_id, season_number), on interroge à la volée /tv/{series_id}/season/{season_number}
    pour obtenir la liste des episodes (episode_number + air_date) EN MÉMOIRE SEULEMENT.
  - Puis on interroge /tv/{series_id}/season/{season_number}/episode/{episode_number} pour CHAQUE épisode cible.
  - Aucun index intermédiaire écrit.

Sortie :
  - data/out/tv_episodes_details.ndjson
  - logs/fetch_TMDB_API/tv_episodes_details.log
  - entity_type : tv_episodes_details

Projection (niveau épisode uniquement ; pas d’images/liens) :
  - episode_id (ex-id)
  - series_id
  - season_number
  - episode_number
  - episode_type
  - name
  - overview
  - air_date
  - runtime
  - production_code
  - vote_average
  - vote_count
  - crew         : array<{job,department,credit_id,id,name,original_name,gender}>
  - guest_stars  : array<{character,credit_id,order,id,name,original_name,gender}>

Politique de refresh :
  - Basée sur air_date de l’épisode (prélevée depuis la réponse de l’ENDPOINT SAISON)
  - Fenêtre 60 jours
  - Si épisode “ancien” (heuristique: air_date < today - 365 jours) → fenêtre 180 jours
  - Un épisode “nouveau” (absent du NDJSON existant) est toujours ciblé, même sans air_date.

Particularités :
  - Pas de pagination
  - Dépend de tv_seasons_details.ndjson → (series_id, season_number)
  - Cardinalité : 1 objet par (series_id, season_number, episode_number)
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


class TVEpisodeDetailsFetcher:
    def __init__(self):
        # IO
        self.seasons_path = DATA_DIR / "tv_seasons_details.ndjson"
        self.output_path = DATA_DIR / "tv_episodes_details.ndjson"
        self.tmp_path = DATA_DIR / "tv_episodes_details.ndjson.tmp"
        self.log_path = LOGS_DIR / "tv_episodes_details.log"
        self.entity_type = "tv_episodes_details"

        # Infra
        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()

        # Concurrence
        self.max_workers = MAX_WORKERS
        self.max_in_flight = MAX_IN_FLIGHT

    # ---------- Lecture des couples (series_id, season_number) ----------
    def _iter_series_seasons(self):
        """
        Source: tv_seasons_details.ndjson
        Émet des tuples (series_id, season_number)
        """
        if not self.seasons_path.exists():
            sys.stderr.write(f"[ERREUR] Fichier introuvable: {self.seasons_path}\n")
            sys.exit(1)

        seen = set()
        parse_errors = 0
        missing = 0

        with self.seasons_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                sid = obj.get("series_id")
                sn = obj.get("season_number")
                if not isinstance(sid, int) or not isinstance(sn, int):
                    missing += 1
                    continue

                key = (sid, sn)
                if key not in seen:
                    seen.add(key)
                    yield sid, sn

        if parse_errors or missing:
            sys.stderr.write(
                f"[WARN] {self.seasons_path.name}: {parse_errors} lignes JSON invalides, {missing} sans (series_id, season_number) exploitables\n"
            )

    # ---------- Scan existant pour copie/refresh ----------
    def _scan_existing(self):
        """
        Fichier: tv_episodes_details.ndjson
        Retourne:
          - existing_keys: set[(series_id, season_number, episode_number)]
          - kept_lines: int
        """
        existing_keys = set()
        kept_lines = 0
        path = self.output_path
        if not path.exists():
            return existing_keys, kept_lines

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                sid = obj.get("series_id")
                sn = obj.get("season_number")
                en = obj.get("episode_number")
                if isinstance(sid, int) and isinstance(sn, int) and isinstance(en, int):
                    existing_keys.add((sid, sn, en))
                    kept_lines += 1

        return existing_keys, kept_lines

    # ---------- Endpoints ----------
    def _endpoint_season(self, series_id: int, season_number: int) -> str:
        return f"/tv/{series_id}/season/{season_number}"

    def _endpoint_episode(self, series_id: int, season_number: int, episode_number: int) -> str:
        return f"/tv/{series_id}/season/{season_number}/episode/{episode_number}"

    # ---------- Sélection champs ----------
    @staticmethod
    def _select_list(lst, keys):
        out = []
        if isinstance(lst, list):
            for it in lst:
                if isinstance(it, dict):
                    out.append({k: it.get(k) for k in keys})
        return out

    def _project_episode(self, d: dict, series_id: int, season_number: int) -> dict:
        crew_keys = ["job", "department", "credit_id", "id", "name", "original_name", "gender"]
        guest_keys = ["character", "credit_id", "order", "id", "name", "original_name", "gender"]

        return {
            "episode_id": d.get("id"),
            "series_id": series_id,
            "season_number": d.get("season_number"),
            "episode_number": d.get("episode_number"),
            "episode_type": d.get("episode_type"),
            "name": d.get("name"),
            "overview": d.get("overview"),
            "air_date": d.get("air_date"),
            "runtime": d.get("runtime"),
            "production_code": d.get("production_code"),
            "vote_average": d.get("vote_average"),
            "vote_count": d.get("vote_count"),
            "crew": self._select_list(d.get("crew"), crew_keys),
            "guest_stars": self._select_list(d.get("guest_stars"), guest_keys),
        }

    # ---------- Fetch ----------
    def _fetch_season_listing(self, series_id: int, season_number: int):
        """
        Retourne la liste [(episode_number, air_date_str), ...]
        issue de l'endpoint SAISON.
        """
        data = tmdb_request(
            endpoint=self._endpoint_season(series_id, season_number),
            bearer=self.bearer,
            limiter=self.limiter,
            error_counter=self.error_counter,
            extra_params=None,
        )
        out = []
        if data and isinstance(data, dict):
            episodes = data.get("episodes")
            if isinstance(episodes, list):
                for ep in episodes:
                    if not isinstance(ep, dict):
                        continue
                    en = ep.get("episode_number")
                    ad = ep.get("air_date")
                    if isinstance(en, int):
                        out.append((en, ad if isinstance(ad, str) else None))
        return out

    def _fetch_episode_details(self, series_id: int, season_number: int, episode_number: int):
        data = tmdb_request(
            endpoint=self._endpoint_episode(series_id, season_number, episode_number),
            bearer=self.bearer,
            limiter=self.limiter,
            error_counter=self.error_counter,
            extra_params=None,
        )
        return data, (series_id, season_number, episode_number)

    # ---------- Décision refresh par fenêtre ----------
    @staticmethod
    def _episode_in_refresh_window(air_date_str: str) -> bool:
        """
        True si l'episode (par son air_date) doit être rafraîchi.
        - fenêtre 60 jours
        - si air_date < today-365 → fenêtre 180 jours
        - si air_date manquante → False (sauf si nouvel épisode, géré ailleurs)
        """
        dt = _parse_date_safe(air_date_str)
        if dt is None:
            return False
        today = datetime.utcnow().date()
        old = dt < (today - timedelta(days=365))
        window = 180 if old else 60
        cutoff = today - timedelta(days=window)
        return cutoff <= dt <= today

    # ---------- Run ----------
    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)

        # 1) Couples (series_id, season_number)
        pairs = list(self._iter_series_seasons())
        if not pairs:
            sys.stderr.write(f"[ERREUR] Aucun couple (series_id, season_number) trouvé dans {self.seasons_path.name}\n")
            sys.exit(1)

        # 2) Scan existant
        existing_keys, kept_lines = self._scan_existing()

        # 3) Découverte des cibles via listings SAISON (en mémoire)
        #    - nouveaux épisodes (absents d'existing_keys) → target
        #    - épisodes dans fenêtre de refresh → target
        targets = []  # liste de triplets (sid, sn, en)
        will_add = 0
        will_update = 0

        # Pour éviter doublons si mêmes (sid,sn) en input répétés
        seen_targets = set()

        for sid, sn in pairs:
            # Listing des épisodes de la saison
            listing = self._fetch_season_listing(sid, sn)
            for en, air_date in listing:
                key = (sid, sn, en)
                # Décision nouveau vs refresh
                if key not in existing_keys:
                    if key not in seen_targets:
                        targets.append(key)
                        seen_targets.add(key)
                        will_add += 1
                else:
                    if self._episode_in_refresh_window(air_date):
                        if key not in seen_targets:
                            targets.append(key)
                            seen_targets.add(key)
                            will_update += 1

        total_to_process = len(targets)
        self.progress.set("total", total_to_process)
        self.progress.set("added", will_add)
        self.progress.set("updated", will_update)

        # 4) Si rien à faire, no-op + log
        if total_to_process == 0:
            if self.output_path.exists():
                self.output_path.replace(self.tmp_path)
                self.tmp_path.replace(self.output_path)
            date_str = time.strftime("%d/%m/%Y")
            append_summary_log(self.log_path, date_str, 0, 0, kept_lines, self.error_counter, self.entity_type)
            sys.stderr.write("\n[OK] Aucun épisode à traiter. Fichier inchangé.\n")
            return

        # 5) Copier l'existant SAUF les triplets à rafraîchir/réécrire
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
                    en = obj.get("episode_number")
                    key = (sid, sn, en) if isinstance(sid, int) and isinstance(sn, int) and isinstance(en, int) else None
                    if key and key in targets_set:
                        continue
                    dst.write(line)
                    copied_existing += 1
        else:
            self.tmp_path.write_text("", encoding="utf-8")

        # 6) Téléchargements concurrents des DÉTAILS ÉPISODE
        added = 0
        updated = 0
        ok = 0
        write_lock = threading.Lock()

        def worker(sid: int, sn: int, en: int):
            return self._fetch_episode_details(sid, sn, en)

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(targets)
            futures = set()

            # Amorçage
            for _ in range(min(self.max_in_flight, total_to_process)):
                try:
                    s_id, s_num, e_num = next(it)
                    futures.add(ex.submit(worker, s_id, s_num, e_num))
                except StopIteration:
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    data, key = fut.result()
                    sid, sn, en = key
                    self.progress.inc("processed")

                    if data is not None:
                        proj = self._project_episode(data, sid, sn)
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
                        s_id, s_num, e_num = next(it)
                        futures.add(ex.submit(worker, s_id, s_num, e_num))
                    except StopIteration:
                        pass

        # 7) Remplacement atomique
        self.tmp_path.replace(self.output_path)

        # 8) Log final
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
    TVEpisodeDetailsFetcher().run()


if __name__ == "__main__":
    main()
