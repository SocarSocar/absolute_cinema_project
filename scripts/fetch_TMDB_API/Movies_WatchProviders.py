#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les watch providers MOVIES via /movie/{movie_id}/watch/providers.
Cardinalité: 1 film -> N lignes (country_code x provider).
Projection: id_movie, provider_id, provider_name, country_code.
Pas de refresh basé sur une date.
"""

import sys
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# Import module stable
sys.path.insert(0, str(Path(__file__).parent))
from fetch_API_TMDB import (
    TMDBFetcher,
    iter_ndjson_ids,
    scan_existing_ndjson,
    append_summary_log,
    tmdb_request,
    DATA_DIR,
    MAX_WORKERS,
    MAX_IN_FLIGHT,
)

class MovieWatchProvidersFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="movie_dumps.json",
            output_file="watch_providers_movies.ndjson",
            log_file="watch_providers_movies.log",
            entity_type="watch_providers_movies",
            window_days=None,
            date_field=None,
            id_field="id",
            extra_params={},
        )
        self.id_field_output = "id_movie"

    def get_endpoint(self, entity_id: int) -> str:
        return f"/movie/{entity_id}/watch/providers"

    def _project_rows(self, movie_id: int, data: dict):
        """
        TMDB payload:
        {
          "id": <movie_id>,
          "results": {
            "US": {"flatrate":[{provider_id,provider_name,...}], "buy":[...], "rent":[...], "ads":[...], "free":[...]},
            "FR": {...},
            ...
          }
        }
        Une ligne par (country_code, provider_id), dédupliquée entre catégories.
        """
        rows = []
        results = data.get("results") or {}
        if not isinstance(results, dict):
            return rows

        for country_code, offer in results.items():
            if not isinstance(offer, dict):
                continue
            provider_objs = []
            for k in ("flatrate", "buy", "rent", "ads", "free"):
                v = offer.get(k)
                if isinstance(v, list):
                    provider_objs.extend([obj for obj in v if isinstance(obj, dict)])

            seen = set()
            for obj in provider_objs:
                pid = obj.get("provider_id")
                pname = obj.get("provider_name")
                if not isinstance(pid, int) or not isinstance(pname, str):
                    continue
                key = (country_code, pid)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    self.id_field_output: movie_id,
                    "provider_id": pid,
                    "provider_name": pname,
                    "country_code": country_code
                })
        return rows

    def fetch_entity_rows(self, entity_id: int):
        data = tmdb_request(
            self.get_endpoint(entity_id),
            self.bearer,
            self.limiter,
            self.error_counter,
            self.extra_params
        )
        if data is None:
            return []
        return self._project_rows(entity_id, data)

    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # 1) IDs d'entrée (champ 'id' dans movie_dumps.json)
        input_ids = list(iter_ndjson_ids(self.input_path, id_field=self.id_field))
        if not input_ids:
            sys.stderr.write(f"[ERREUR] Aucun ID trouvé dans {self.input_path.name}\n")
            sys.exit(1)

        # 2) Scanner l'existant avec l'ID de sortie ('id_movie')
        existing_ids, _, kept_lines = scan_existing_ndjson(
            self.output_path,
            window_days=None,
            date_field=None,
            id_field=self.id_field_output
        )

        # 3) Cibles: uniquement nouveaux films (pas de refresh)
        targets = [mid for mid in input_ids if mid not in existing_ids]
        total_to_process = len(targets)
        self.progress.set("total", total_to_process)
        self.progress.set("added", 0)
        self.progress.set("updated", 0)

        if total_to_process == 0:
            if self.output_path.exists():
                tmp = self.tmp_path
                self.output_path.replace(tmp)
                tmp.replace(self.output_path)
            date_str = __import__("time").strftime("%d/%m/%Y")
            append_summary_log(self.log_path, date_str, 0, 0, kept_lines, self.error_counter, self.entity_type)
            sys.stderr.write("\n[OK] Aucun ID à traiter. Fichier inchangé.\n")
            return

        # 4) Copier l'existant en excluant films ciblés (full replace par film)
        targets_set = set(targets)
        copied_existing = 0
        if self.output_path.exists():
            with self.output_path.open("r", encoding="utf-8") as src, self.tmp_path.open("w", encoding="utf-8") as dst:
                for line in src:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    out_id = obj.get(self.id_field_output)
                    if isinstance(out_id, int) and out_id in targets_set:
                        continue
                    dst.write(line)
                    copied_existing += 1
        else:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            self.tmp_path.write_text("", encoding="utf-8")

        # 5) Téléchargement concurrent et écriture N lignes/film
        added_movies = 0
        ok_movies = 0
        write_lock = threading.Lock()

        def worker(movie_id: int):
            return self.fetch_entity_rows(movie_id), movie_id

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(targets)
            futures = set()

            for _ in range(min(MAX_IN_FLIGHT, total_to_process)):
                try:
                    futures.add(ex.submit(worker, next(it)))
                except StopIteration:
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    rows, mid = fut.result()
                    self.progress.inc("processed")

                    if rows:
                        payload = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
                        with write_lock:
                            out.write(payload)
                        ok_movies += 1
                        self.progress.set("ok", ok_movies)
                        added_movies += 1
                        self.progress.set("added", added_movies)

                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()

                    try:
                        futures.add(ex.submit(worker, next(it)))
                    except StopIteration:
                        pass

        # 6) Swap atomique
        self.tmp_path.replace(self.output_path)

        # 7) Recompte des lignes en sortie
        total_lines = 0
        try:
            with self.output_path.open("r", encoding="utf-8") as f:
                for _ in f:
                    total_lines += 1
        except Exception:
            total_lines = copied_existing

        # 8) Log synthèse
        date_str = __import__("time").strftime("%d/%m/%Y")
        append_summary_log(self.log_path, date_str, added_movies, 0, total_lines, self.error_counter, self.entity_type)

        # 9) Fin
        sys.stderr.write(f"\n[OK] NDJSON écrit : {self.output_path} | movies_added={added_movies} | kept_lines={copied_existing} | total_lines={total_lines}\n")


def main():
    fetcher = MovieWatchProvidersFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
