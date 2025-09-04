#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GENRE MOVIE LIST — /genre/movie/list
Entrée:  data/out/ref_languages.ndjson (clé: iso_639_1)
Sortie:  data/out/ref_genre_movies.ndjson
Log:     logs/fetch_TMDB_API/ref_genre_movies.log
entity_type: ref_genre_movies
Champs: iso_639_1, id, name
Rebuild complet à chaque run (pas de refresh par date).
"""

import json
import sys
import threading
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


class MovieGenreListFetcher:
    def __init__(self):
        self.input_path = DATA_DIR / "ref_languages.ndjson"
        self.output_path = DATA_DIR / "ref_genre_movies.ndjson"
        self.tmp_path = DATA_DIR / "ref_genre_movies.ndjson.tmp"
        self.log_path = LOGS_DIR / "ref_genre_movies.log"
        self.entity_type = "ref_genre_movies"

        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()

        self.max_workers = MAX_WORKERS
        self.max_in_flight = MAX_IN_FLIGHT

    def _iter_languages(self):
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
                code = obj.get("iso_639_1")
                if not code or not isinstance(code, str):
                    missing += 1
                    continue
                if code not in seen:
                    seen.add(code)
                    yield code

        if parse_errors or missing:
            sys.stderr.write(
                f"[WARN] {self.input_path.name}: {parse_errors} lignes JSON invalides, {missing} sans iso_639_1 exploitable\n"
            )

    def _endpoint(self) -> str:
        return "/genre/movie/list"

    def _fetch_for_language(self, lang_code: str):
        data = tmdb_request(
            endpoint=self._endpoint(),
            bearer=self.bearer,
            limiter=self.limiter,
            error_counter=self.error_counter,
            extra_params={"language": lang_code},
        )
        out = []
        if data and isinstance(data, dict):
            genres = data.get("genres", [])
            if isinstance(genres, list):
                for g in genres:
                    if not isinstance(g, dict):
                        continue
                    gid = g.get("id")
                    name = g.get("name")
                    if isinstance(gid, int) and isinstance(name, str):
                        out.append(
                            {
                                "iso_639_1": lang_code,
                                "id": gid,
                                "name": name,
                            }
                        )
        return out

    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)

        languages = list(self._iter_languages())
        total_langs = len(languages)
        if total_langs == 0:
            sys.stderr.write(f"[ERREUR] Aucun code langue dans {self.input_path.name}\n")
            sys.exit(1)

        self.progress.set("total", total_langs)

        added_lines = 0
        ok_langs = 0
        write_lock = threading.Lock()

        def worker(lang_code: str):
            return self._fetch_for_language(lang_code), lang_code

        with self.tmp_path.open("w", encoding="utf-8") as out:
            pass

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(languages)
            futures = set()

            for _ in range(min(self.max_in_flight, total_langs)):
                try:
                    futures.add(ex.submit(worker, next(it)))
                except StopIteration:
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    rows, lang = fut.result()
                    self.progress.inc("processed")

                    if rows:
                        with write_lock:
                            for r in rows:
                                out.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
                        ok_langs += 1
                        added_lines += len(rows)
                        self.progress.set("ok", ok_langs)
                        self.progress.set("added", added_lines)

                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()

                    try:
                        futures.add(ex.submit(worker, next(it)))
                    except StopIteration:
                        pass

        self.tmp_path.replace(self.output_path)

        date_str = __import__("time").strftime("%d/%m/%Y")
        append_summary_log(
            self.log_path,
            date_str,
            added=added_lines,
            updated=0,
            total_lines=added_lines,
            error_counter=self.error_counter,
            entity_type=self.entity_type,
        )

        sys.stderr.write(
            f"\n[OK] NDJSON écrit : {self.output_path} | languages_ok={ok_langs}/{total_langs} | lines={added_lines}\n"
        )


def main():
    MovieGenreListFetcher().run()


if __name__ == "__main__":
    main()
