#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GENRE TV LIST — /genre/tv/list
Objectif: construire un référentiel des genres TV par langue.

Spécifications imposées
-----------------------
1) Endpoint exact:
   - URL: https://api.themoviedb.org/3/genre/tv/list?language={iso_639_1}
   - ID attendu: aucun (on itère sur les langues)
   - extra_params fixes: none (paramètre language injecté dynamiquement)

2) Input attendu:
   - Fichier: ref_languages.ndjson (dans data/out)
   - Clé d'ID: iso_639_1

3) Output attendu:
   - NDJSON: ref_genre_series.ndjson (dans data/out)
   - Log:    ref_genre_series.log (dans logs/fetch_TMDB_API)
   - entity_type: "ref_genre_series"

4) Champs projetés (exhaustif, 1 par ligne):
   - iso_639_1
   - id
   - name

5) Politique de refresh:
   - Pas basée sur une date -> on reconstruit entièrement le fichier à chaque run.

6) Particularités:
   - Pas de pagination
   - Pas de dépendances supplémentaires
   - Cardinalité: N objets/ langue (une ligne par (langue, genre))
"""

import json
import sys
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# Path import pour accéder au module commun
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # noqa: E402
    TMDB_API_HOST,
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


class TVGenreListFetcher:
    """
    Fetcher spécialisé (multi-lignes par entrée).
    On ne s'appuie pas sur TMDBFetcher.run car on doit:
      - Itérer sur des codes langue (str, pas int)
      - Éclater la réponse (liste de genres) en N lignes
      - Reconstruire le NDJSON en entier à chaque run
    """

    def __init__(self):
        # Entrées / sorties
        self.input_path = DATA_DIR / "ref_languages.ndjson"
        self.output_path = DATA_DIR / "ref_genre_series.ndjson"
        self.tmp_path = DATA_DIR / "ref_genre_series.ndjson.tmp"
        self.log_path = LOGS_DIR / "ref_genre_series.log"
        self.entity_type = "ref_genre_series"

        # Infra commune
        self.bearer = load_bearer_from_env_file()
        self.limiter = RateLimiter(TARGET_RPS, per=1.0)
        self.error_counter = ErrorCounter()
        self.progress = ProgressTracker()

        # Concurrence
        self.max_workers = MAX_WORKERS
        self.max_in_flight = MAX_IN_FLIGHT

    # -------- Input --------
    def _iter_languages(self):
        """
        Lit data/out/ref_languages.ndjson et itère les codes iso_639_1 uniques.
        """
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

    # -------- Fetch --------
    def _endpoint(self) -> str:
        return "/genre/tv/list"

    def _fetch_for_language(self, lang_code: str):
        """
        Appelle /genre/tv/list?language={lang_code}
        Retourne une liste de dicts déjà projetés:
        [{"iso_639_1": lang_code, "id": <int>, "name": <str>}, ...]
        """
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

    # -------- Run --------
    def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)

        # 1) Charges langues
        languages = list(self._iter_languages())
        total_langs = len(languages)
        if total_langs == 0:
            sys.stderr.write(f"[ERREUR] Aucun code langue dans {self.input_path.name}\n")
            sys.exit(1)

        self.progress.set("total", total_langs)

        # 2) Construire fichier depuis zéro (pas de refresh partiel)
        added_lines = 0
        ok_langs = 0
        write_lock = threading.Lock()

        def worker(lang_code: str):
            return self._fetch_for_language(lang_code), lang_code

        # Vide le tmp
        with self.tmp_path.open("w", encoding="utf-8") as out:
            pass

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
            it = iter(languages)
            futures = set()

            # Amorçage
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
                        # Écrit toutes les lignes (une par genre)
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

        # 3) Remplacement atomique
        self.tmp_path.replace(self.output_path)

        # 4) Log synthèse
        date_str = __import__("time").strftime("%d/%m/%Y")
        append_summary_log(
            self.log_path,
            date_str,
            added=added_lines,
            updated=0,               # reconstruction complète
            total_lines=added_lines, # total = lignes écrites
            error_counter=self.error_counter,
            entity_type=self.entity_type,
        )

        sys.stderr.write(
            f"\n[OK] NDJSON écrit : {self.output_path} | languages_ok={ok_langs}/{total_langs} | lines={added_lines}\n"
        )


def main():
    TVGenreListFetcher().run()


if __name__ == "__main__":
    main()
