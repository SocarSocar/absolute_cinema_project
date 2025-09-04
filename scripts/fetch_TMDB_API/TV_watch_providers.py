#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère les watch providers TV via /tv/{series_id}/watch/providers
Cardinalité: 1 série -> N lignes (pays x provider)
Projection: id_series, provider_id, provider_name, country_code
Pas de refresh basé sur une date.
"""

import sys
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# Assure l'import du module stable
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # adapte si ton module stable a un autre nom
    TMDBFetcher,
    iter_ndjson_ids,
    scan_existing_ndjson,
    append_summary_log,
    RateLimiter,
    ErrorCounter,
    ProgressTracker,
    tmdb_request,
    DATA_DIR,
)


class TVWatchProvidersFetcher(TMDBFetcher):
    """Fetcher spécifique pour /tv/{series_id}/watch/providers"""

    def __init__(self):
        super().__init__(
            input_file="tv_series_dumps.json",            # IDs source
            output_file="watch_providers_series.ndjson",  # NDJSON cible
            log_file="watch_providers_series.log",        # log synthèse
            entity_type="watch_providers_series",
            window_days=None,          # pas de refresh par date
            date_field=None,           # aucun champ date
            id_field="id",             # champ d'ID dans l'INPUT
            extra_params={},           # aucun paramètre fixe
        )
        # id_field_output distinct pour l'OUTPUT
        self.id_field_output = "id_series"

    def get_endpoint(self, entity_id: int) -> str:
        return f"/tv/{entity_id}/watch/providers"

    def _project_rows(self, series_id: int, data: dict):
        """
        Transforme la réponse API en une liste de lignes "plates".
        TMDB renvoie:
        {
          "id": <series_id>,
          "results": {
            "US": {"flatrate":[{provider_id,provider_name,...}], "buy":[...], "rent":[...], ...},
            "FR": {...},
            ...
          }
        }
        On produit une ligne par (country_code, provider_id) en dédupliquant
        un provider présent dans plusieurs catégories (flatrate/buy/rent/...).
        """
        rows = []
        results = data.get("results") or {}
        if not isinstance(results, dict):
            return rows

        for country_code, offer in results.items():
            if not isinstance(offer, dict):
                continue

            # Agrège toutes les listes potentielles de providers
            provider_objs = []
            for k in ("flatrate", "buy", "rent", "ads", "free"):
                v = offer.get(k)
                if isinstance(v, list):
                    provider_objs.extend([obj for obj in v if isinstance(obj, dict)])

            # Dédup par provider_id
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
                    self.id_field_output: series_id,
                    "provider_id": pid,
                    "provider_name": pname,
                    "country_code": country_code
                })
        return rows

    def fetch_entity_rows(self, entity_id: int):
        """Wrap de la requête + projection en lignes"""
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
        """Surcharge de run() pour gérer:
           - input_id_field = 'id' (dans tv_series_dumps.json)
           - output_id_field = 'id_series' (dans watch_providers_series.ndjson)
           - cardinalité multiple: plusieurs lignes par série
        """
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # 1) Lire les IDs d'entrée (champ 'id')
        input_ids = list(iter_ndjson_ids(self.input_path, id_field=self.id_field))
        if not input_ids:
            sys.stderr.write(f"[ERREUR] Aucun ID trouvé dans {self.input_path.name}\n")
            sys.exit(1)

        # 2) Scanner l'existant avec l'ID de sortie ('id_series')
        existing_ids, refresh_ids, kept_lines = scan_existing_ndjson(
            self.output_path,
            window_days=None,
            date_field=None,
            id_field=self.id_field_output
        )

        # 3) Déterminer les cibles: ici, pas de refresh par date
        targets = [sid for sid in input_ids if sid not in existing_ids]
        will_add = len(targets)
        will_update = 0  # pas de refresh

        total_to_process = len(targets)
        self.progress.set("total", total_to_process)
        self.progress.set("added", 0)
        self.progress.set("updated", 0)

        if total_to_process == 0:
            # Rien à faire: swap no-op pour garder atomicité/perms
            if self.output_path.exists():
                tmp = self.tmp_path
                self.output_path.replace(tmp)
                tmp.replace(self.output_path)
            date_str = __import__("time").strftime("%d/%m/%Y")
            append_summary_log(self.log_path, date_str, 0, 0, kept_lines, self.error_counter, self.entity_type)
            sys.stderr.write("\n[OK] Aucun ID à traiter. Fichier inchangé.\n")
            return

        # 4) Copier l'existant en excluant les séries qui vont être générées
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
                        # on exclut toutes les anciennes lignes de cette série (full replace)
                        continue
                    dst.write(line)
                    copied_existing += 1
        else:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            self.tmp_path.write_text("", encoding="utf-8")

        # 5) Téléchargement concurrent + écriture de N lignes par série
        added_series = 0
        ok_series = 0
        write_lock = threading.Lock()

        def worker(series_id: int):
            return self.fetch_entity_rows(series_id), series_id

        from fetch_API_TMDB import MAX_WORKERS, MAX_IN_FLIGHT  # reuse constants

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, self.tmp_path.open("a", encoding="utf-8") as out:
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
                    rows, sid = fut.result()
                    self.progress.inc("processed")

                    if rows:
                        payload = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
                        with write_lock:
                            out.write(payload)
                        ok_series += 1
                        self.progress.set("ok", ok_series)
                        added_series += 1
                        self.progress.set("added", added_series)

                    self.progress.set("errors", self.error_counter.total())
                    self.progress.print_progress()

                    try:
                        futures.add(ex.submit(worker, next(it)))
                    except StopIteration:
                        pass

        # 6) Remplacement atomique
        self.tmp_path.replace(self.output_path)

        # 7) Total final: lignes existantes conservées + toutes les lignes ajoutées
        total_lines = 0
        try:
            # recompte rapide
            with self.output_path.open("r", encoding="utf-8") as f:
                for _ in f:
                    total_lines += 1
        except Exception:
            total_lines = copied_existing  # fallback

        # 8) Log synthèse (added = nb séries ajoutées)
        date_str = __import__("time").strftime("%d/%m/%Y")
        append_summary_log(self.log_path, date_str, added_series, 0, total_lines, self.error_counter, self.entity_type)

        # 9) Fin
        sys.stderr.write(f"\n[OK] NDJSON écrit : {self.output_path} | series_added={added_series} | kept_lines={copied_existing} | total_lines={total_lines}\n")


def main():
    fetcher = TVWatchProvidersFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
