#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère la liste des certifications TV via /certification/tv/list.
Aucun input d’IDs. Un appel unique -> N lignes (pays x certifications).
Projection: country_code, certification, meaning.
Pas de refresh par date.
"""

import sys
import json
from pathlib import Path

# Import module stable
sys.path.insert(0, str(Path(__file__).parent))
from fetch_API_TMDB import (
    TMDBFetcher,
    append_summary_log,
    tmdb_request,
    DATA_DIR,
)


class TVCertificationsFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="_unused_input.json",          # non utilisé
            output_file="certification_series.ndjson",
            log_file="certification_series.log",
            entity_type="certification_series",
            window_days=None,
            date_field=None,
            id_field="id",                            # sans impact ici
            extra_params={},
        )

    def get_endpoint(self, _: int = 0) -> str:
        return "/certification/tv/list"

    def _project_rows(self, data: dict):
        """
        TMDB payload attendu:
        {
          "certifications": {
            "US": [
              {"certification":"TV-MA","meaning":"...","order":5},
              ...
            ],
            "FR": [ ... ],
            ...
          }
        }
        Sortie: une ligne par (country_code x certification).
        """
        rows = []
        certs = data.get("certifications") or {}
        if not isinstance(certs, dict):
            return rows

        for country_code, items in certs.items():
            if not isinstance(items, list):
                continue
            for obj in items:
                if not isinstance(obj, dict):
                    continue
                c = obj.get("certification")
                m = obj.get("meaning")
                if not isinstance(country_code, str) or not isinstance(c, str) or not isinstance(m, str):
                    continue
                rows.append({
                    "country_code": country_code,
                    "certification": c,
                    "meaning": m,
                })
        return rows

    def run(self):
        # 1) Appel unique
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = tmdb_request(
            self.get_endpoint(),
            self.bearer,
            self.limiter,
            self.error_counter,
            self.extra_params
        )
        rows = self._project_rows(data or {})

        # 2) Écriture atomique
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        with self.tmp_path.open("w", encoding="utf-8") as out:
            for r in rows:
                out.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.tmp_path.replace(self.output_path)

        # 3) Log synthèse
        date_str = __import__("time").strftime("%d/%m/%Y")
        total_lines = len(rows)
        append_summary_log(self.log_path, date_str, total_lines, 0, total_lines, self.error_counter, self.entity_type)

        # 4) Fin (message court)
        sys.stderr.write(f"\n[OK] NDJSON écrit : {self.output_path} | rows={total_lines}\n")


def main():
    TVCertificationsFetcher().run()


if __name__ == "__main__":
    main()
