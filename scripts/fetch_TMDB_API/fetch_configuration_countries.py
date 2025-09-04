#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère la configuration des pays via /configuration/countries.
Aucun input d’IDs. Un appel unique -> N lignes (une par pays).
Projection: iso_3166_1, english_name, native_name. Pas de refresh par date.
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

class ConfigurationCountriesFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="_unused_input.json",   # non utilisé
            output_file="ref_countries.ndjson",
            log_file="ref_countries.log",
            entity_type="ref_countries",
            window_days=None,
            date_field=None,
            id_field="id",                     # sans impact ici
            extra_params={},
        )

    def get_endpoint(self, _: int = 0) -> str:
        return "/configuration/countries"

    def _project_rows(self, data):
        """
        TMDB payload attendu: liste de pays
        [
          {"iso_3166_1":"US","english_name":"United States of America","native_name":"United States of America"},
          {"iso_3166_1":"FR","english_name":"France","native_name":"France"},
          ...
        ]
        """
        rows = []
        if isinstance(data, list):
            for obj in data:
                if not isinstance(obj, dict):
                    continue
                iso = obj.get("iso_3166_1")
                en = obj.get("english_name")
                nat = obj.get("native_name")
                if not isinstance(iso, str) or not isinstance(en, str) or not isinstance(nat, str):
                    continue
                rows.append({
                    "iso_3166_1": iso,
                    "english_name": en,
                    "native_name": nat,
                })
        return rows

    def run(self):
        # Appel unique
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = tmdb_request(
            self.get_endpoint(),
            self.bearer,
            self.limiter,
            self.error_counter,
            self.extra_params
        )
        rows = self._project_rows(data or [])

        # Écriture atomique
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        with self.tmp_path.open("w", encoding="utf-8") as out:
            for r in rows:
                out.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.tmp_path.replace(self.output_path)

        # Log synthèse
        date_str = __import__("time").strftime("%d/%m/%Y")
        total_lines = len(rows)
        append_summary_log(self.log_path, date_str, total_lines, 0, total_lines, self.error_counter, self.entity_type)

        sys.stderr.write(f"\n[OK] NDJSON écrit : {self.output_path} | rows={total_lines}\n")


def main():
    ConfigurationCountriesFetcher().run()


if __name__ == "__main__":
    main()
