#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupère la liste des genres TV via /genre/tv/list.
Aucun input d’IDs. Un appel unique -> N lignes (une par genre).
Projection: id, name. Pas de refresh par date.
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

class TVGenresFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="_unused_input.json",     # non utilisé
            output_file="ref_genre_series.ndjson",
            log_file="ref_genre_series.log",
            entity_type="ref_genre_series",
            window_days=None,
            date_field=None,
            id_field="id",                       # sans impact ici
            extra_params={},
        )

    def get_endpoint(self, _: int = 0) -> str:
        return "/genre/tv/list"

    def _project_rows(self, data):
        """
        TMDB payload:
        {
          "genres": [
            {"id": 10759, "name": "Action & Adventure"},
            {"id": 16, "name": "Animation"},
            ...
          ]
        }
        """
        rows = []
        genres = data.get("genres") or []
        if isinstance(genres, list):
            for g in genres:
                if not isinstance(g, dict):
                    continue
                gid = g.get("id")
                name = g.get("name")
                if not isinstance(gid, int) or not isinstance(name, str):
                    continue
                rows.append({"id": gid, "name": name})
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
        rows = self._project_rows(data or {})

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
    TVGenresFetcher().run()


if __name__ == "__main__":
    main()
