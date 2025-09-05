#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PERSON DETAILS — /person/{person_id}
Entrée :  data/out/people_dumps.json (clé: id)
Sortie :  data/out/people_details.ndjson
Log :     logs/fetch_TMDB_API/people_details.log
entity_type : people_details
Champs projetés (exhaustif) :
  - id
  - name
  - also_known_as
  - biography
  - birthday
  - deathday
  - place_of_birth
  - popularity
  - gender
  - known_for_department
Pas de refresh par date.
"""

import sys
from pathlib import Path

# Import du module commun
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # noqa: E402
    TMDBFetcher,
)


class PersonDetailsFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="people_dumps.json",
            output_file="people_details.ndjson",
            log_file="people_details.log",
            entity_type="people_details",
            window_days=None,
            date_field=None,
            id_field="id",
            extra_params=None,  # aucun paramètre fixe
        )

    def get_endpoint(self, entity_id: int) -> str:
        return f"/person/{entity_id}"

    def project_fields(self, data: dict) -> dict:
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "also_known_as": data.get("also_known_as") if isinstance(data.get("also_known_as"), list) else [],
            "biography": data.get("biography"),
            "birthday": data.get("birthday"),
            "deathday": data.get("deathday"),
            "place_of_birth": data.get("place_of_birth"),
            "popularity": data.get("popularity"),
            "gender": data.get("gender"),
            "known_for_department": data.get("known_for_department"),
        }


def main():
    PersonDetailsFetcher().run()


if __name__ == "__main__":
    main()
