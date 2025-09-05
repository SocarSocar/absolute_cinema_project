#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
COMPANY DETAILS — /company/{company_id}
Entrée :  data/out/production_companies_dumps.json (clé: id)
Sortie :  data/out/company_details.ndjson
Log :     logs/fetch_TMDB_API/company_details.log
entity_type : company_details
Champs projetés (exhaustif) :
  - id
  - name
  - description
  - origin_country
  - headquarters
  - parent_company.id
  - parent_company.name
Pas de refresh par date.
"""

import sys
from pathlib import Path

# Import du module commun
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # noqa: E402
    TMDBFetcher,
)


class CompanyDetailsFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="production_companies_dumps.json",
            output_file="company_details.ndjson",
            log_file="company_details.log",
            entity_type="company_details",
            window_days=None,
            date_field=None,
            id_field="id",
            extra_params=None,  # aucun paramètre fixe
        )

    def get_endpoint(self, entity_id: int) -> str:
        return f"/company/{entity_id}"

    def project_fields(self, data: dict) -> dict:
        parent = data.get("parent_company") or {}
        parent_id = parent.get("id") if isinstance(parent, dict) else None
        parent_name = parent.get("name") if isinstance(parent, dict) else None

        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "description": data.get("description"),
            "origin_country": data.get("origin_country"),
            "headquarters": data.get("headquarters"),
            "parent_company": {
                "id": parent_id,
                "name": parent_name,
            },
        }


def main():
    CompanyDetailsFetcher().run()


if __name__ == "__main__":
    main()
