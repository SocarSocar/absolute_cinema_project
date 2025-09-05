#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NETWORK DETAILS — /network/{network_id}
Entrée :  data/out/tv_networks_dumps.json (clé: id)
Sortie :  data/out/tv_networks_details.ndjson
Log :     logs/fetch_TMDB_API/tv_networks_details.log
entity_type : tv_networks_details
Champs projetés : headquarters, id, name, origin_country
Pas de refresh par date.
"""

import sys
from pathlib import Path

# Import du module commun
sys.path.insert(0, str(Path(__file__).parent))

from fetch_API_TMDB import (  # noqa: E402
    TMDBFetcher,
)


class NetworkDetailsFetcher(TMDBFetcher):
    def __init__(self):
        super().__init__(
            input_file="tv_networks_dumps.json",
            output_file="tv_networks_details.ndjson",
            log_file="tv_networks_details.log",
            entity_type="tv_networks_details",
            window_days=None,
            date_field=None,
            id_field="id",
            extra_params=None,  # aucun paramètre fixe
        )

    def get_endpoint(self, entity_id: int) -> str:
        return f"/network/{entity_id}"

    def project_fields(self, data: dict) -> dict:
        return {
            "headquarters": data.get("headquarters"),
            "id": data.get("id"),
            "name": data.get("name"),
            "origin_country": data.get("origin_country"),
        }


def main():
    NetworkDetailsFetcher().run()


if __name__ == "__main__":
    main()
