# ------------------------------------------------------
# Makefile pour gérer facilement les conteneurs Docker
# du projet Absolute Cinema
#
# Astuce : toutes les commandes sont des raccourcis
# pour éviter de taper les longues commandes docker compose.
#
# Exemple : "make up" = "docker compose up -d"
# ------------------------------------------------------

.PHONY: build up down sh logs upload clean full-update ps images prune

# ------------------------------------------------------
# Construit les images Docker (app, upload, full_update)
# Refaire "make build" si tu modifies requirements.txt
# ou le Dockerfile
# ------------------------------------------------------
build:
	docker compose build

# ------------------------------------------------------
# Démarre le conteneur app en arrière-plan (-d = detached)
# Le conteneur "app" reste vivant (sleep infinity).
# ------------------------------------------------------
up:
	docker compose up -d

# ------------------------------------------------------
# Stoppe et supprime les conteneurs en cours
# (mais garde les volumes et réseaux).
# ------------------------------------------------------
down:
	docker compose down

# ------------------------------------------------------
# Ouvre un shell bash dans le conteneur app
# Très pratique pour lancer des scripts à la main
# ou tester FastAPI plus tard.
# ------------------------------------------------------
sh:
	docker compose exec app bash

# ------------------------------------------------------
# Affiche les logs récents de tous les conteneurs
# (200 dernières lignes) et suit en temps réel (-f).
# ------------------------------------------------------
logs:
	docker compose logs -f --tail=200

# ------------------------------------------------------
# Lance le service "upload" :
# exécute uniquement script/Load_Snowflake/upload_to_stage.py
# puis s'arrête. Parfait pour tester l'upload seul.
# ------------------------------------------------------
upload:
	docker compose run --rm upload

# ------------------------------------------------------
# Supprime les conteneurs, volumes et orphelins
# + fait un gros ménage du système Docker.
# ATTENTION : ça peut supprimer des images pas liées à ce projet.
# ------------------------------------------------------
clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# ------------------------------------------------------
# Lance le service "full_update" :
# exécute script/fetch_TMDB_API/run_all_scripts.py
# puis script/Load_Snowflake/upload_to_stage.py
# C'est le cycle complet (fetch + upload).
# ------------------------------------------------------
full-update:
	docker compose run --rm full_update

# ------------------------------------------------------
# Commandes supplémentaires utiles :
# ------------------------------------------------------

# Affiche l'état des conteneurs (comme "docker ps")
ps:
	docker compose ps

# Liste les images Docker présentes
images:
	docker images

# Nettoyage soft (supprime juste conteneurs et réseaux inutilisés)
prune:
	docker system prune -f
