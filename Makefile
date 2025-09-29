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

# =========================
# Airflow — gestion complète
# =========================
# Utilise un venv dédié (~/.venvs/airflow), AIRFLOW_HOME dans ./airflow,
# webserver et scheduler lancés en arrière-plan (PID enregistrés).
# DAG ciblé: daily_full_update (créé à l’étape de ton guide précédent).
# FastAPI (app) doit être up pour que la tâche api_ping passe: `docker compose up -d app`

SHELL := /bin/bash

PROJECT_DIR := $(shell pwd)
AIRFLOW_HOME := $(PROJECT_DIR)/airflow
AIRFLOW_VENV := $(HOME)/.venvs/airflow
AIRFLOW_PORT ?= 8080
AIRFLOW_VERSION ?= 2.9.3
PYTHON_BIN := python3

# Fichiers PID pour arrêter proprement
AIRFLOW_WEB_PID := $(AIRFLOW_HOME)/.airflow-web.pid
AIRFLOW_SCH_PID := $(AIRFLOW_HOME)/.airflow-sch.pid

.PHONY: airflow-help airflow-venv airflow-install airflow-init airflow-user airflow-up airflow-stop airflow-status airflow-logs airflow-open airflow-test airflow-trigger airflow-reset

airflow-help:
	@echo "Airflow — commandes:"
	@echo "  make airflow-venv     # Crée le venv dédié (~/.venvs/airflow)"
	@echo "  make airflow-install  # Installe Airflow $(AIRFLOW_VERSION) + deps dans le venv"
	@echo "  make airflow-init     # Initialise AIRFLOW_HOME=$(AIRFLOW_HOME) et la DB"
	@echo "  make airflow-user     # Crée l'utilisateur admin (admin/admin)"
	@echo "  make airflow-up       # Démarre webserver:$(AIRFLOW_PORT) + scheduler en arrière-plan"
	@echo "  make airflow-stop     # Arrête webserver + scheduler via PID"
	@echo "  make airflow-status   # Affiche l'état des processus Airflow"
	@echo "  make airflow-logs     # Indique l'emplacement des logs"
	@echo "  make airflow-open     # Affiche l'URL de l'UI"
	@echo "  make airflow-test     # Teste le DAG daily_full_update sans enregistrer de run"
	@echo "  make airflow-trigger  # Déclenche un run réel du DAG daily_full_update"
	@echo "  make airflow-reset    # Réinitialise la DB (danger: supprime l'historique)"

# 1) Crée le venv propre à Airflow
airflow-venv:
	@echo "[airflow-venv] Création du venv: $(AIRFLOW_VENV)"
	$(PYTHON_BIN) -m venv "$(AIRFLOW_VENV)"
	@echo "[airflow-venv] OK -> source $(AIRFLOW_VENV)/bin/activate"

# 2) Installe Airflow + contraintes + deps (dans le venv)
airflow-install:
	@echo "[airflow-install] Installation Airflow $(AIRFLOW_VERSION) + contraintes"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  pip install --upgrade pip && \
	  PYV=$$(python -c 'import sys;print(".".join(map(str,sys.version_info[:2])))') && \
	  pip install "apache-airflow==$(AIRFLOW_VERSION)" \
	    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$$PYV.txt"
	@echo "[airflow-install] Dépendances opérateurs/checks"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  pip install snowflake-connector-python python-dotenv requests
	@echo "[airflow-install] OK"

# 3) Initialise AIRFLOW_HOME et la base de métadonnées
airflow-init:
	@echo "[airflow-init] AIRFLOW_HOME=$(AIRFLOW_HOME)"
	mkdir -p "$(AIRFLOW_HOME)/dags" "$(AIRFLOW_HOME)/logs" "$(AIRFLOW_HOME)/plugins"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow db init
	@echo "[airflow-init] OK"

# 4) Crée l'utilisateur admin (UI login: admin / admin)
airflow-user:
	@echo "[airflow-user] Création user admin"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow users create \
	    --username admin \
	    --firstname Admin \
	    --lastname User \
	    --role Admin \
	    --email admin@example.com \
	    --password admin
	@echo "[airflow-user] OK"

# 5) Démarre webserver + scheduler en arrière-plan, enregistre les PID
airflow-up:
	@echo "[airflow-up] Démarrage webserver sur port $(AIRFLOW_PORT) & scheduler"
	mkdir -p "$(AIRFLOW_HOME)"
	# Webserver
	nohup bash -lc 'source "$(AIRFLOW_VENV)/bin/activate"; export AIRFLOW_HOME="$(AIRFLOW_HOME)"; airflow webserver --port $(AIRFLOW_PORT)' \
	  > "$(AIRFLOW_HOME)/webserver.out" 2>&1 < /dev/null &
	echo $$! > "$(AIRFLOW_WEB_PID)"
	# Scheduler
	nohup bash -lc 'source "$(AIRFLOW_VENV)/bin/activate"; export AIRFLOW_HOME="$(AIRFLOW_HOME)"; airflow scheduler' \
	  > "$(AIRFLOW_HOME)/scheduler.out" 2>&1 < /dev/null &
	echo $$! > "$(AIRFLOW_SCH_PID)"
	@echo "[airflow-up] Web PID: $$(cat $(AIRFLOW_WEB_PID)) | Sch PID: $$(cat $(AIRFLOW_SCH_PID))"
	@echo "[airflow-up] UI -> http://localhost:$(AIRFLOW_PORT)"

# 6) Stoppe proprement via PID (si les fichiers existent)
airflow-stop:
	@echo "[airflow-stop] Arrêt des services Airflow"
	@if [ -f "$(AIRFLOW_WEB_PID)" ]; then kill $$(cat "$(AIRFLOW_WEB_PID)") || true; rm -f "$(AIRFLOW_WEB_PID)"; fi
	@if [ -f "$(AIRFLOW_SCH_PID)" ]; then kill $$(cat "$(AIRFLOW_SCH_PID)") || true; rm -f "$(AIRFLOW_SCH_PID)"; fi
	@echo "[airflow-stop] OK"

# 7) Affiche l'état des processus (utile pour vérifier après airflow-up)
airflow-status:
	@echo "[airflow-status] Processus webserver/scheduler:"
	@ps aux | grep -E "airflow (webserver|scheduler)" | grep -v grep || true

# 8) Indique où lire les logs (webserver.out / scheduler.out + logs des tâches)
airflow-logs:
	@echo "[airflow-logs] Webserver: $(AIRFLOW_HOME)/webserver.out"
	@echo "[airflow-logs] Scheduler: $(AIRFLOW_HOME)/scheduler.out"
	@echo "[airflow-logs] Tâches: $(AIRFLOW_HOME)/logs/<dag_id>/<task_id>/<run_id>/"

# 9) Affiche l’URL d’accès UI
airflow-open:
	@echo "http://localhost:$(AIRFLOW_PORT)"

# 10) Teste localement le DAG (n’enregistre pas d’historique) — utile après modif du DAG
airflow-test:
	@echo "[airflow-test] airflow dags test daily_full_update <ts>"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow dags test daily_full_update $$($(PYTHON_BIN) - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)

# 11) Déclenche un run réel du DAG (enregistré dans l'historique)
airflow-trigger:
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow dags trigger daily_full_update

# 12) Réinitialise la DB Airflow (danger: supprime l’historique et les runs)
airflow-reset:
	@echo "[airflow-reset] ATTENTION: suppression de l'historique Airflow"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow db reset -y
	rm -f "$(AIRFLOW_WEB_PID)" "$(AIRFLOW_SCH_PID)" || true
	@echo "[airflow-reset] OK"
