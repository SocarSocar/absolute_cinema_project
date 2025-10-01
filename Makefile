# ------------------------------------------------------
# Makefile pour gérer facilement les conteneurs Docker
# du projet Absolute Cinema
#
# Astuce : toutes les commandes sont des raccourcis
# pour éviter de taper les longues commandes docker compose.
#
# Exemple : "make up" = "docker compose up -d"
# ------------------------------------------------------

.PHONY: build up down sh logs upload clean full-update ps images prune \
        start-all stop-all \
        monitoring-up monitoring-down monitoring-logs monitoring-reload monitoring-status monitoring-open \
        connect-monitor-net check-health \
        airflow-help airflow-venv airflow-install airflow-init airflow-user airflow-up airflow-stop airflow-status airflow-logs airflow-open airflow-test airflow-trigger airflow-reset

SHELL := /bin/bash

# Répertoires et fichiers
PROJECT_DIR := $(shell pwd)
MON_DIR := $(PROJECT_DIR)/monitoring
MON_COMPOSE := $(MON_DIR)/docker-compose.monitor.yml
PROM_FILE := $(MON_DIR)/prometheus.yml

# Réseau de monitoring (défini dans docker-compose.monitor.yml)
MON_NET := mon_net

# =========================
# Docker de l'application
# =========================

# ------------------------------------------------------
# Construit les images Docker (app, upload, full_update)
# Refaire "make build" si tu modifies requirements.txt
# ou le Dockerfile
# ------------------------------------------------------
build:
	docker compose build

# ------------------------------------------------------
# Démarre les services de l'app en arrière-plan (-d)
# Ex: "app" (FastAPI) reste vivant (sleep infinity ou uvicorn)
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

# Commandes supplémentaires utiles :
ps:
	docker compose ps

images:
	docker images

prune:
	docker system prune -f


# =========================
# Monitoring (Prom/Grafana)
# =========================
# Ports par défaut:
# - Prometheus : 9090
# - Grafana    : 3000
# - cAdvisor   : 8081 (8080 réservé à Airflow)
# - Blackbox   : 9115
# - Alertmgr   : 9093

monitoring-up:
	@echo "[monitoring-up] Démarrage stack monitoring"
	cd "$(MON_DIR)" && docker compose -f "$(MON_COMPOSE)" up -d
	@echo "[monitoring-up] OK -> Prometheus: http://localhost:9090 | Grafana: http://localhost:3000"

monitoring-down:
	@echo "[monitoring-down] Arrêt stack monitoring"
	cd "$(MON_DIR)" && docker compose -f "$(MON_COMPOSE)" down

monitoring-logs:
	cd "$(MON_DIR)" && docker compose -f "$(MON_COMPOSE)" logs -f --tail=200

monitoring-reload:  ## recharge Prometheus (si fichier prometheus.yml modifié)
	@echo "[monitoring-reload] Rechargement Prometheus"
	# SIGHUP (si exposé) sinon redéploiement propre
	- docker kill -s HUP mon_prometheus || true
	cd "$(MON_DIR)" && docker compose -f "$(MON_COMPOSE)" up -d

monitoring-status:
	@echo "[monitoring-status] Services monitoring:"
	cd "$(MON_DIR)" && docker compose -f "$(MON_COMPOSE)" ps

monitoring-open:
	@echo "Prometheus -> http://localhost:9090"
	@echo "Grafana    -> http://localhost:3000"
	@echo "Alertmgr   -> http://localhost:9093"
	@echo "cAdvisor   -> http://localhost:8081"


# =========================
# Réseau entre API & Monit
# =========================
# Connecte l'API (absolute_app) au réseau mon_net pour que Prometheus/Blackbox
# puissent joindre l'endpoint santé interne (ex: http://absolute_app:8000/v1/health)

connect-monitor-net:
	@echo "[connect-monitor-net] Création réseau $(MON_NET) si absent"
	- docker network create $(MON_NET) >/dev/null 2>&1 || true
	@echo "[connect-monitor-net] Connexion du conteneur absolute_app -> $(MON_NET)"
	- docker network connect $(MON_NET) absolute_app >/dev/null 2>&1 || true
	@echo "[connect-monitor-net] Test rapide depuis $(MON_NET) -> http://absolute_app:8000/v1/health"
	- docker run --rm --network $(MON_NET) curlimages/curl -sS http://absolute_app:8000/v1/health || true
	@echo "[connect-monitor-net] Si tu préfères l'alias 'app', exécute:"
	@echo "  docker network disconnect $(MON_NET) absolute_app && docker network connect --alias app $(MON_NET) absolute_app"

# Petit check santé (fonctionne si l'API est connectée à $(MON_NET))
check-health:
	@docker run --rm --network $(MON_NET) curlimages/curl -sS http://absolute_app:8000/v1/health || true


# =========================
# Airflow — gestion complète
# =========================
# Utilise un venv dédié (~/.venvs/airflow), AIRFLOW_HOME dans ./airflow,
# webserver et scheduler lancés en arrière-plan (PID enregistrés).
# DAG ciblé: daily_full_update (si créé).
# FastAPI (app) doit être up pour que la tâche api_ping passe: `docker compose up -d app`

AIRFLOW_HOME := $(PROJECT_DIR)/airflow
AIRFLOW_VENV := $(HOME)/.venvs/airflow
AIRFLOW_PORT ?= 8080
AIRFLOW_VERSION ?= 2.9.3
PYTHON_BIN := python3

# Fichiers PID pour arrêter proprement
AIRFLOW_WEB_PID := $(AIRFLOW_HOME)/.airflow-web.pid
AIRFLOW_SCH_PID := $(AIRFLOW_HOME)/.airflow-sch.pid

airflow-help:
	@echo "Airflow — commandes:"
	@echo "  make airflow-venv     # Crée le venv dédié (~/.venvs/airflow)"
	@echo "  make airflow-install  # Installe Airflow $(AIRFLOW_VERSION) + deps"
	@echo "  make airflow-init     # Initialise AIRFLOW_HOME=$(AIRFLOW_HOME) et la DB"
	@echo "  make airflow-user     # Crée l'utilisateur admin (admin/admin)"
	@echo "  make airflow-up       # Démarre webserver:$(AIRFLOW_PORT) + scheduler"
	@echo "  make airflow-stop     # Arrête webserver + scheduler via PID"
	@echo "  make airflow-status   # Affiche l'état des processus Airflow"
	@echo "  make airflow-logs     # Emplacement des logs"
	@echo "  make airflow-open     # Affiche l'URL de l'UI"
	@echo "  make airflow-test     # Test du DAG daily_full_update (dry-run)"
	@echo "  make airflow-trigger  # Déclenche un run réel du DAG"
	@echo "  make airflow-reset    # Réinitialise la DB (danger)"

airflow-venv:
	@echo "[airflow-venv] Création du venv: $(AIRFLOW_VENV)"
	$(PYTHON_BIN) -m venv "$(AIRFLOW_VENV)"
	@echo "[airflow-venv] OK -> source $(AIRFLOW_VENV)/bin/activate"

airflow-install:
	@echo "[airflow-install] Installation Airflow $(AIRFLOW_VERSION) + contraintes"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  pip install --upgrade pip && \
	  PYV=$$(python -c 'import sys;print(".".join(map(str,sys.version_info[:2])))') && \
	  pip install "apache-airflow==$(AIRFLOW_VERSION)" \
	    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$$PYV.txt"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  pip install snowflake-connector-python python-dotenv requests
	@echo "[airflow-install] OK"

airflow-init:
	@echo "[airflow-init] AIRFLOW_HOME=$(AIRFLOW_HOME)"
	mkdir -p "$(AIRFLOW_HOME)/dags" "$(AIRFLOW_HOME)/logs" "$(AIRFLOW_HOME)/plugins"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow db init
	@echo "[airflow-init] OK"

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

airflow-stop:
	@echo "[airflow-stop] Arrêt des services Airflow"
	@if [ -f "$(AIRFLOW_WEB_PID)" ]; then kill $$(cat "$(AIRFLOW_WEB_PID)") || true; rm -f "$(AIRFLOW_WEB_PID)"; fi
	@if [ -f "$(AIRFLOW_SCH_PID)" ]; then kill $$(cat "$(AIRFLOW_SCH_PID)") || true; rm -f "$(AIRFLOW_SCH_PID)"; fi
	@echo "[airflow-stop] OK"

airflow-status:
	@echo "[airflow-status] Processus webserver/scheduler:"
	@ps aux | grep -E "airflow (webserver|scheduler)" | grep -v grep || true

airflow-logs:
	@echo "[airflow-logs] Webserver: $(AIRFLOW_HOME)/webserver.out"
	@echo "[airflow-logs] Scheduler: $(AIRFLOW_HOME)/scheduler.out"
	@echo "[airflow-logs] Tâches: $(AIRFLOW_HOME)/logs/<dag_id>/<task_id>/<run_id>/"

airflow-open:
	@echo "http://localhost:$(AIRFLOW_PORT)"

airflow-test:
	@echo "[airflow-test] airflow dags test daily_full_update <ts>"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow dags test daily_full_update $$($(PYTHON_BIN) - <<'PY'
	from datetime import datetime, timezone
	print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
	PY
	)

airflow-trigger:
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow dags trigger daily_full_update

airflow-reset:
	@echo "[airflow-reset] ATTENTION: suppression de l'historique Airflow"
	source "$(AIRFLOW_VENV)/bin/activate" && \
	  export AIRFLOW_HOME="$(AIRFLOW_HOME)" && \
	  airflow db reset -y
	rm -f "$(AIRFLOW_WEB_PID)" "$(AIRFLOW_SCH_PID)" || true
	@echo "[airflow-reset] OK"


# =========================
# Lancer / Arrêter toute la stack
# =========================

start-all:
	@echo "[start-all] Démarrage API (app), Airflow, et Monitoring"
	# 0) S'assure que le réseau externe mon_net existe (utile car monitoring l'attend)
	- docker network create $(MON_NET) >/dev/null 2>&1 || true

	# 1) Monitoring (utilise mon_net en external:true)
	$(MAKE) monitoring-up

	# 2) App (FastAPI)
	docker compose up -d app

	# 3) Connecte l'app au réseau de monitoring (au cas où)
	- docker network connect $(MON_NET) absolute_app >/dev/null 2>&1 || true

	# 4) Airflow (webserver + scheduler)
	$(MAKE) airflow-up

	@echo "[start-all] OK."
	@echo "  -> API:      http://localhost:8000/docs"
	@echo "  -> Health:   http://localhost:8000/v1/health"
	@echo "  -> Airflow:  http://localhost:8080"
	@echo "  -> Prom:     http://localhost:9090"
	@echo "  -> Grafana:  http://localhost:3000"
	@echo "  -> cAdvisor: http://localhost:8081"

stop-all:
	@echo "[stop-all] Arrêt de l'API (app), Airflow et Monitoring"
	# API
	docker compose stop app || true
	# Airflow
	$(MAKE) airflow-stop
	# Monitoring
	$(MAKE) monitoring-down
	@echo "[stop-all] OK"

# reset dur si mon_net a un mauvais label (utiliser en dernier recours)
mon-net-reset:
	- docker compose -f $(MON_COMPOSE) down || true
	- docker network rm $(MON_NET) || true
	docker network create $(MON_NET)
