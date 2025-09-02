#!/usr/bin/env bash
# ============================================================
# Script d’automatisation de récupération des IDs TMDB
# Objectif global :
#   1) Télécharger les exports quotidiens TMDB (IDs par domaine).
#   2) Décompresser en JSONL.
#   3) Fusionner de façon incrémentale et dédupliquée via un script Python.
#   4) Logguer précisément et garantir l’idempotence journalière.
#
# Ce script est volontairement verbeux pour documenter chaque étape.
# Emplacement: scripts/dumps_daily/fetch_dumps_daily.sh
# ============================================================

# ---------- Discipline d'exécution ----------
set -euo pipefail
# -e          : stoppe le script au premier échec
# -u          : interdit l’usage de variables non définies
# -o pipefail : propage l’erreur d’une commande dans un pipe

# ---------- Racine de projet et arborescence contrôlée ----------
# Scripts placés sous scripts/dumps_daily/ → remonter de 2 niveaux
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

IDS="$ROOT/data/ids"     # Dépôt temporaire des archives .gz téléchargées.
OUT="$ROOT/data/out"     # Fichiers JSONL cumulés, prêts pour la suite du pipeline.
STATE="$ROOT/state"      # État persistant minimal (date de dernier succès).
LOGS="$ROOT/logs"        # Journaux d’exécution (audit, debug, monitoring).

mkdir -p "$IDS" "$OUT" "$STATE" "$LOGS"

# ---------- Référence temporelle en UTC ----------
STAMP="$(date -u +%F)"  # AAAA-MM-JJ
MM="$(date -u +%m)"
DD="$(date -u +%d)"
YYYY="$(date -u +%Y)"

# ---------- Fichiers de pilotage ----------
LAST_STAMP_FILE="$STATE/last_success_date.txt"
LOG_FILE="$LOGS/fetch_dumps.log"

# ---------- Utilitaires ----------
log() { echo "$(date -u +'%F %T') | $1" | tee -a "$LOG_FILE" >/dev/null; }

curl_get() {
  curl -fsSL --retry 5 --retry-delay 2 -o "$2" "$1"
}

# ---------- Idempotence journalière ----------
if [[ -f "$LAST_STAMP_FILE" ]] && [[ "$(cat "$LAST_STAMP_FILE")" == "$STAMP" ]]; then
  log "SKIP already done for ${STAMP}"
  exit 0
fi

# ---------- Fonction générique de traitement d’un export ----------
# $1 = label logs  | $2 = mode python | $3 = prefix TMDB | $4 = nom final JSONL
process_export() {
  local LABEL="$1"
  local MODE="$2"
  local PREFIX="$3"
  local FINAL_BASENAME="$4"

  local GZ="$IDS/${PREFIX}_${STAMP}.json.gz"
  local TMP="$OUT/.tmp_${PREFIX}_${STAMP}.jsonl"
  local FINAL="$OUT/${FINAL_BASENAME}"

  if curl_get "https://files.tmdb.org/p/exports/${PREFIX}_${MM}_${DD}_${YYYY}.json.gz" "$GZ"; then
    zcat "$GZ" > "$TMP"

    local PY_OUT
    if ! PY_OUT="$(python3 "$ROOT/scripts/dumps_daily/merge_dumps_final.py" "$MODE" "$TMP" "$FINAL")"; then
      log "ERROR ${LABEL} ${STAMP} merge failed"
      rm -f "$GZ" "$TMP"
      return
    fi

    local ADDED TOTAL
    ADDED="$(printf '%s' "$PY_OUT" | awk -F'added=' '{print $2}' | awk '{print $1}')"
    TOTAL="$(printf '%s' "$PY_OUT" | awk -F'total=' '{print $2}' | awk '{print $1}')"

    rm -f "$GZ" "$TMP"
    log "OK ${LABEL} ${STAMP} added=${ADDED} total=${TOTAL}"
  else
    log "ERROR ${LABEL} ${STAMP} download failed"
  fi
}

# ---------- Exécutions par domaine ----------
process_export "movies"               "movies"    "movie_ids"              "movie_dumps.json"
process_export "tv_series"            "tv"        "tv_series_ids"          "tv_series_dumps.json"
process_export "people"               "people"    "person_ids"             "people_dumps.json"
process_export "tv_networks"          "networks"  "tv_network_ids"         "tv_networks_dumps.json"
process_export "keywords"             "keywords"  "keyword_ids"            "keywords_dumps.json"
process_export "production_companies" "companies" "production_company_ids" "production_companies_dumps.json"

# ---------- Validation d’un cycle quotidien réussi ----------
if grep -q "OK .* ${STAMP}" "$LOG_FILE"; then
  echo "$STAMP" > "$LAST_STAMP_FILE"
fi
# Fin du script
