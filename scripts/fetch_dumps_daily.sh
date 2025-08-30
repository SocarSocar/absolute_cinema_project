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
# ============================================================

# ---------- Discipline d'exécution ----------
set -euo pipefail
# -e  : stoppe le script au premier échec (sécurité transactionnelle côté Bash).
# -u  : interdit l’usage de variables non définies (évite les chemins vides).
# -o pipefail : propage une erreur si une commande quelconque dans un pipe échoue.

# ---------- Racine de projet et arborescence contrôlée ----------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# ^ Détermine la racine comme le dossier parent du script, peu importe d’où on l’exécute.

IDS="$ROOT/data/ids"     # Dépôt temporaire des archives .gz téléchargées.
OUT="$ROOT/data/out"     # Fichiers JSONL "finaux" cumulés, prêts pour la suite du pipeline.
STATE="$ROOT/state"      # État persistant minimal (date de dernier succès).
LOGS="$ROOT/logs"        # Journaux d’exécution (audit, debug, monitoring).

# Crée l’arborescence si nécessaire (idempotent).
mkdir -p "$IDS" "$OUT" "$STATE" "$LOGS"

# ---------- Référence temporelle en UTC ----------
STAMP="$(date -u +%F)"  # Date ISO (AAAA-MM-JJ) → pivot d’idempotence journalière.
MM="$(date -u +%m)"      # Mois 2 chiffres (ex: 08).
DD="$(date -u +%d)"      # Jour 2 chiffres.
YYYY="$(date -u +%Y)"    # Année 4 chiffres.

# ---------- Fichiers de pilotage ----------
LAST_STAMP_FILE="$STATE/last_success_date.txt"  # Garde la dernière date traitée avec succès.
LOG_FILE="$LOGS/fetch_ids.log"                  # Journal append-only horodaté.

# ---------- Utilitaires ----------
log() {
  # Log standardisé en UTC : facilite la corrélation multi-serveurs.
  # Forme : "YYYY-MM-DD HH:MM:SS | MESSAGE"
  echo "$(date -u +'%F %T') | $1" | tee -a "$LOG_FILE" >/dev/null
}

curl_get() {
  # Téléchargement robuste :
  # -f : fail en cas de code HTTP >=400
  # -sS : silencieux mais affiche les erreurs
  # -L : suit les redirections (sécurité côté CDN)
  # --retry / --retry-delay : résilience réseau (ex: instabilité CDN/TMDB)
  curl -fsSL --retry 5 --retry-delay 2 -o "$2" "$1"
}

# ---------- Idempotence journalière ----------
# Si on a déjà un succès daté du jour, on ne refait rien.
if [[ -f "$LAST_STAMP_FILE" ]] && [[ "$(cat "$LAST_STAMP_FILE")" == "$STAMP" ]]; then
  log "SKIP already done for ${STAMP}"
  exit 0
fi

# ---------- Fonction générique de traitement d’un export ----------
# Paramètres :
#   $1 = label pour les logs            (ex: movies, tv_series, people, ...)
#   $2 = mode pour le script Python     (ex: movies, tv, people, networks, keywords, companies)
#   $3 = préfixe du fichier distant     (ex: movie_ids, tv_series_ids, person_ids, ...)
#   $4 = nom du fichier final JSONL     (ex: movie_dumps.json)
process_export() {
  local LABEL="$1"
  local MODE="$2"
  local PREFIX="$3"
  local FINAL_BASENAME="$4"

  # Chemins journaliers intermédiaires/finale
  local GZ="$IDS/${PREFIX}_${STAMP}.json.gz"   # Archive brute du jour.
  local TMP="$OUT/.tmp_${PREFIX}_${STAMP}.jsonl" # Décompression JSONL intermédiaire.
  local FINAL="$OUT/${FINAL_BASENAME}"         # Cumul JSONL dédupliqué.

  # 1) Téléchargement de l’archive du jour
  if curl_get "https://files.tmdb.org/p/exports/${PREFIX}_${MM}_${DD}_${YYYY}.json.gz" "$GZ"; then
    # 2) Décompression en JSON Lines (1 objet JSON par ligne)
    #    zcat est non destructif et rapide ; alternative: gunzip -c
    zcat "$GZ" > "$TMP"

    # 3) Fusion incrémentale/dédupliquée via Python (réécriture atomique côté Python)
    local PY_OUT
    if ! PY_OUT="$(python3 "$ROOT/scripts/merge_tmdb_into_final.py" "$MODE" "$TMP" "$FINAL")"; then
      # Si la fusion échoue, on journalise et on nettoie les temporaires du jour
      log "ERROR ${LABEL} ${STAMP} merge failed"
      rm -f "$GZ" "$TMP"
      return
    fi

    # 4) Extraction des métriques retournées par le script Python (added / total)
    local ADDED TOTAL
    ADDED="$(printf '%s' "$PY_OUT" | awk -F'added=' '{print $2}' | awk '{print $1}')"
    TOTAL="$(printf '%s' "$PY_OUT" | awk -F'total=' '{print $2}' | awk '{print $1}')"

    # 5) Nettoyage des fichiers journaliers (économie d’espace)
    rm -f "$GZ" "$TMP"

    # 6) Journalisation succès synthétique
    log "OK ${LABEL} ${STAMP} added=${ADDED} total=${TOTAL}"
  else
    # Téléchargement échoué → pas de fusion
    log "ERROR ${LABEL} ${STAMP} download failed"
  fi
}

# ---------- Exécutions par domaine ----------
# Films (IDs de films TMDB)
process_export "movies"                  "movies"    "movie_ids"                 "movie_dumps.json"
# Séries TV (IDs de séries TMDB)
process_export "tv_series"               "tv"        "tv_series_ids"             "tv_series_dumps.json"
# Personnes (IDs de personnes TMDB)
process_export "people"                  "people"    "person_ids"                "people_dumps.json"
# Chaînes TV (IDs de networks TMDB)
process_export "tv_networks"             "networks"  "tv_network_ids"            "tv_networks_dumps.json"
# Mots-clés (IDs de mots-clés TMDB)
process_export "keywords"                "keywords"  "keyword_ids"               "keywords_dumps.json"
# Sociétés de production (IDs de compagnies TMDB)
process_export "production_companies"    "companies" "production_company_ids"    "production_companies_dumps.json"

# ---------- Validation d’un cycle quotidien réussi ----------
# On écrit la date uniquement si au moins un domaine a terminé en "OK".
# Cela laisse la possibilité de relancer le jour même si tout a échoué.
if grep -q "OK .* ${STAMP}" "$LOG_FILE"; then
  echo "$STAMP" > "$LAST_STAMP_FILE"
fi
# Fin du script