# ============================================================
# Script d’automatisation de récupération des IDs TMDB
#
# Objectif :
#   - Télécharger chaque jour les fichiers d’export officiels de TMDB
#     (liste des films et des séries, compressés en .json.gz).
#   - Les décompresser, compter les lignes (= nombre d’IDs).
#   - Fusionner ces données dans les fichiers finaux du projet via
#     un script Python dédié (merge_tmdb_into_final.py).
#   - Enregistrer les logs d’exécution et garder la date du dernier
#     succès pour éviter de relancer plusieurs fois dans la même journée.
#
# Pourquoi :
#   Ce script sert de point d’entrée unique pour maintenir à jour
#   les bases locales de films et de séries. Il automatise le cycle
#   complet : téléchargement, intégration, nettoyage, suivi.
#
# Usage :
#   Lancer directement : ./fetch_dumps_daily.sh
#   → Les données mises à jour sont écrites dans data/out/
#   → Les logs sont écrits dans logs/fetch_ids.log
#   → La date du dernier succès est dans state/last_success_date.txt
#
# Bénéfice :
#   Une seule commande garde les datasets films/séries toujours à jour,
#   sans doublons, avec suivi clair des réussites/erreurs.
# ============================================================

#!/usr/bin/env bash
# Utilise Bash comme interpréteur et configure un mode strict d'exécution

set -euo pipefail
# -e  : stoppe le script si une commande retourne une erreur
# -u  : stoppe si une variable non initialisée est utilisée
# -o pipefail : stoppe si une commande dans un pipe échoue

# Définition des chemins de base
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"  # Dossier racine du projet (un niveau au-dessus du script)
IDS="$ROOT/data/ids"    # Dossier pour stocker les fichiers d'IDs bruts
OUT="$ROOT/data/out"    # Dossier pour stocker les fichiers finaux
STATE="$ROOT/state"     # Dossier pour stocker l'état (ex: dernière date traitée)
LOGS="$ROOT/logs"       # Dossier pour stocker les logs

# Création des dossiers si absents
mkdir -p "$IDS" "$OUT" "$STATE" "$LOGS"

# Variables de date UTC
STAMP="$(date -u +%F)"   # Date complète (AAAA-MM-JJ)
MM="$(date -u +%m)"      # Mois
DD="$(date -u +%d)"      # Jour
YYYY="$(date -u +%Y)"    # Année

# Chemins complets vers les fichiers d'entrée et sortie
MOV_GZ="$IDS/movie_ids_${STAMP}.json.gz"           # Fichier brut compressé films
TV_GZ="$IDS/tv_series_ids_${STAMP}.json.gz"        # Fichier brut compressé séries
MOV_OUT="$OUT/movie_dumps.json"                    # Fichier final JSON films
TV_OUT="$OUT/tv_series_dumps.json"                 # Fichier final JSON séries

# Fichiers d'état et de log
LAST_STAMP_FILE="$STATE/last_success_date.txt"     # Fichier contenant la dernière date de succès
LOG_FILE="$LOGS/fetch_ids.log"                     # Fichier de log

# Fonction de log avec horodatage en UTC
log() { echo "$(date -u +'%F %T') | $1" | tee -a "$LOG_FILE" >/dev/null; }

# Si le script a déjà été exécuté avec succès aujourd'hui, on quitte
if [[ -f "$LAST_STAMP_FILE" ]] && [[ "$(cat "$LAST_STAMP_FILE")" == "$STAMP" ]]; then
  log "SKIP already done for ${STAMP}"
  exit 0
fi

# Fonction pour télécharger un fichier avec gestion d'erreurs et retry
curl_get() { curl -fsSL --retry 5 --retry-delay 2 -o "$2" "$1"; }

# --- Traitement des films ---
if curl_get "https://files.tmdb.org/p/exports/movie_ids_${MM}_${DD}_${YYYY}.json.gz" "$MOV_GZ"; then
  TMP_MOV="$OUT/.tmp_movie_ids_${STAMP}.jsonl"                    # Fichier temporaire décompressé
  zcat "$MOV_GZ" > "$TMP_MOV"                                     # Décompression .gz en JSONL
  LINES_MOV=$(wc -l < "$TMP_MOV" | awk '{print $1}')               # Nombre de lignes (= nb d'IDs)
  python3 "$ROOT/scripts/merge_tmdb_into_final.py" movies "$TMP_MOV" "$MOV_OUT"  # Fusion dans le fichier final
  rm -f "$MOV_GZ" "$TMP_MOV"                                      # Nettoyage
  log "OK movies ${STAMP} merged=${LINES_MOV}"                    # Log succès
else
  log "ERROR movies ${STAMP} download failed"                     # Log erreur
fi

# --- Traitement des séries ---
if curl_get "https://files.tmdb.org/p/exports/tv_series_ids_${MM}_${DD}_${YYYY}.json.gz" "$TV_GZ"; then
  TMP_TV="$OUT/.tmp_tv_series_ids_${STAMP}.jsonl"                  # Fichier temporaire décompressé
  zcat "$TV_GZ" > "$TMP_TV"                                        # Décompression .gz en JSONL
  LINES_TV=$(wc -l < "$TMP_TV" | awk '{print $1}')                  # Nombre de lignes (= nb d'IDs)
  python3 "$ROOT/scripts/merge_tmdb_into_final.py" tv "$TMP_TV" "$TV_OUT"  # Fusion dans le fichier final
  rm -f "$TV_GZ" "$TMP_TV"                                         # Nettoyage
  log "OK tv_series ${STAMP} merged=${LINES_TV}"                   # Log succès
else
  log "ERROR tv_series ${STAMP} download failed"                   # Log erreur
fi

# --- Mise à jour de la date de dernier succès ---
# On l'écrit uniquement si au moins un téléchargement/merge a réussi aujourd'hui
if grep -q "OK .* ${STAMP}" "$LOG_FILE"; then
  echo "$STAMP" > "$LAST_STAMP_FILE"
fi
