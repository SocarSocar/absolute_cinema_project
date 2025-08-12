#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IDS="$ROOT/data/ids"
OUT="$ROOT/data/out"
STATE="$ROOT/state"
LOGS="$ROOT/logs"

mkdir -p "$IDS" "$OUT" "$STATE" "$LOGS"

STAMP="$(date -u +%F)"   # 2025-08-08
MM="$(date -u +%m)"
DD="$(date -u +%d)"
YYYY="$(date -u +%Y)"

MOV_GZ="$IDS/movie_ids_${STAMP}.json.gz"
TV_GZ="$IDS/tv_series_ids_${STAMP}.json.gz"
MOV_OUT="$OUT/movie_dumps.json"
TV_OUT="$OUT/tv_series_dumps.json"

LAST_STAMP_FILE="$STATE/last_success_date.txt"
LOG_FILE="$LOGS/fetch_ids.log"

log() { echo "$(date -u +'%F %T') | $1" | tee -a "$LOG_FILE" >/dev/null; }

# skip si déjà fait aujourd'hui
if [[ -f "$LAST_STAMP_FILE" ]] && [[ "$(cat "$LAST_STAMP_FILE")" == "$STAMP" ]]; then
  log "SKIP already done for ${STAMP}"
  exit 0
fi

curl_get() { curl -fsSL --retry 5 --retry-delay 2 -o "$2" "$1"; }

# movies
if curl_get "https://files.tmdb.org/p/exports/movie_ids_${MM}_${DD}_${YYYY}.json.gz" "$MOV_GZ"; then
  TMP_MOV="$OUT/.tmp_movie_ids_${STAMP}.jsonl"
  zcat "$MOV_GZ" > "$TMP_MOV"
  LINES_MOV=$(wc -l < "$TMP_MOV" | awk '{print $1}')
  python3 "$ROOT/scripts/merge_tmdb_into_final.py" movies "$TMP_MOV" "$MOV_OUT"
  rm -f "$MOV_GZ" "$TMP_MOV"
  log "OK movies ${STAMP} merged=${LINES_MOV}"
else
  log "ERROR movies ${STAMP} download failed"
fi

# tv
if curl_get "https://files.tmdb.org/p/exports/tv_series_ids_${MM}_${DD}_${YYYY}.json.gz" "$TV_GZ"; then
  TMP_TV="$OUT/.tmp_tv_series_ids_${STAMP}.jsonl"
  zcat "$TV_GZ" > "$TMP_TV"
  LINES_TV=$(wc -l < "$TMP_TV" | awk '{print $1}')
  python3 "$ROOT/scripts/merge_tmdb_into_final.py" tv "$TMP_TV" "$TV_OUT"
  rm -f "$TV_GZ" "$TMP_TV"
  log "OK tv_series ${STAMP} merged=${LINES_TV}"
else
  log "ERROR tv_series ${STAMP} download failed"
fi

# maj dernière date si au moins un succès aujourd'hui
if grep -q "OK .* ${STAMP}" "$LOG_FILE"; then
  echo "$STAMP" > "$LAST_STAMP_FILE"
fi