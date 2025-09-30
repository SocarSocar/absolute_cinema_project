#!/usr/bin/env bash
set -euo pipefail

# 1) se placer à la racine du repo
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

# 2) horodatage et logs
mkdir -p logs
TS="$(date -u +'%Y-%m-%dT%H-%M-%SZ')"
LOG_FILE="logs/full_update_${TS}.log"
LATEST_LOG="logs/full_update_latest.log"

# 3) vérifier .env présent
if [[ ! -f ".env" ]]; then
  echo "[ERROR] .env manquant à la racine du repo" | tee -a "${LOG_FILE}"
  exit 1
fi

# 4) pull éventuel des images + build si nécessaire (facultatif mais robuste)
echo "[INFO] $(date -u) docker compose pull/build" | tee -a "${LOG_FILE}"
docker compose pull || true
docker compose build | tee -a "${LOG_FILE}"

# 5) exécuter le job
echo "[INFO] $(date -u) run full_update" | tee -a "${LOG_FILE}"
set +e
docker compose run --rm full_update | tee -a "${LOG_FILE}"
CODE=${PIPESTATUS[0]}
set -e

# 6) exposer dernier log
ln -sf "$(basename "${LOG_FILE}")" "${LATEST_LOG}"

# 7) code retour
if [[ ${CODE} -ne 0 ]]; then
  echo "[ERROR] full_update exit code ${CODE}" | tee -a "${LOG_FILE}"
  exit ${CODE}
fi
echo "[OK] $(date -u) full_update terminé" | tee -a "${LOG_FILE}"
