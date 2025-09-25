# Image légère, prête pour Python 3.11
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Dépendances système minimales pour wheels fréquents
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
 && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Installer les dépendances Python
# Si requirements.txt n'existe pas, on évite d'invalider le cache
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip && \
    if [ -s /tmp/requirements.txt ]; then pip install -r /tmp/requirements.txt; fi

# User non-root
RUN useradd -ms /bin/bash appuser
USER appuser

# Préparer FastAPI plus tard
EXPOSE 8000

# Entrypoint sobre; la commande sera définie par docker compose
