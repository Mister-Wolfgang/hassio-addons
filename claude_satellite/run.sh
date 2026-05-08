#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Claude Satellite..."

# Les credentials Claude (login Max) sont stockés dans /data/.claude/
# qui persiste entre les redémarrages du container
export HOME=/data
export OPTIONS_PATH="/data/options.json"

# Remplace keytar (libsecret/D-Bus natif) par une implémentation fichier
# pour que le token OAuth Claude persiste dans /data/.claude-keytar.json
export NODE_OPTIONS="--require /app/keytar-preload.js"

exec uvicorn main:app --host 0.0.0.0 --port 8089 --log-level info
