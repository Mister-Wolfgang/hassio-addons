#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Claude Satellite..."

# Les credentials Claude (login Max) sont stockés dans /data/.claude/
# qui persiste entre les redémarrages du container
export HOME=/data
export OPTIONS_PATH="/data/options.json"

exec uvicorn main:app --host 0.0.0.0 --port 8089 --log-level info
