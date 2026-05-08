#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Claude Satellite..."

export ANTHROPIC_API_KEY=$(bashio::config 'anthropic_api_key')
export OPTIONS_PATH="/data/options.json"

exec uvicorn main:app --host 0.0.0.0 --port 8089 --log-level info
