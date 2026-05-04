#!/usr/bin/with-contenv bashio

bashio::log.info "Starting 2way Audio Arenti..."

export ARENTI_USER=$(bashio::config 'arenti_email')
export ARENTI_PASS=$(bashio::config 'arenti_password')
export ARENTI_VOLUME=$(bashio::config 'volume')
export OPTIONS_PATH="/data/options.json"

exec uvicorn main:app --host 0.0.0.0 --port 8080 --log-level info
