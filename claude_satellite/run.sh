#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Claude Satellite..."

# Les credentials Claude (login Max) sont stockés dans /data/.claude/
# qui persiste entre les redémarrages du container
export HOME=/data
export OPTIONS_PATH="/data/options.json"

# D-Bus session bus + gnome-keyring pour que keytar (Claude CLI) puisse stocker le token OAuth
# Le keyring est dans /data/.local/share/keyrings/ — persiste entre les redémarrages
mkdir -p /data/.local/share/keyrings
mkdir -p /run/dbus

# Démarrer le system bus si pas déjà là (peut échouer silencieusement)
dbus-daemon --system --fork 2>/dev/null || true

# Démarrer un session bus et exporter l'adresse
DBUS_SESSION_BUS_ADDRESS=$(dbus-daemon --session --fork --print-address 2>/dev/null)
if [ -n "$DBUS_SESSION_BUS_ADDRESS" ]; then
    export DBUS_SESSION_BUS_ADDRESS
    bashio::log.info "D-Bus session: $DBUS_SESSION_BUS_ADDRESS"
    # Démarrer gnome-keyring (secrets) avec unlock vide — données dans /data/
    echo -n "" | gnome-keyring-daemon \
        --unlock \
        --daemonize \
        --components=secrets \
        2>/dev/null || true
else
    bashio::log.warning "D-Bus non disponible — login Claude peut ne pas persister"
fi

exec uvicorn main:app --host 0.0.0.0 --port 8089 --log-level info
