#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Claude Satellite..."

export HOME=/data
export OPTIONS_PATH="/data/options.json"

# Démarrer D-Bus session bus (requis par libsecret/keytar pour stocker le token OAuth)
export DBUS_SESSION_BUS_ADDRESS=$(dbus-daemon --session --print-address --fork 2>/dev/null)
if [ -n "$DBUS_SESSION_BUS_ADDRESS" ]; then
    bashio::log.info "D-Bus démarré: ${DBUS_SESSION_BUS_ADDRESS}"
    # Démarrer gnome-keyring — stocke les credentials Claude dans /data/.local/share/keyrings/
    # (persistant entre les rebuilds)
    mkdir -p "${HOME}/.local/share/keyrings"
    eval $(gnome-keyring-daemon --unlock --components=secrets <<< "" 2>/dev/null)
    bashio::log.info "gnome-keyring-daemon démarré"
else
    bashio::log.warning "D-Bus indisponible — credentials Claude non persistés (re-login requis après rebuild)"
fi

exec uvicorn main:app --host 0.0.0.0 --port 8089 --log-level info
