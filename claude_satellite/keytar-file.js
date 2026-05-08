'use strict';
// File-based keytar mock — remplace le native keytar (libsecret/D-Bus)
// pour les environnements container sans keychain OS.
// Stocke les credentials dans $HOME/.claude-keytar.json (volume /data/).
const fs = require('fs');
const path = require('path');

const storePath = path.join(process.env.HOME || '/data', '.claude-keytar.json');

function read() {
  try { return JSON.parse(fs.readFileSync(storePath, 'utf8')); }
  catch { return {}; }
}

function write(store) {
  fs.writeFileSync(storePath, JSON.stringify(store), 'utf8');
}

function key(service, account) { return service + '\x00' + account; }

module.exports = {
  getPassword: (service, account) =>
    Promise.resolve(read()[key(service, account)] || null),

  setPassword: (service, account, password) => {
    const s = read(); s[key(service, account)] = password; write(s);
    return Promise.resolve();
  },

  deletePassword: (service, account) => {
    const s = read(); const k = key(service, account);
    const existed = k in s; delete s[k]; write(s);
    return Promise.resolve(existed);
  },

  findCredentials: (service) => {
    const prefix = service + '\x00';
    return Promise.resolve(
      Object.entries(read())
        .filter(([k]) => k.startsWith(prefix))
        .map(([k, v]) => ({ account: k.slice(prefix.length), password: v }))
    );
  },

  findPassword: (service) => {
    const prefix = service + '\x00';
    const entry = Object.entries(read()).find(([k]) => k.startsWith(prefix));
    return Promise.resolve(entry ? entry[1] : null);
  },
};
