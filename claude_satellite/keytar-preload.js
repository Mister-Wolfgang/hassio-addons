'use strict';
// Préchargé via NODE_OPTIONS=--require pour intercepter keytar
// avant que le binaire natif (libsecret/D-Bus) ne soit chargé.
const Module = require('module');
const originalLoad = Module._load;
Module._load = function(request, parent, isMain) {
  if (request === 'keytar' ||
      (typeof request === 'string' && request.endsWith('/keytar')) ||
      (typeof request === 'string' && request.includes('/keytar/'))) {
    return require('/app/keytar-file.js');
  }
  return originalLoad.apply(this, arguments);
};
