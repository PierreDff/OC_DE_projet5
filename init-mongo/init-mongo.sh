#!/bin/bash
set -e

# Script d'initialisation exécuté au premier démarrage du conteneur MongoDB.
# Il crée les utilisateurs et gère l'idempotence (ne crash pas si l'utilisateur existe déjà).

mongosh <<EOF
use admin
try {
  // Création de l'admin root
  db.createUser({
    user: "$MONGO_INITDB_ROOT_USERNAME",
    pwd: "$MONGO_INITDB_ROOT_PASSWORD",
    roles: [{ role: "root", db: "admin" }]
  })
} catch (e) {
  if (e.code !== 51003) { throw e; } // 51003 = User already exists
  print("L'utilisateur root existe déjà, on continue.");
}

use healthcare_db
try {
  // Création de l'utilisateur applicatif avec droits de lecture/écriture
  db.createUser({
    user: "$APP_USER",
    pwd: "$APP_PASSWORD",
    roles: [{ role: "readWrite", db: "healthcare_db" }]
  })
} catch (e) {
  if (e.code !== 51003) { throw e; }
  print("L'utilisateur applicatif existe déjà, on continue.");
}
EOF
