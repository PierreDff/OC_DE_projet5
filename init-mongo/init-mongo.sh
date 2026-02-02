#!/bin/bash
set -e

mongosh <<EOF
use admin
try {
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
