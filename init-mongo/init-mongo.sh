#!/bin/bash
set -e

mongosh <<EOF
use admin
db.createUser({
  user: "$MONGO_INITDB_ROOT_USERNAME",
  pwd: "$MONGO_INITDB_ROOT_PASSWORD",
  roles: [{ role: "root", db: "admin" }]
})

use healthcare_db
db.createUser({
  user: "app_user",
  pwd: "app_password_secure",
  roles: [{ role: "readWrite", db: "healthcare_db" }]
})
EOF
