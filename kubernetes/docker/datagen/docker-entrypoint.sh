#!/usr/bin/env sh

#perform any
export JDBC_URL="jdbc:splice://${DB_HOST}:1527/splicedb;user=${DB_USER};password=${DB_PWD};"

exec "$@"
