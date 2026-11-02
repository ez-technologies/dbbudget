#!/usr/bin/env bash
# Starts the local servers and emulators the store tests run against (no cloud account, no credentials), and prints
# the environment the tests read. Usage:
#   eval "$(scripts/test-env.sh up)"   # start them and export the variables
#   scripts/test-env.sh down           # remove them
set -euo pipefail
P=dbbudget-test
case "${1:-up}" in
up)
  run() { docker start "$P-$1" >/dev/null 2>&1 || docker run -d --name "$P-$1" "${@:2}" >/dev/null; }
  run nats -p 127.0.0.1:4241:4222 nats:2.15.2-alpine -js
  run valkey -p 127.0.0.1:6392:6379 valkey/valkey:8.1-alpine
  run dynamodb -p 127.0.0.1:8002:8000 amazon/dynamodb-local:2.6.1 -jar DynamoDBLocal.jar -inMemory
  run firestore -p 127.0.0.1:8692:8692 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
    gcloud beta emulators firestore start --host-port=0.0.0.0:8692 --project=dbbudget-test
  run postgres -p 127.0.0.1:5552:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_DB=dbbudget postgres:16-alpine
  for i in $(seq 1 60); do
    curl -sf http://127.0.0.1:8692 >/dev/null 2>&1 && docker exec $P-postgres pg_isready -q -U postgres 2>/dev/null && break
    sleep 1
  done
  cat <<VARS
export DBBUDGET_TEST_NATS_URL=nats://127.0.0.1:4241
export DBBUDGET_TEST_REDIS_ADDR=127.0.0.1:6392
export DBBUDGET_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:8002
export DBBUDGET_TEST_FIRESTORE_EMULATOR=127.0.0.1:8692
export DBBUDGET_TEST_POSTGRES_URL=postgres://postgres:test@127.0.0.1:5552/dbbudget?sslmode=disable
VARS
  ;;
down)
  docker rm -f $P-nats $P-valkey $P-dynamodb $P-firestore $P-postgres >/dev/null 2>&1 || true
  ;;
esac
