#!/bin/sh
set -eu

source_path="${GOOGLE_SERVICE_ACCOUNT_JSON_FILE:-/run/host-secrets/google-service-account.json}"
target_path="/run/topus-secrets/google-service-account.json"

install -o topus -g topus -m 0400 "$source_path" "$target_path"
chown topus:topus /data
chmod 0700 /data
export GOOGLE_SERVICE_ACCOUNT_JSON_FILE="$target_path"

exec su -s /bin/sh topus -c 'exec python /app/watchdog.py'
