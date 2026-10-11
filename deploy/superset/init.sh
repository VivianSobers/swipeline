#!/bin/sh
set -eu

SECRETS_DIR="${SUPERSET_SECRETS_DIR:-/secrets}"
mkdir -p "$SECRETS_DIR"

gen_secret() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c 'import secrets;print(secrets.token_urlsafe(36))'
    elif command -v python >/dev/null 2>&1; then
        python -c 'import secrets;print(secrets.token_urlsafe(36))'
    else
        head -c 36 /dev/urandom | base64 | tr -d '\n'
        echo
    fi
}

if [ ! -f "$SECRETS_DIR/secret-key" ]; then
    gen_secret > "$SECRETS_DIR/secret-key"
    chmod 0600 "$SECRETS_DIR/secret-key"
fi

if [ ! -f "$SECRETS_DIR/admin-password" ]; then
    gen_secret > "$SECRETS_DIR/admin-password"
    chmod 0600 "$SECRETS_DIR/admin-password"
fi

if [ "${SUPERSET_SECRETS_ONLY:-0}" = "1" ]; then
    exit 0
fi

ADMIN_PASS="$(cat "$SECRETS_DIR/admin-password")"

superset db upgrade

if ! superset fab list-users | grep -q 'admin'; then
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@swipeline.local \
        --password "$ADMIN_PASS"
fi

superset init

if [ -f /app/dashboards/payments-operations.zip ]; then
    superset import-dashboards -p /app/dashboards/payments-operations.zip -u admin
fi

exec gunicorn --workers 1 --threads 4 --timeout 60 -b 0.0.0.0:8088 "superset.app:create_app()"
