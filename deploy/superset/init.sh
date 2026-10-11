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
