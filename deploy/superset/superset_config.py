import os

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset-local@postgres:5432/superset",
)

_secret_key_path = os.environ.get("SUPERSET_SECRET_KEY_PATH", "/secrets/secret-key")
if not os.path.isfile(_secret_key_path):
    raise RuntimeError(f"Missing required secret key file: {_secret_key_path}")

with open(_secret_key_path, "r", encoding="utf-8") as _f:
    SECRET_KEY = _f.read().strip()

if not SECRET_KEY:
    raise RuntimeError(f"Secret key file {_secret_key_path} is empty")

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_THRESHOLD": 500,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_THRESHOLD": 500,
}

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": False,
}

RESULTS_BACKEND = None
SUPERSET_WEBSERVER_TIMEOUT = 60
