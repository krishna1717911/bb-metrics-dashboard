#!/usr/bin/env bash
# Load .env and start the dashboard.
set -euo pipefail
cd "$(dirname "$0")"
[[ -f .env ]] || { echo "no .env - copy .env.example and fill it in"; exit 1; }
set -a; . ./.env; set +a
exec python3 app.py "$@"
