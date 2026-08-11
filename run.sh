#!/usr/bin/env bash
# Load .env and start the dashboard.
#
# Binds 0.0.0.0 so the page is reachable from other machines. The dashboard has
# no authentication and will serve whatever the configured credentials can read,
# so put it behind a private network (a tailnet interface, a VPN) rather than a
# public one. Override with BIND_HOST=127.0.0.1 ./run.sh, or --host on the
# command line, which wins over both.
set -euo pipefail
cd "$(dirname "$0")"
[[ -f .env ]] || { echo "no .env - copy .env.example and fill it in"; exit 1; }
set -a; . ./.env; set +a
exec python3 app.py --host "${BIND_HOST:-0.0.0.0}" "$@"
