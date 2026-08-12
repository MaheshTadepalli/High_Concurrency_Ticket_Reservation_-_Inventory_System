#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -q -r requirements.txt

docker compose up -d
echo "Waiting for Postgres..."
until docker compose exec -T postgres pg_isready -U ticket -d tickets >/dev/null 2>&1; do
  sleep 1
done

echo "Starting API on http://127.0.0.1:8001 ..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
