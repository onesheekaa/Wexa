#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "1/4  bringing up self-hosted platforms..."
docker compose up -d
echo "    waiting 30s for services to settle..."
sleep 30

echo "2/4  building dataset (skips if already downloaded)..."
python -m src.dataset

echo "3/4  running every platform..."
python -m src.runner --platform cognodb
python -m src.runner --platform neo4j
python -m src.runner --platform memgraph
python -m src.runner --platform arangodb
python -m src.runner --platform dgraph

echo "4/4  generating RESULTS.md..."
python -m src.report

echo "done - see RESULTS.md"
