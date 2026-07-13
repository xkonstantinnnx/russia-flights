#!/usr/bin/env bash
# Поднимает dev container перед сессией Claude Code, использующей Playwright
# MCP (.mcp.json) для инспекции console/network на https://russia-flights.ru.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== Проверка Docker =="
if ! docker info >/dev/null 2>&1; then
  echo "Docker не отвечает. Если Docker Desktop в Resource Saver Mode — открой его окно вручную и повтори." >&2
  exit 1
fi
echo "Docker OK"

echo "== Поднимаю dev container (создаст, если не существует) =="
devcontainer up --workspace-folder "$PROJECT_DIR"

echo "== Готово. Можно запускать claude из $PROJECT_DIR =="
