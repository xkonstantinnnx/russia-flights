#!/usr/bin/env python3
"""Копит помесячную динамику сети (число маршрутов) для страницы статистики.

Источник истории — коммиты routes.json (git log). Своей чистой помесячной
статистики раньше не было, поэтому при первом запуске скрипт восстанавливает
грубую оценку по git-истории (март–август 2026), а дальше на каждый запуск
добавляет/обновляет запись за текущий календарный месяц из актуального
routes.json. Со временем реальные снапшоты вытеснят реконструированные точки
из окна последних 12 месяцев.

Хранилище — network_trend.json в корне репозитория (отдельно от routes.json,
чтобы не путать с данными самого пайплайна). Формат:
  [{"month": "2026-08", "routes": 492, "cities": 47, "source": "git-history"|"snapshot"}, ...]

Не часть автопайплайна update.yml — запускать вручную либо после явного
решения добавить шаг в workflow (см. CLAUDE.md: изменения CI подтверждаются
отдельно).
"""
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TREND_PATH = ROOT / "network_trend.json"
ROUTES_PATH = ROOT / "routes.json"

MAX_POINTS = 12
# Средние маршруты/город устойчиво держатся в районе 4-13 на всей истории;
# несколько коммитов конца марта-начала апреля 2026 показывают ~45/город —
# явный дефект тогдашнего источника данных (не реальный рост сети),
# отфильтровываем по этому порогу.
MAX_SANE_ROUTES_PER_CITY = 20


def route_counts(routes: dict) -> tuple[int, int]:
    n_cities = len(routes)
    n_routes = sum(len(v) for v in routes.values())
    return n_routes, n_cities


def git_history_points() -> list[dict]:
    """Восстанавливает по одной точке на календарный месяц из git-истории routes.json."""
    out = subprocess.run(
        ["git", "log", "--follow", "--format=%H|%ad", "--date=format:%Y-%m-%d", "--", "routes.json"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.strip()
    commits = [line.split("|") for line in out.splitlines()]
    commits.reverse()  # старые сначала

    by_month: dict[str, dict] = {}
    for commit_hash, day in commits:
        raw = subprocess.run(
            ["git", "show", f"{commit_hash}:routes.json"],
            cwd=ROOT, capture_output=True, text=True,
        )
        if raw.returncode != 0:
            continue
        try:
            data = json.loads(raw.stdout)
        except json.JSONDecodeError:
            continue
        routes = data.get("routes", {})
        n_routes, n_cities = route_counts(routes)
        if n_cities == 0:
            continue
        if n_routes / n_cities > MAX_SANE_ROUTES_PER_CITY:
            continue  # известный дефектный период, см. докстринг
        month = day[:7]
        # Берём последний валидный коммит месяца — перезапишет более ранний.
        by_month[month] = {"month": month, "routes": n_routes, "cities": n_cities, "source": "git-history"}

    return [by_month[m] for m in sorted(by_month)]


def current_point() -> dict:
    data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    n_routes, n_cities = route_counts(data.get("routes", {}))
    month = date.today().strftime("%Y-%m")
    return {"month": month, "routes": n_routes, "cities": n_cities, "source": "snapshot"}


def main() -> None:
    if TREND_PATH.exists():
        points = json.loads(TREND_PATH.read_text(encoding="utf-8"))
        print(f"network_trend.json уже существует, {len(points)} точек", flush=True)
    else:
        points = git_history_points()
        print(f"Восстановлено {len(points)} точек из git-истории", flush=True)

    new_point = current_point()
    points = [p for p in points if p["month"] != new_point["month"]]
    points.append(new_point)
    points.sort(key=lambda p: p["month"])
    points = points[-MAX_POINTS:]

    TREND_PATH.write_text(
        json.dumps(points, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Записано {len(points)} точек в {TREND_PATH.name}, "
          f"последняя: {points[-1]}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
