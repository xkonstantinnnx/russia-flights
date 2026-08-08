#!/usr/bin/env python3
"""Генерирует STATS_DATA для страницы «Статистика сети» в index.html.

Источники: routes.json (routes/destinations/weights), VISA_REGIMES и
SEASON_DATA (уже встроены в index.html), network_trend.json
(scripts/gen_network_trend.py). Ничего не запрашивает по сети — считает
агрегаты по уже собранным данным.

Не часть автопайплайна update.yml, как и gen_visa_regimes.py/gen_season_data.py —
данные для статистики достаточно пересчитывать вручную после заметных
изменений routes.json. network_trend.json обновляется отдельным скриптом
перед этим (см. gen_network_trend.py) — здесь его состояние только читается.
"""
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"
TREND_PATH = ROOT / "network_trend.json"

FREQ_BUCKETS = [
    ("Ежедневно", lambda v: v >= 7),
    ("5–6 раз в неделю", lambda v: 5 <= v < 7),
    ("3–4 раза в неделю", lambda v: 3 <= v < 5),
    ("1–2 раза в неделю", lambda v: v < 3),
]

VISA_LABELS = {
    "free": "Без визы",
    "easy": "Виза по прилёту / электронная",
    "visa": "Нужна виза заранее",
}

MONTH_SHORT = ["янв", "фев", "мар", "апр", "май", "июн",
               "июл", "авг", "сен", "окт", "ноя", "дек"]
MONTH_FULL = ["января", "февраля", "марта", "апреля", "мая", "июня",
              "июля", "августа", "сентября", "октября", "ноября", "декабря"]


def extract_const(html: str, name: str) -> dict:
    m = re.search(rf"const {name} = (\{{.*?\}});", html, re.DOTALL)
    if not m:
        raise SystemExit(f"const {name} не найден в index.html")
    return json.loads(m.group(1))


def replace_between(text: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start_marker!r}/{end_marker!r} не найдены в index.html")
    replacement = f"{start_marker}\n{new_content}\n{end_marker}"
    return pattern.sub(lambda _m: replacement, text, count=1)


def build_badges(routes: dict, destinations: dict, edges: list) -> list:
    n_cities = len(routes)
    n_routes = len(edges)
    countries = {destinations[d]["c"] for _, d in edges if d in destinations}
    dest_cities = {d for _, d in edges}
    return [
        {"value": n_cities, "label": "ГОРОДОВ ВЫЛЕТА"},
        {"value": n_routes, "label": "МАРШРУТОВ"},
        {"value": len(countries), "label": "СТРАН"},
        {"value": len(dest_cities), "label": "ГОРОДОВ ПРИЛЁТА"},
        {"value": round(n_routes / n_cities, 1) if n_cities else 0, "label": "В СРЕДНЕМ НА ГОРОД"},
    ]


def build_top_cities(routes: dict) -> list:
    rows = [{"name": city, "value": len(dests)} for city, dests in routes.items()]
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


def build_top_dests(edges: list, destinations: dict, limit: int = 15) -> list:
    counts: dict[str, int] = {}
    for _, d in edges:
        counts[d] = counts.get(d, 0) + 1
    rows = []
    for d, n in counts.items():
        country = destinations.get(d, {}).get("c", "")
        label = f"{d} · {country}" if country else d
        rows.append({"name": label, "value": n})
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows[:limit]


def build_freq(edges: list, weights: dict) -> dict:
    dpw_values = []
    for o, d in edges:
        w = weights.get(f"{o}→{d}")
        if w and w.get("days_per_week") is not None:
            dpw_values.append(w["days_per_week"])
    buckets = [{"label": label, "value": sum(1 for v in dpw_values if test(v))}
               for label, test in FREQ_BUCKETS]
    return {"buckets": buckets, "covered": len(dpw_values), "total": len(edges)}


def build_regions(edges: list, weights: dict, destinations: dict) -> dict:
    by_region: dict[str, list] = {}
    for o, d in edges:
        w = weights.get(f"{o}→{d}")
        region = destinations.get(d, {}).get("r")
        if not w or w.get("duration_min") is None or not region:
            continue
        by_region.setdefault(region, []).append(w["duration_min"])

    rows = []
    all_values = []
    for region, values in by_region.items():
        values.sort()
        median = statistics.median(values)
        rows.append({
            "name": region,
            "min": values[0],
            "max": values[-1],
            "median": round(median),
            "n": len(values),
        })
        all_values.extend(values)
    rows.sort(key=lambda r: r["median"])

    return {
        "regions": rows,
        "global_min": min(all_values) if all_values else 0,
        "global_max": max(all_values) if all_values else 0,
    }


def build_visa(edges: list, destinations: dict, visa_regimes: dict) -> dict:
    counts = {k: 0 for k in VISA_LABELS}
    origin_cities = {k: set() for k in VISA_LABELS}
    classified = 0
    for o, d in edges:
        country = destinations.get(d, {}).get("c")
        regime = visa_regimes.get(country) if country else None
        if regime not in VISA_LABELS:
            continue
        counts[regime] += 1
        origin_cities[regime].add(o)
        classified += 1

    rows = []
    for key, label in VISA_LABELS.items():
        n = counts[key]
        rows.append({
            "key": key,
            "label": label,
            "value": n,
            "pct": round(n / classified * 100) if classified else 0,
            "cities": len(origin_cities[key]),
        })
    return {"rows": rows, "classified": classified, "total": len(edges)}


def build_seasonality(edges: list, season_data: dict) -> dict:
    considered = [d for _, d in edges if d in season_data]
    month_counts = []
    for i in range(12):
        n = sum(1 for d in considered if season_data[d][i])
        month_counts.append(n)
    return {
        "months": [{"label": MONTH_SHORT[i], "value": month_counts[i]} for i in range(12)],
        "covered": len(considered),
        "total": len(edges),
    }


def build_trend() -> dict:
    if not TREND_PATH.exists():
        return {"points": [], "delta_text": ""}
    points = json.loads(TREND_PATH.read_text(encoding="utf-8"))
    delta_text = ""
    if len(points) >= 2:
        first, last = points[0], points[-1]
        diff = last["routes"] - first["routes"]
        sign = "+" if diff >= 0 else ""
        y, m = first["month"].split("-")
        month_label = f"{MONTH_FULL[int(m) - 1]} {y}"
        delta_text = f"{sign}{diff} маршрутов с {month_label}"
    return {
        "points": [{"month": p["month"], "routes": p["routes"], "source": p["source"]} for p in points],
        "delta_text": delta_text,
    }


def build_js_block(stats: dict) -> str:
    lines = [
        "// Агрегаты для страницы статистики. Пересчитывается из routes.json,",
        "// VISA_REGIMES, SEASON_DATA и network_trend.json — см. scripts/gen_stats.py.",
        "const STATS_DATA = " + json.dumps(stats, ensure_ascii=False, indent=2) + ";",
    ]
    return "\n".join(lines)


def main() -> None:
    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    routes = routes_data["routes"]
    destinations = routes_data["destinations"]
    weights = routes_data.get("weights", {})

    html = INDEX_PATH.read_text(encoding="utf-8")
    visa_regimes = extract_const(html, "VISA_REGIMES")
    season_data = extract_const(html, "SEASON_DATA")

    edges = [(city, d) for city, dests in routes.items() for d in dests]

    stats = {
        "updated": routes_data.get("updated"),
        "badges": build_badges(routes, destinations, edges),
        "topCities": build_top_cities(routes),
        "topDests": build_top_dests(edges, destinations),
        "freq": build_freq(edges, weights),
        "durationByRegion": build_regions(edges, weights, destinations),
        "visa": build_visa(edges, destinations, visa_regimes),
        "seasonality": build_seasonality(edges, season_data),
        "trend": build_trend(),
    }

    html = replace_between(html, "// STATS_DATA_START", "// STATS_DATA_END", build_js_block(stats))
    INDEX_PATH.write_text(html, encoding="utf-8")

    print(f"STATS_DATA обновлён: {stats['badges'][1]['value']} маршрутов, "
          f"{stats['badges'][0]['value']} городов, "
          f"частота известна для {stats['freq']['covered']}/{stats['freq']['total']}, "
          f"длительность для {sum(r['n'] for r in stats['durationByRegion']['regions'])}/{len(edges)}",
          flush=True)


if __name__ == "__main__":
    main()
