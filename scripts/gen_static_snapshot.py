#!/usr/bin/env python3
"""Генерирует статичный текстовый снапшот routes.json для index.html.

Карта рендерится через fetch('routes.json') + Leaflet — краулеры, которые
не исполняют JS (GPTBot, ClaudeBot, PerplexityBot и т.п.), видят пустую
страницу. Этот скрипт кладёт те же данные обычным текстом в <details>
(секция #static-routes-snapshot в index.html) + JSON-LD, чтобы они были
видны без исполнения JS. Правки внутри блоков STATIC_*_START/END в
index.html затираются при следующем запуске — редактировать только
здесь или в routes.json.

Запускается вручную (`python scripts/gen_static_snapshot.py`) и из
GitHub Actions после update_routes.py.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"
SITEMAP_PATH = ROOT / "sitemap.xml"

CUR_YEAR = datetime.now(timezone.utc).year


def esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_fragment(data: dict) -> str:
    routes = data.get("routes", {})
    dests = data.get("destinations", {})
    updated = data.get("updated", "")
    updated_date = updated[:10] if updated else "—"
    n_cities = len(routes)
    n_routes = sum(len(v) for v in routes.values())

    parts = [
        f"<summary>📋 Список маршрутов текстом "
        f"({n_cities} городов, {n_routes} направлений, обновлено {esc(updated_date)})</summary>",
        '<div class="ssr-inner">',
        "<p>Прямые международные рейсы без пересадок из городов России. "
        "Данные обновляются автоматически из AirLabs, AeroDataBox и "
        f"Яндекс.Расписаний — актуально на {esc(updated_date)}.</p>",
    ]

    for city, dst_list in routes.items():
        if not dst_list:
            continue
        items = []
        for d in sorted(dst_list):
            country = dests.get(d, {}).get("c")
            items.append(f"{esc(d)} ({esc(country)})" if country else esc(d))
        parts.append(f"<h3>Из города {esc(city)}</h3>")
        parts.append(f"<p>{', '.join(items)}.</p>")

    parts.append("<h3>Частые вопросы</h3>")
    parts.append(
        f"<p><b>Куда можно улететь из России без пересадки в {CUR_YEAR} году?</b><br>"
        "Полный список направлений и городов вылета — выше на этой странице, "
        "данные обновляются автоматически несколько раз в месяц.</p>"
    )
    parts.append(
        "<p><b>Как часто обновляются данные о прямых рейсах?</b><br>"
        "Автоматически, из нескольких независимых источников "
        "(AirLabs, AeroDataBox, Яндекс.Расписания), без ручного вмешательства.</p>"
    )
    parts.append("</div>")
    return "\n".join(parts)


def build_jsonld(data: dict) -> str:
    routes = data.get("routes", {})
    updated = data.get("updated", "")
    n_cities = len(routes)
    n_routes = sum(len(v) for v in routes.values())
    obj = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": "Прямые международные рейсы из городов России",
        "description": (
            f"Список прямых международных рейсов без пересадок из {n_cities} "
            f"городов России, {n_routes} маршрутов. Обновляется автоматически "
            "из AirLabs, AeroDataBox и Яндекс.Расписаний."
        ),
        "url": "https://russia-flights.ru/",
        "inLanguage": "ru",
        "creator": {"@type": "Organization", "name": "russia-flights.ru", "url": "https://russia-flights.ru/"},
        "isAccessibleForFree": True,
    }
    if updated:
        obj["dateModified"] = updated
    return json.dumps(obj, ensure_ascii=False, indent=2)


def replace_between(text: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start_marker!r}/{end_marker!r} не найдены в index.html")
    replacement = f"{start_marker}\n{new_content}\n{end_marker}"
    return pattern.sub(lambda _m: replacement, text, count=1)


def build_sitemap(updated: str) -> str:
    lastmod = updated[:10] if updated else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://russia-flights.ru/</loc>
    <lastmod>{lastmod}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>
"""


def main():
    data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    html = INDEX_PATH.read_text(encoding="utf-8")

    html = replace_between(
        html, "<!-- STATIC_SNAPSHOT_START -->", "<!-- STATIC_SNAPSHOT_END -->", build_fragment(data)
    )

    jsonld_block = f'<script type="application/ld+json">\n{build_jsonld(data)}\n</script>'
    html = replace_between(html, "<!-- STATIC_JSONLD_START -->", "<!-- STATIC_JSONLD_END -->", jsonld_block)

    INDEX_PATH.write_text(html, encoding="utf-8")
    SITEMAP_PATH.write_text(build_sitemap(data.get("updated", "")), encoding="utf-8")
    print(f"OK: index.html и sitemap.xml обновлены ({len(data.get('routes', {}))} городов)", file=sys.stderr)


if __name__ == "__main__":
    main()
