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

from _js_embed import json_for_script  # noqa: E402

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


def build_faq_entries(data: dict, top_n: int = 5) -> list[tuple[str, str]]:
    """Данные для FAQ считаются из routes.json — при изменении маршрутов
    вопросы про топ-направления автоматически подстраиваются под текущие
    данные, а не остаются висеть про то, чего уже нет."""
    routes = data.get("routes", {})
    dests = data.get("destinations", {})

    reverse: dict[str, list[str]] = {}
    for city, dst_list in routes.items():
        for d in dst_list:
            reverse.setdefault(d, []).append(city)
    top_dests = sorted(reverse.items(), key=lambda kv: -len(kv[1]))[:top_n]

    faqs = [
        (
            f"Куда можно улететь из России без пересадки в {CUR_YEAR} году?",
            "Полный список направлений и городов вылета — выше на этой странице, "
            "данные обновляются автоматически несколько раз в месяц.",
        ),
        (
            "Как часто обновляются данные о прямых рейсах?",
            "Автоматически, из нескольких независимых источников "
            "(AirLabs, AeroDataBox, Яндекс.Расписания), без ручного вмешательства.",
        ),
        (
            "Из каких городов России есть прямые международные рейсы?",
            ", ".join(sorted(routes.keys())) + ".",
        ),
    ]
    for dest, cities in top_dests:
        country = dests.get(dest, {}).get("c")
        dest_label = f"{dest} ({country})" if country else dest
        faqs.append(
            (
                f"Из каких городов России есть прямой рейс в {dest_label}?",
                ", ".join(sorted(cities)) + ".",
            )
        )
    return faqs


def build_fragment(data: dict, faqs: list[tuple[str, str]]) -> str:
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
    for q, a in faqs:
        parts.append(f"<p><b>{esc(q)}</b><br>{esc(a)}</p>")
    parts.append("</div>")
    return "\n".join(parts)


def build_jsonld(data: dict, faqs: list[tuple[str, str]]) -> str:
    routes = data.get("routes", {})
    updated = data.get("updated", "")
    n_cities = len(routes)
    n_routes = sum(len(v) for v in routes.values())

    dataset = {
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
        "license": "https://creativecommons.org/licenses/by/4.0/",
    }
    if updated:
        dataset["dateModified"] = updated

    faqpage = {
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {"@type": "Answer", "text": a},
            }
            for q, a in faqs
        ],
    }

    graph = {"@context": "https://schema.org", "@graph": [dataset, faqpage]}
    return json_for_script(graph, indent=2)


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
  <url>
    <loc>https://russia-flights.ru/privacy.html</loc>
    <changefreq>yearly</changefreq>
    <priority>0.1</priority>
  </url>
</urlset>
"""


def main():
    data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    html = INDEX_PATH.read_text(encoding="utf-8")
    faqs = build_faq_entries(data)

    html = replace_between(
        html, "<!-- STATIC_SNAPSHOT_START -->", "<!-- STATIC_SNAPSHOT_END -->", build_fragment(data, faqs)
    )

    jsonld_block = f'<script type="application/ld+json">\n{build_jsonld(data, faqs)}\n</script>'
    html = replace_between(html, "<!-- STATIC_JSONLD_START -->", "<!-- STATIC_JSONLD_END -->", jsonld_block)

    INDEX_PATH.write_text(html, encoding="utf-8")
    SITEMAP_PATH.write_text(build_sitemap(data.get("updated", "")), encoding="utf-8")
    print(f"OK: index.html и sitemap.xml обновлены ({len(data.get('routes', {}))} городов)", file=sys.stderr)


if __name__ == "__main__":
    main()
