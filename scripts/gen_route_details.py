#!/usr/bin/env python3
"""Генерирует ROUTE_DETAILS / AIRLINE_NAMES / DEST_CC для index.html.

Что это. Карточка маршрута и строка списка в макете показывают авиакомпании,
аэропорт вылета и двухбуквенный код страны. Пайплайн этих полей не хранил:
update_routes.py считал перевозчиков (`carriers`), но выбрасывал сами коды,
а аэропорт вылета схлопывал в город. Пока накопятся данные из AirLabs,
источник — публичный снэпшот Jonty/airline-route-data (MIT, без ключа,
тот же, что используется пятым источником пайплайна).

Как это стыкуется с пайплайном. Скрипт ничего не пишет в routes.json —
только в index.html между маркерами ROUTE_DETAILS_START/END, как это делают
gen_visa_regimes.py и gen_season_data.py. Когда update_routes.py начнёт
класть в weights поля airlines/apts, фронтенд возьмёт их оттуда, а этот блок
останется запасным для маршрутов, которые AirLabs не покрыл.

Запуск: python3 scripts/gen_route_details.py [--jonty путь_к_снэпшоту]
Без аргумента снэпшот (~22 МБ) скачивается заново.
"""
import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

from _js_embed import json_for_script  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

JONTY_URL = "https://raw.githubusercontent.com/Jonty/airline-route-data/main/airline_routes.json"

# Русские названия для перевозчиков, которые у Jonty записаны латиницей.
# Ручная таблица: покрывает российских и соседних перевозчиков, у которых
# есть общеупотребимое русское имя. Остальные остаются как в источнике
# (Emirates, Turkish Airlines — их по-русски обычно и не пишут).
AIRLINE_RU = {
    "SU": "Аэрофлот",
    "FV": "Россия",
    "DP": "Победа",
    "S7": "S7 Airlines",
    "U6": "Уральские авиалинии",
    "UT": "ЮТэйр",
    "N4": "Nordwind",
    "ZF": "Azur Air",
    "A4": "Азимут",
    "5N": "Смартавиа",
    "WZ": "Red Wings",
    "IO": "ИрАэро",
    "HZ": "Аврора",
    "YC": "Ямал",
    "7R": "РусЛайн",
    "EO": "Икар",
    "RL": "Royal Flight",
    "6R": "АЛРОСА",
    "B2": "Белавиа",
    "HY": "Uzbekistan Airways",
    "KC": "Air Astana",
    "DV": "SCAT",
    "J2": "AZAL",
    "RJ": "Royal Jordanian",
    "OM": "MIAT Mongolian",
}


# Направления, где страна аэропорта по данным источника не совпадает с
# принятой в проекте (частично признанные территории). Двухбуквенный код
# для них не выводим — название страны в интерфейсе и так есть.
CC_SKIP = {"Сухум"}

# Ручные исключения для аэропорта вылета — маршруты, которых нет в снэпшоте
# Jonty (обычно новые/чартерные направления), но аэропорт вылета известен из
# независимого подтверждения (не выдумано). Перекрывает то, что нашёл бы
# автоматический разбор снэпшота, если бы там была запись.
MANUAL_APT_OVERRIDE = {
    # Air Tanzania, рейс из Внуково — подтверждено вручную 09.08.2026
    # (см. TODO.md, «Ручные маршруты не переживают пайплайн»), в снэпшоте
    # Jonty маршрута нет вовсе.
    "Москва→Занзибар": ["VKO"],
}


def load_jonty(path: str | None) -> dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    print(f"Скачиваем снэпшот Jonty (~22 МБ)...", file=sys.stderr, flush=True)
    with urllib.request.urlopen(JONTY_URL, timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))


def extract_js_map(html: str, name: str) -> dict:
    """Достаёт из index.html объектный литерал вида `const NAME = {...};`.

    Литералы в index.html написаны с висящей запятой и без кавычек у части
    ключей, поэтому json.loads к ним неприменим — разбираем регуляркой по
    парам "ключ":"значение", этого формата достаточно для IATA-таблиц.
    """
    m = re.search(rf"const {name} = \{{(.*?)\n\}};", html, re.DOTALL)
    if not m:
        raise SystemExit(f"const {name} не найден в index.html")
    return dict(re.findall(r'"([^"]+)"\s*:\s*"([^"]+)"', m.group(1)))


def origin_airports(city: str, ru_iata: dict, multi: dict) -> list[str]:
    """IATA-коды всех аэропортов города вылета (у Москвы их четыре)."""
    if city in multi:
        return multi[city]
    code = ru_iata.get(city)
    return [code] if code else []


def extract_multi_airport(html: str) -> dict[str, list[str]]:
    m = re.search(r"const RU_MULTI_AIRPORT = \{(.*?)\};", html, re.DOTALL)
    if not m:
        return {}
    out = {}
    for city, codes in re.findall(r'"([^"]+)"\s*:\s*\[([^\]]+)\]', m.group(1)):
        out[city] = re.findall(r'"([^"]+)"', codes)
    return out


def build(routes_data: dict, jonty: dict, html: str) -> tuple[dict, dict, dict, dict]:
    ru_iata = extract_js_map(html, "RU_AIRPORT_IATA")
    dest_iata = extract_js_map(html, "DEST_AIRPORT_IATA")
    multi = extract_multi_airport(html)

    details: dict[str, dict] = {}
    airlines_seen: dict[str, str] = {}
    dest_cc: dict[str, str] = {}
    stats = {"routes": 0, "with_airlines": 0, "with_apts": 0, "no_dest_iata": set()}

    for city, dests in routes_data["routes"].items():
        apts = origin_airports(city, ru_iata, multi)
        for dest in dests:
            stats["routes"] += 1
            d_iata = dest_iata.get(dest)
            if not d_iata:
                stats["no_dest_iata"].add(dest)
                continue

            # Код страны назначения берём из карточки самого аэропорта —
            # он есть у Jonty для всех 3900 аэропортов, даже если конкретный
            # маршрут в снэпшоте не найден.
            dest_entry = jonty.get(d_iata)
            if dest_entry and dest_entry.get("country_code") and dest not in CC_SKIP:
                dest_cc.setdefault(dest, dest_entry["country_code"])

            carriers: dict[str, str] = {}
            from_apts: list[str] = []
            for apt in apts:
                airport = jonty.get(apt)
                if not airport:
                    continue
                for route in airport.get("routes", []):
                    if route.get("iata") != d_iata:
                        continue
                    from_apts.append(apt)
                    for c in route.get("carriers") or []:
                        code, name = c.get("iata"), c.get("name")
                        if code and name:
                            carriers[code] = name
                    break

            if not carriers and not from_apts:
                continue
            entry: dict = {}
            if carriers:
                entry["al"] = sorted(carriers)
                airlines_seen.update(carriers)
                stats["with_airlines"] += 1
            if from_apts:
                entry["apt"] = from_apts
                stats["with_apts"] += 1
            details[f"{city}→{dest}"] = entry

    for key, apts in MANUAL_APT_OVERRIDE.items():
        if key not in details:
            stats["with_apts"] += 1
        details.setdefault(key, {})["apt"] = apts

    names = {code: AIRLINE_RU.get(code, name) for code, name in airlines_seen.items()}
    return details, names, dest_cc, stats


def js_block(details: dict, names: dict, dest_cc: dict) -> str:
    def dump(obj):
        return json_for_script(obj, sort_keys=True, separators=(",", ":"))

    return "\n".join([
        "// Источник: снэпшот Jonty/airline-route-data (MIT). Пересобирается",
        "// скриптом scripts/gen_route_details.py — вручную не править.",
        "// al — IATA-коды перевозчиков, apt — аэропорты вылета из города.",
        f"const ROUTE_DETAILS = {dump(details)};",
        f"const AIRLINE_NAMES = {dump(names)};",
        f"const DEST_CC = {dump(dest_cc)};",
    ])


def replace_between(text: str, start: str, end: str, content: str) -> str:
    pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start!r}/{end!r} не найдены в index.html")
    return pattern.sub(lambda _m: f"{start}\n{content}\n{end}", text, count=1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jonty", help="путь к локальной копии airline_routes.json")
    args = ap.parse_args()

    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    html = INDEX_PATH.read_text(encoding="utf-8")
    jonty = load_jonty(args.jonty)

    details, names, dest_cc, stats = build(routes_data, jonty, html)
    html = replace_between(html, "// ROUTE_DETAILS_START", "// ROUTE_DETAILS_END",
                           js_block(details, names, dest_cc))
    INDEX_PATH.write_text(html, encoding="utf-8")

    print(f"ROUTE_DETAILS: {len(details)} из {stats['routes']} маршрутов "
          f"(авиакомпании у {stats['with_airlines']}, аэропорты вылета у {stats['with_apts']}), "
          f"AIRLINE_NAMES: {len(names)}, DEST_CC: {len(dest_cc)}")
    if stats["no_dest_iata"]:
        print(f"  без IATA назначения ({len(stats['no_dest_iata'])}): "
              f"{', '.join(sorted(stats['no_dest_iata']))}")


if __name__ == "__main__":
    main()
