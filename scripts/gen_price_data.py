#!/usr/bin/env python3
"""Генерирует PRICE_DATA для index.html — диапазон цен по каждой паре
РФ-город→направление из routes.json → routes, через Travelpayouts Data API
(/v1/prices/cheap, тот же провайдер, что и партнёрские ссылки «Найти билеты»,
но отдельный токен — не affiliate marker).

Полный пересчёт на каждый прогон (в отличие от gen_season_data.py — климат
стабилен, а цены обязаны быть свежими). Пара, для которой запрос не удался
или для которой Travelpayouts не нашёл ни одной цены, просто не попадает в
PRICE_DATA в этом прогоне и будет предпринята заново через 2 недели — тот же
защитный принцип, что и у остального пайплайна: не писать заведомо
неверные/устаревшие данные вместо повтора.

IATA-коды берутся не из routes.json (там их нет), а из уже существующих
рукописных таблиц в index.html (RU_AIRPORT_IATA/DEST_AIRPORT_IATA/CITY_IATA) —
единственного источника правды для кодов в проекте, который использует и
tpLink() для диплинков «Найти билеты». Это гарантирует, что цены считаются
по тем же городам/аэропортам, что и сама кнопка бронирования.

Без TRAVELPAYOUTS_TOKEN в окружении скрипт молча завершается, не трогая
index.html — это ожидаемо на первых прогонах, пока пользователь не добавил
секрет в GitHub Actions.

Запуск: python3 scripts/gen_price_data.py [--limit N]
"""
import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

PRICES_URL = "https://api.travelpayouts.com/v1/prices/cheap"
CURRENCY = "rub"

REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 4
RETRY_START_WAIT = 5
RETRY_CAP_WAIT = 40
PAUSE_BETWEEN_REQUESTS = 0.3  # официальный rate limit не задокументирован — самоограничение


def extract_js_map(html: str, name: str) -> dict:
    """Достаёт из index.html объектный литерал вида `const NAME = {...};`.

    Как extract_js_map в gen_route_details.py, но без требования переноса
    строки перед `};` — CITY_IATA записан в одну строку, в отличие от
    RU_AIRPORT_IATA/DEST_AIRPORT_IATA.
    """
    m = re.search(rf"const {name} = \{{(.*?)\}};", html, re.DOTALL)
    if not m:
        raise SystemExit(f"const {name} не найден в index.html")
    return dict(re.findall(r'"([^"]+)"\s*:\s*"([^"]+)"', m.group(1)))


def _read_json(r) -> dict:
    raw = r.read()
    if r.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def fetch_cheap_prices(origin: str, destination: str, token: str) -> list[float]:
    params = {"origin": origin, "destination": destination, "currency": CURRENCY, "token": token}
    url = PRICES_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "russia-flights-price-script/1.0",
        "Accept-Encoding": "gzip, deflate",
    })
    wait = RETRY_START_WAIT
    last_err = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
                payload = _read_json(r)
            break
        except urllib.error.HTTPError as e:
            # 4xx кроме 429 (rate limit) — постоянная ошибка на стороне запроса
            # (неизвестный/нелётный IATA-код и т.п., см. живые тесты: FRU/KVD),
            # а не временный сбой. Ретраить бессмысленно — сразу пропускаем пару.
            if e.code != 429 and e.code < 500:
                try:
                    msg = json.loads(e.read().decode("utf-8")).get("error", str(e))
                except Exception:
                    msg = str(e)
                print(f"    {origin}->{destination}: {msg}", file=sys.stderr)
                return []
            last_err = e
            print(f"    retry {attempt+1}/{RETRY_ATTEMPTS} in {wait}s ({e})", file=sys.stderr)
            time.sleep(wait)
            wait = min(wait * 2, RETRY_CAP_WAIT)
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            print(f"    retry {attempt+1}/{RETRY_ATTEMPTS} in {wait}s ({e})", file=sys.stderr)
            time.sleep(wait)
            wait = min(wait * 2, RETRY_CAP_WAIT)
    else:
        raise RuntimeError(f"не удалось получить {origin}->{destination}: {last_err}")

    if not payload.get("success"):
        return []
    data = payload.get("data") or {}
    variants = data.get(destination)
    if variants is None and len(data) == 1:
        variants = next(iter(data.values()))
    if not variants:
        return []
    return [v["price"] for v in variants.values() if isinstance(v, dict) and "price" in v]


def js_block(price_data: dict) -> str:
    lines = [
        "// Диапазон цен по маршрутам (min/max среди закэшированных на момент",
        "// запуска цен: прямой рейс/1-2 пересадки, обычно 1-3 варианта).",
        "// Источник — Travelpayouts (/v1/prices/cheap), не поиск в реальном времени.",
        "// d: дата прогона (YYYY-MM-DD).",
        "// Пересчитывается полностью на каждый прогон пайплайна — см. scripts/gen_price_data.py.",
        "const PRICE_DATA = {",
    ]
    entries = []
    for pair, v in price_data.items():
        entries.append(
            f'  {json.dumps(pair, ensure_ascii=False)}: '
            f'{{"min":{v["min"]},"max":{v["max"]},"d":{json.dumps(v["d"])}}}'
        )
    lines.append(",\n".join(entries))
    lines.append("};")
    return "\n".join(lines)


def replace_between(text: str, start: str, end: str, content: str) -> str:
    pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start!r}/{end!r} не найдены в index.html")
    return pattern.sub(lambda _m: f"{start}\n{content}\n{end}", text, count=1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="обработать только первые N пар (для ручной проверки)")
    args = ap.parse_args()

    token = os.environ.get("TRAVELPAYOUTS_TOKEN")
    if not token:
        print("TRAVELPAYOUTS_TOKEN не задан — пропуск, index.html не трогаем", file=sys.stderr)
        return

    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    html = INDEX_PATH.read_text(encoding="utf-8")
    ru_iata = extract_js_map(html, "RU_AIRPORT_IATA")
    dest_iata = extract_js_map(html, "DEST_AIRPORT_IATA")
    city_iata = extract_js_map(html, "CITY_IATA")

    # Источник пар — routes (реальный состав маршрутов на карте), а не weights:
    # weights заполняют только AirLabs/OpenSky/Jonty (см. update_routes.py),
    # маршруты от AeroDataBox и Яндекс.Расписаний (в т.ч. вручную добавленные,
    # например Занзибар) в weights не попадают, хотя реально существуют.
    pairs = [f"{city}→{dest}" for city, dests in routes_data["routes"].items() for dest in dests]
    if args.limit:
        pairs = pairs[:args.limit]
    total = len(pairs)

    price_data = {}
    today = date.today().isoformat()
    for i, pair in enumerate(pairs, 1):
        city, dest = pair.split("→", 1)
        o = city_iata.get(city) or ru_iata.get(city)
        d = city_iata.get(dest) or dest_iata.get(dest)
        if not o or not d:
            print(f"[{i}/{total}] {pair:40s} -> нет IATA-кода, пропуск", file=sys.stderr)
            continue
        try:
            prices = fetch_cheap_prices(o, d, token)
        except Exception as e:
            print(f"[{i}/{total}] {pair:40s} -> ОШИБКА ({e}), пропуск (повторится в следующем запуске)",
                  file=sys.stderr)
            time.sleep(PAUSE_BETWEEN_REQUESTS)
            continue
        if not prices:
            print(f"[{i}/{total}] {pair:40s} -> нет цен в кэше", file=sys.stderr)
        else:
            price_data[pair] = {"min": min(prices), "max": max(prices), "d": today}
            print(f"[{i}/{total}] {pair:40s} -> {min(prices)}-{max(prices)} ({len(prices)})", file=sys.stderr)
        time.sleep(PAUSE_BETWEEN_REQUESTS)

    html = replace_between(html, "// PRICE_DATA_START", "// PRICE_DATA_END", js_block(price_data))
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"\nOK: index.html обновлён, PRICE_DATA {len(price_data)}/{total} пар", file=sys.stderr)


if __name__ == "__main__":
    main()
