#!/usr/bin/env python3
"""Генерирует SEASON_DATA для index.html — климатическая комфортность
каждого месяца по каждому направлению из routes.json → destinations.

Источник — Open-Meteo Historical Weather API (archive-api.open-meteo.com),
бесплатно, без ключа. Для каждого направления (используются координаты
la/lo, уже собранные в routes.json) берутся ~5 лет дневных
temperature_2m_max/precipitation_sum, усредняются по месяцам.

Критерий "не сезон" (климатический дискомфорт, а не туристическая
загруженность/цены): средний дневной максимум температуры выше 34°C или
ниже 5°C, ИЛИ средние осадки за месяц выше 100мм. Проверено вручную на
Дубае (июнь-сентябрь под 40°C) и Гоа (муссон июнь-сентябрь, 400-1200мм/мес)
— совпадает с реальностью.

Порог осадков (100мм) выбран не по climate-зонам Кёппена (там дискомфорт
не определяется — только «сухой месяц» < 60мм), а ближе к порогам
туристических индексов комфорта (Tourism Climate Index, Mieczkowski 1985):
там осадки от ~75-90мм/месяц уже оцениваются как неприемлемые для отдыха.
200мм (прежнее значение) ловил только выраженный муссон и не считал
дискомфортным просто затяжной дождливый месяц.

ВНИМАНИЕ: не часть автопайплайна (update.yml). Климат стабилен год к
году — запускать вручную раз в несколько месяцев/лет, при появлении
новых направлений в routes.json или просто для актуализации.
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
YEARS_OF_HISTORY = 5
HOT_THRESHOLD_C = 34
COLD_THRESHOLD_C = 5
WET_THRESHOLD_MM = 100
REQUEST_DELAY_SEC = 0.3  # вежливая пауза между запросами к бесплатному API


def fetch_climate(lat: float, lon: float) -> dict:
    end = date.today() - timedelta(days=5)  # свежие пара дней у архива могут быть недоступны
    start = end.replace(year=end.year - YEARS_OF_HISTORY)
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_max,precipitation_sum",
        "timezone": "auto",
    }
    url = OPEN_METEO_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "russia-flights-season-script/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def classify_months(daily: dict) -> list[bool]:
    """Возвращает список из 12 bool (индекс 0 = январь), True = комфортно."""
    temps = defaultdict(list)
    precip = defaultdict(list)
    for t, tmax, p in zip(daily["time"], daily["temperature_2m_max"], daily["precipitation_sum"]):
        if tmax is None or p is None:
            continue
        m = int(t.split("-")[1])
        temps[m].append(tmax)
        precip[m].append(p)

    result = []
    for m in range(1, 13):
        if not temps[m]:
            result.append(True)  # нет данных — не блокируем оптимистичным дефолтом
            continue
        avg_t = sum(temps[m]) / len(temps[m])
        avg_p_month = sum(precip[m]) / len(precip[m]) * 30  # среднесуточные -> примерно за месяц
        bad = avg_t > HOT_THRESHOLD_C or avg_t < COLD_THRESHOLD_C or avg_p_month > WET_THRESHOLD_MM
        result.append(not bad)
    return result


def build_season_data(destinations: dict) -> dict:
    result = {}
    total = len(destinations)
    for i, (name, meta) in enumerate(destinations.items(), 1):
        la, lo = meta.get("la"), meta.get("lo")
        if la is None or lo is None:
            print(f"[{i}/{total}] {name:28s} -> нет координат, пропуск", file=sys.stderr)
            continue
        try:
            data = fetch_climate(la, lo)
            months = classify_months(data["daily"])
        except Exception as e:
            print(f"[{i}/{total}] {name:28s} -> ОШИБКА ({e}), все месяцы = комфортно", file=sys.stderr)
            months = [True] * 12
        result[name] = months
        n_bad = months.count(False)
        marker = f"не сезон: {n_bad} мес." if n_bad else "круглый год комфортно"
        print(f"[{i}/{total}] {name:28s} -> {marker} {months}", file=sys.stderr)
        time.sleep(REQUEST_DELAY_SEC)
    return result


def replace_between(text: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start_marker!r}/{end_marker!r} не найдены в index.html")
    replacement = f"{start_marker}\n{new_content}\n{end_marker}"
    return pattern.sub(lambda _m: replacement, text, count=1)


def build_js_block(season_data: dict) -> str:
    lines = [
        "// Климатическая комфортность по месяцам для каждого направления.",
        "// Индекс массива 0 = январь ... 11 = декабрь. true = комфортно, false = не сезон.",
        f"// Критерий: сред. дневной максимум t > {HOT_THRESHOLD_C}°C или < {COLD_THRESHOLD_C}°C,",
        f"// либо сред. осадки за месяц > {WET_THRESHOLD_MM}мм (муссон). Источник — Open-Meteo",
        f"// Historical Weather API ({YEARS_OF_HISTORY} лет истории). Не автообновляется —",
        "// см. scripts/gen_season_data.py.",
        "const SEASON_DATA = " + json.dumps(season_data, ensure_ascii=False, indent=2) + ";",
    ]
    return "\n".join(lines)


def main():
    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    destinations = routes_data["destinations"]

    season_data = build_season_data(destinations)
    html = INDEX_PATH.read_text(encoding="utf-8")
    html = replace_between(html, "// SEASON_DATA_START", "// SEASON_DATA_END", build_js_block(season_data))
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"\nOK: index.html обновлён, {len(season_data)} направлений", file=sys.stderr)


if __name__ == "__main__":
    main()
