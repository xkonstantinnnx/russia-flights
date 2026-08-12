#!/usr/bin/env python3
"""Генерирует SEASON_DATA для index.html — климатический комфорт по месяцам
для каждого направления из routes.json → destinations, в трёх категориях
(не сезон / экскурсионный / пляжный), с наложенными температурами.

Источники — Open-Meteo Historical Weather API (archive-api.open-meteo.com)
и Marine API (marine-api.open-meteo.com), бесплатно, без ключа. Для каждого
направления (координаты la/lo из routes.json) берутся ~5 лет дневных
temperature_2m_max/precipitation_sum/cloud_cover_mean, усредняются по месяцам.
Для направлений у моря дополнительно ищется ближайшая точка с покрытием
Marine API (сеткой смещений — прибрежная линия не всегда совпадает с
исходными координатами города) и берётся sea_surface_temperature_mean.

Категория месяца:
  не сезон       — средний дневной максимум t > 34°C или < 5°C, ИЛИ
                    одновременно много осадков (>100мм/мес) и плотная
                    облачность (>76%) — короткий ливень при в основном
                    ясном небе комфорту не мешает, а вот затяжная морось
                    под сплошными тучами — да;
  пляжный сезон  — направление у моря, море теплее 20°C, и то же самое
                    условие "не сезон" не сработало, ЗА ИСКЛЮЧЕНИЕМ верхнего
                    порога температуры — для пляжного статуса он мягче,
                    38°C, а не 34°C (жара сама по себе не мешает пляжному
                    отдыху, если рядом есть где охладиться);
  экскурсионный  — всё остальное: климатически комфортно, но не пляж
                    (либо не у моря, либо море ещё/уже холодное).

Пороги откалиброваны в ходе анализа (см. переписку и research/ в рабочей
директории на момент разработки): подтверждены на Мале, полном прогоне по
82 направлениям и точечной проверке на здравый смысл.

MANUAL_EXCEPTIONS — точечные переопределения там, где формула расходится со
здравым смыслом (см. каждую запись — обоснование в комментарии):
  - Пхукет, Хошимин — настоящий тропический муссон: сильный дождь идёт при
    стабильно плотной облачности много часов подряд (в отличие от Мале, где
    ливень короткий при в основном ясном небе) — гибрид "осадки И
    облачность" здесь не спасает и почти повторяет чистый порог осадков,
    хотя оба направления остаются посещаемыми круглый год;
  - Хошимин дополнительно никогда не "пляж" — найденная Marine API точка
    у координат города это дельта Меконга, а не курортный пляж;
  - Баку — Каспийское море вне покрытия Marine API (проверено на нескольких
    точках по всему морю), поэтому автоматически там пляжа никогда не будет;
    добавлен пляжный сезон летом по общим климатическим знаниям об
    Апшеронском полуострове (НЕ измеренные данные), температура воды —
    climatology-оценка на все 12 месяцев, а не только на пляжные;
  - Пхеньян — формально найденная точка воды в ~40км от города (устье
    Тэдонган/порт Нампхо) нерелевантна причине поездки, пляжный статус
    убран полностью;
  - Даламан, июль — 38.14°C, на 0.14° выше BEACH_HOT=38, а это пик пляжного
    сезона турецкого Эгейского побережья. Порог специально не подняли: в
    диапазоне 38.0-38.2°C одновременно с Даламаном попадают настоящие пики
    аравийской жары (Маскат июль 38.09°C, Манама июнь 38.16°C, Абу-Даби
    июнь 38.12°C), которые обязаны остаться "не сезон" — единого порога,
    разделяющего эти два климата, не существует, поэтому точечное
    исключение одного месяца чище, чем сдвиг общего порога.

Часть автопайплайна (update.yml), но по умолчанию считает ТОЛЬКО направления,
которых ещё нет в SEASON_DATA в index.html (сверяется по имени) — климат
стабилен год к году, пересчитывать существующие направления на каждый прогон
незачем. Направление, для которого Open-Meteo не ответил, в SEASON_DATA не
попадает и будет повторено на следующем прогоне (та же защитная логика, что
у остального пайплайна — не писать фиктивные данные вместо повтора).

Флаг --all форсирует полный пересчёт всех направлений — для ручной
актуализации (пороги, MANUAL_EXCEPTIONS, устаревшие климатические данные):
    python scripts/gen_season_data.py --all
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
YEARS_OF_HISTORY = 5
MARINE_HIST_START = "2023-01-15"  # раньше Marine API отдаёт null (проверено эмпирически)

HOT_THRESHOLD_C = 34
BEACH_HOT_THRESHOLD_C = 38
COLD_THRESHOLD_C = 5
WET_THRESHOLD_MM = 100
CLOUD_THRESHOLD_PCT = 76
SEA_THRESHOLD_C = 20

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 15
RETRY_START_WAIT = 20
RETRY_CAP_WAIT = 180

# сетка поиска ближайшей точки с покрытием Marine API от исходных координат
MARINE_OFFSETS = [(0, 0)] + [
    (dlat, dlon)
    for r in (0.05, 0.1, 0.15, 0.25, 0.4)
    for dlat, dlon in [(0, -r), (0, r), (-r, 0), (r, 0), (-r, -r), (-r, r), (r, -r), (r, r)]
]

# Ручные исключения — см. докстринг. Значения по месяцам (янв..дек), None = взять из формулы.
MANUAL_EXCEPTIONS = {
    "Пхукет": ["beach", "beach", "beach", "beach", "excursion", "excursion",
               "excursion", "excursion", "excursion", "excursion", "beach", "beach"],
    "Хошимин": ["excursion"] * 12,
    "Баку": ["excursion", "excursion", "excursion", "excursion", "excursion", "beach",
             "beach", "beach", "beach", "excursion", "excursion", "excursion"],
    "Пхеньян": ["none", "none", "excursion", "excursion", "excursion", "excursion",
                "excursion", "excursion", "excursion", "excursion", "excursion", "none"],
    "Даламан": [None, None, None, None, None, None,
                "beach", None, None, None, None, None],
}
# Баку — оценочная температура воды у Апшерона по климатическим нормам
# (НЕ измеренные данные, Marine API Каспий не покрывает). На все 12 месяцев —
# чтобы работало то же правило, что и у направлений с реальными данными Marine
# API: раз есть хоть один пляжный месяц, воду показываем весь год. Показываем
# как ориентир, не как точный факт.
BAKU_ESTIMATED_SEA_C = {
    1: 8, 2: 7, 3: 8, 4: 12, 5: 16, 6: 22,
    7: 26, 8: 27, 9: 25, 10: 20, 11: 16, 12: 11,
}


def http_get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "russia-flights-season-script/2.0"})
    wait = RETRY_START_WAIT
    last_err = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            print(f"    retry {attempt+1}/{RETRY_ATTEMPTS} in {wait}s ({e})", file=sys.stderr)
            time.sleep(wait)
            wait = min(int(wait * 1.6), RETRY_CAP_WAIT)
    raise RuntimeError(f"не удалось получить {url}: {last_err}")


def fetch_weather_months(lat: float, lon: float) -> dict:
    end = date.today() - timedelta(days=5)
    start = end.replace(year=end.year - YEARS_OF_HISTORY)
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "daily": "temperature_2m_max,precipitation_sum,cloud_cover_mean",
        "timezone": "auto",
    }
    data = http_get_json(WEATHER_URL + "?" + urllib.parse.urlencode(params))
    daily = data["daily"]
    tmax = defaultdict(list)
    precip_by_year_month = defaultdict(float)
    cloud = defaultdict(list)
    for t, tm, p, cl in zip(daily["time"], daily["temperature_2m_max"],
                             daily["precipitation_sum"], daily["cloud_cover_mean"]):
        y, m, _ = t.split("-")
        m = int(m)
        if tm is not None:
            tmax[m].append(tm)
        if p is not None:
            precip_by_year_month[(y, m)] += p
        if cl is not None:
            cloud[m].append(cl)
    precip_years = defaultdict(list)
    for (y, m), total in precip_by_year_month.items():
        precip_years[m].append(total)

    months = {}
    for m in range(1, 13):
        months[m] = {
            "t_max": sum(tmax[m]) / len(tmax[m]) if tmax[m] else None,
            "p_mm": sum(precip_years[m]) / len(precip_years[m]) if precip_years[m] else None,
            "cloud": sum(cloud[m]) / len(cloud[m]) if cloud[m] else None,
        }
    return months


def find_coastal_point(lat: float, lon: float):
    for dlat, dlon in MARINE_OFFSETS:
        tlat, tlon = lat + dlat, lon + dlon
        params = {"latitude": tlat, "longitude": tlon,
                   "start_date": "2025-08-01", "end_date": "2025-08-01",
                   "daily": "sea_surface_temperature_mean", "timezone": "auto"}
        try:
            data = http_get_json(MARINE_URL + "?" + urllib.parse.urlencode(params))
        except RuntimeError:
            continue
        vals = data.get("daily", {}).get("sea_surface_temperature_mean", [None])
        if vals and vals[0] is not None:
            return tlat, tlon
    return None


def fetch_sea_months(lat: float, lon: float):
    pt = find_coastal_point(lat, lon)
    if pt is None:
        return None
    tlat, tlon = pt
    params = {"latitude": tlat, "longitude": tlon,
              "start_date": MARINE_HIST_START, "end_date": date.today().isoformat(),
              "daily": "sea_surface_temperature_mean", "timezone": "auto"}
    data = http_get_json(MARINE_URL + "?" + urllib.parse.urlencode(params))
    daily = data.get("daily", {})
    by_month = defaultdict(list)
    for t, v in zip(daily.get("time", []), daily.get("sea_surface_temperature_mean", [])):
        if v is None:
            continue
        m = int(t.split("-")[1])
        by_month[m].append(v)
    return {m: (sum(vs) / len(vs)) for m, vs in by_month.items()}


def classify_months(weather: dict, sea: dict | None) -> list[dict]:
    """Возвращает список из 12 {c: 'none'|'excursion'|'beach', t: int, s: int|None}."""
    result = []
    for m in range(1, 13):
        w = weather[m]
        t_max = w["t_max"]
        if t_max is None:
            result.append({"c": "excursion", "t": None, "s": None})
            continue
        cold_bad = t_max < COLD_THRESHOLD_C
        excursion_hot_bad = t_max > HOT_THRESHOLD_C
        beach_hot_bad = t_max > BEACH_HOT_THRESHOLD_C
        p_mm, cloud = w["p_mm"], w["cloud"]
        rain_bad = p_mm is not None and p_mm > WET_THRESHOLD_MM and cloud is not None and cloud > CLOUD_THRESHOLD_PCT

        sea_val = sea.get(m) if sea else None
        beach_ok = (sea_val is not None and sea_val >= SEA_THRESHOLD_C
                    and not cold_bad and not beach_hot_bad and not rain_bad)
        excursion_ok = not cold_bad and not excursion_hot_bad and not rain_bad

        if beach_ok:
            cat = "beach"
        elif excursion_ok:
            cat = "excursion"
        else:
            cat = "none"
        result.append({"c": cat, "t": round(t_max), "s": round(sea_val) if sea_val is not None else None})
    return result


def apply_exceptions(name: str, months: list[dict]) -> list[dict]:
    override = MANUAL_EXCEPTIONS.get(name)
    if override is None:
        return months
    for i, cat in enumerate(override):
        if cat is None:
            continue
        months[i]["c"] = cat
    if name == "Баку":
        for i, sea_c in BAKU_ESTIMATED_SEA_C.items():
            months[i - 1]["s"] = sea_c
    if name == "Пхеньян":
        for month in months:
            month["s"] = None
    return months


def build_season_data(destinations: dict) -> dict:
    result = {}
    total = len(destinations)
    for i, (name, meta) in enumerate(destinations.items(), 1):
        la, lo = meta.get("la"), meta.get("lo")
        if la is None or lo is None:
            print(f"[{i}/{total}] {name:28s} -> нет координат, пропуск", file=sys.stderr)
            continue
        try:
            weather = fetch_weather_months(la, lo)
        except Exception as e:
            print(f"[{i}/{total}] {name:28s} -> ОШИБКА погоды ({e}), пропуск (повторится в следующем запуске)", file=sys.stderr)
            continue
        try:
            sea = fetch_sea_months(la, lo)
        except Exception as e:
            print(f"    ОШИБКА моря ({e}), без пляжного статуса", file=sys.stderr)
            sea = None

        months = classify_months(weather, sea)
        months = apply_exceptions(name, months)
        result[name] = months

        n_beach = sum(1 for x in months if x["c"] == "beach")
        n_exc = sum(1 for x in months if x["c"] == "excursion")
        n_none = sum(1 for x in months if x["c"] == "none")
        print(f"[{i}/{total}] {name:28s} -> пляж={n_beach} экскурсия={n_exc} не сезон={n_none}", file=sys.stderr)
    return result


def read_existing_season_data(html: str) -> dict:
    """SEASON_DATA — валидный JSON внутри JS-объявления, парсим прямо оттуда."""
    block_match = re.search(
        re.escape("// SEASON_DATA_START") + r"(.*?)" + re.escape("// SEASON_DATA_END"),
        html, re.DOTALL,
    )
    if not block_match:
        return {}
    obj_match = re.search(r"const SEASON_DATA = (\{.*\});", block_match.group(1), re.DOTALL)
    if not obj_match:
        return {}
    return json.loads(obj_match.group(1))


def replace_between(text: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start_marker!r}/{end_marker!r} не найдены в index.html")
    replacement = f"{start_marker}\n{new_content}\n{end_marker}"
    return pattern.sub(lambda _m: replacement, text, count=1)


def build_js_block(season_data: dict) -> str:
    def month_json(m):
        parts = [f'"c":"{m["c"][0]}"']
        parts.append(f'"t":{m["t"] if m["t"] is not None else "null"}')
        parts.append(f'"s":{m["s"] if m["s"] is not None else "null"}')
        return "{" + ",".join(parts) + "}"

    lines = [
        "// Климатический комфорт по месяцам для каждого направления, три категории.",
        "// Индекс массива 0 = январь ... 11 = декабрь.",
        '// c: "n" не сезон | "e" экскурсионный сезон | "b" пляжный сезон.',
        "// t: средний дневной максимум температуры воздуха, °C (округлено).",
        "// s: средняя температура воды, °C (округлено) — только для направлений у моря.",
        f"// Пороги: HOT={HOT_THRESHOLD_C}°C (экскурсия) / {BEACH_HOT_THRESHOLD_C}°C (пляж),",
        f"// COLD={COLD_THRESHOLD_C}°C, WET={WET_THRESHOLD_MM}мм, CLOUD={CLOUD_THRESHOLD_PCT}%, SEA={SEA_THRESHOLD_C}°C.",
        "// Источник — Open-Meteo Historical Weather + Marine API "
        f"({YEARS_OF_HISTORY} лет истории). Не автообновляется — см. scripts/gen_season_data.py.",
        "const SEASON_DATA = {",
    ]
    dest_lines = []
    for name, months in season_data.items():
        months_js = ",".join(month_json(m) for m in months)
        dest_lines.append(f'  {json.dumps(name, ensure_ascii=False)}: [{months_js}]')
    lines.append(",\n".join(dest_lines))
    lines.append("};")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true",
                         help="пересчитать все направления, а не только новые (актуализация)")
    args = parser.parse_args()

    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    destinations = routes_data["destinations"]

    html = INDEX_PATH.read_text(encoding="utf-8")
    existing = read_existing_season_data(html)

    if args.all:
        targets = destinations
    else:
        targets = {name: meta for name, meta in destinations.items() if name not in existing}

    if not targets:
        print("Новых направлений без сезонности нет — пропуск, 0 запросов к Open-Meteo", file=sys.stderr)
        return

    computed = build_season_data(targets)
    merged = {**existing, **computed}

    html = replace_between(html, "// SEASON_DATA_START", "// SEASON_DATA_END", build_js_block(merged))
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"\nOK: index.html обновлён, посчитано {len(computed)}/{len(targets)}, всего в SEASON_DATA {len(merged)}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
