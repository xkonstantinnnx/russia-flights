#!/usr/bin/env python3
"""
Обновление маршрутов из России через OpenSky Network API.
Запускается еженедельно через GitHub Actions.

Логика:
  - Запрашиваем вылеты из каждого российского аэропорта за последние 28 дней
  - Оставляем только международные рейсы (не в Россию)
  - Маршрут считается активным, если выполнен >= 2 раз за 28 дней
  - Результат сохраняется в routes.json
"""

import os, json, time, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

OPENSKY_USER = os.environ.get("OPENSKY_USER", "")
OPENSKY_PASS = os.environ.get("OPENSKY_PASS", "")

# ─── Российские аэропорты (ICAO → город для карты) ───────────────────────────
# Три московских аэропорта объединяем в "Москва"
RU_AIRPORTS = {
    "UUEE": "Москва",           # Шереметьево
    "UUDD": "Москва",           # Домодедово
    "UUWW": "Москва",           # Внуково
    "ULLI": "Санкт-Петербург",  # Пулково
    "URSS": "Сочи",             # Адлер
    "URKK": "Краснодар",        # Пашковский
    "URMM": "Мин. Воды",        # Минеральные Воды
    "USSS": "Екатеринбург",     # Кольцово
    "USCC": "Челябинск",        # Баландино
    "UWOO": "Оренбург",         # Центральный
    "UWUU": "Уфа",
    "UWKD": "Казань",
    "UWGG": "Нижний Новгород",  # Стригино
    "USPP": "Пермь",            # Большое Савино
    "UWWW": "Самара",           # Курумоч
    "URWW": "Волгоград",        # Гумрак
    "UWSS": "Саратов",          # Гагарин
    "USTR": "Тюмень",           # Рощино
    "UNOO": "Омск",             # Центральный
    "USRR": "Сургут",
    "USNN": "Нижневартовск",
    "UNNT": "Новосибирск",      # Толмачёво
    "UNKL": "Красноярск",       # Емельяново
    "UIII": "Иркутск",
    "UIUU": "Улан-Удэ",         # Мухино
    "UHHH": "Хабаровск",        # Новый
    "UHWW": "Владивосток",      # Кневичи
    "UHBB": "Благовещенск",     # Игнатьево
    "ULMM": "Мурманск",         # Мурмаши
    "UIAA": "Чита",             # Кадала
    "UHSS": "Южно-Сахалинск",   # Хомутово
}

# Префиксы ICAO российских аэропортов (для фильтрации внутренних рейсов)
RU_ICAO_PREFIXES = ("UU", "UI", "UH", "UK", "UL", "UM", "UN", "UR", "US", "UW")

# ─── Аэропорты назначений (ICAO → метаданные) ────────────────────────────────
DEST_INFO = {
    # СНГ
    "UMMS": {"n":"Минск",       "c":"Беларусь",       "la":53.9,"lo":27.6, "r":"СНГ"},
    "UDYZ": {"n":"Ереван",      "c":"Армения",        "la":40.2,"lo":44.5, "r":"СНГ"},
    "UGTB": {"n":"Тбилиси",     "c":"Грузия",         "la":41.7,"lo":44.8, "r":"СНГ"},
    "UGKO": {"n":"Батуми",      "c":"Грузия",         "la":41.6,"lo":41.6, "r":"СНГ"},
    "UBBB": {"n":"Баку",        "c":"Азербайджан",    "la":40.4,"lo":49.9, "r":"СНГ"},
    "UGSS": {"n":"Сухум",       "c":"Абхазия",        "la":43.0,"lo":41.0, "r":"СНГ"},
    "UTTT": {"n":"Ташкент",     "c":"Узбекистан",     "la":41.3,"lo":69.3, "r":"СНГ"},
    "UTSS": {"n":"Самарканд",   "c":"Узбекистан",     "la":39.7,"lo":67.0, "r":"СНГ"},
    "UCFM": {"n":"Бишкек",      "c":"Кыргызстан",     "la":42.9,"lo":74.6, "r":"СНГ"},
    "UAFO": {"n":"Ош",          "c":"Кыргызстан",     "la":40.5,"lo":72.8, "r":"СНГ"},
    "UTDD": {"n":"Душанбе",     "c":"Таджикистан",    "la":38.6,"lo":68.8, "r":"СНГ"},
    "UAAA": {"n":"Алматы",      "c":"Казахстан",      "la":43.3,"lo":77.0, "r":"СНГ"},
    "UACC": {"n":"Астана",      "c":"Казахстан",      "la":51.2,"lo":71.5, "r":"СНГ"},
    "UTAA": {"n":"Ашхабад",     "c":"Туркменистан",   "la":38.0,"lo":58.4, "r":"СНГ"},
    # Европа
    "LYBE": {"n":"Белград",     "c":"Сербия",         "la":44.8,"lo":20.5, "r":"Европа"},
    # Ближний Восток / Турция
    "LTFM": {"n":"Стамбул",     "c":"Турция",         "la":41.0,"lo":29.0, "r":"БВ"},
    "LTBA": {"n":"Стамбул",     "c":"Турция",         "la":41.0,"lo":29.0, "r":"БВ"},
    "LTAI": {"n":"Анталья",     "c":"Турция",         "la":36.9,"lo":30.7, "r":"БВ"},
    "LTBS": {"n":"Даламан",     "c":"Турция",         "la":36.8,"lo":28.8, "r":"БВ"},
    "LTBJ": {"n":"Измир",       "c":"Турция",         "la":38.3,"lo":27.2, "r":"БВ"},
    "OMDB": {"n":"Дубай",       "c":"ОАЭ",            "la":25.2,"lo":55.3, "r":"БВ"},
    "OMAA": {"n":"Абу-Даби",    "c":"ОАЭ",            "la":24.5,"lo":54.4, "r":"БВ"},
    "OTHH": {"n":"Доха",        "c":"Катар",          "la":25.3,"lo":51.5, "r":"БВ"},
    "OERK": {"n":"Эр-Рияд",     "c":"Саудовская Аравия","la":24.7,"lo":46.7,"r":"БВ"},
    "OEDF": {"n":"Даммам",      "c":"Саудовская Аравия","la":26.5,"lo":49.8,"r":"БВ"},
    "OOMS": {"n":"Маскат",      "c":"Оман",           "la":23.6,"lo":58.4, "r":"БВ"},
    "OOSA": {"n":"Салала",      "c":"Оман",           "la":17.0,"lo":54.1, "r":"БВ"},
    "OBBI": {"n":"Манама",      "c":"Бахрейн",        "la":26.2,"lo":50.6, "r":"БВ"},
    "OKBK": {"n":"Эль-Кувейт",  "c":"Кувейт",        "la":29.4,"lo":48.0, "r":"БВ"},
    "LLBG": {"n":"Тель-Авив",   "c":"Израиль",        "la":32.1,"lo":34.8, "r":"БВ"},
    "OJAM": {"n":"Амман",       "c":"Иордания",       "la":31.9,"lo":35.9, "r":"БВ"},
    "OIIE": {"n":"Тегеран",     "c":"Иран",           "la":35.7,"lo":51.4, "r":"БВ"},
    "ORBI": {"n":"Багдад",      "c":"Ирак",           "la":33.3,"lo":44.2, "r":"БВ"},
    # Азия
    "VTBS": {"n":"Бангкок",     "c":"Таиланд",        "la":13.8,"lo":100.5,"r":"Азия"},
    "VTBD": {"n":"Бангкок",     "c":"Таиланд",        "la":13.8,"lo":100.5,"r":"Азия"},
    "VTSP": {"n":"Пхукет",      "c":"Таиланд",        "la":7.9, "lo":98.4, "r":"Азия"},
    "VTSK": {"n":"Краби",       "c":"Таиланд",        "la":8.1, "lo":98.9, "r":"Азия"},
    "VVTS": {"n":"Хошимин",     "c":"Вьетнам",        "la":10.8,"lo":106.6,"r":"Азия"},
    "VVCR": {"n":"Нячанг",      "c":"Вьетнам",        "la":12.2,"lo":109.2,"r":"Азия"},
    "VVNB": {"n":"Ханой",       "c":"Вьетнам",        "la":21.0,"lo":105.8,"r":"Азия"},
    "VVDN": {"n":"Дананг",      "c":"Вьетнам",        "la":16.1,"lo":108.2,"r":"Азия"},
    "VVPQ": {"n":"Фукуок",      "c":"Вьетнам",        "la":10.2,"lo":104.0,"r":"Азия"},
    "WADD": {"n":"Денпасар",    "c":"Индонезия (Бали)","la":-8.7,"lo":115.2,"r":"Азия"},
    "RPLL": {"n":"Манила",      "c":"Филиппины",      "la":14.6,"lo":121.0,"r":"Азия"},
    "ZBAA": {"n":"Пекин",       "c":"Китай",          "la":39.9,"lo":116.4,"r":"Азия"},
    "ZBAD": {"n":"Пекин",       "c":"Китай",          "la":39.5,"lo":116.4,"r":"Азия"},
    "ZSPD": {"n":"Шанхай",      "c":"Китай",          "la":31.2,"lo":121.5,"r":"Азия"},
    "ZSSS": {"n":"Шанхай",      "c":"Китай",          "la":31.2,"lo":121.3,"r":"Азия"},
    "ZUUU": {"n":"Чэнду",       "c":"Китай",          "la":30.6,"lo":104.1,"r":"Азия"},
    "ZJSY": {"n":"Санья",       "c":"Китай",          "la":18.3,"lo":109.5,"r":"Азия"},
    "ZYHB": {"n":"Харбин",      "c":"Китай",          "la":45.8,"lo":126.6,"r":"Азия"},
    "ZYTL": {"n":"Далянь",      "c":"Китай",          "la":38.9,"lo":121.6,"r":"Азия"},
    "ZGGG": {"n":"Гуанчжоу",    "c":"Китай",          "la":23.4,"lo":113.3,"r":"Азия"},
    "ZGSZ": {"n":"Шэньчжэнь",   "c":"Китай",          "la":22.6,"lo":113.8,"r":"Азия"},
    "VHHH": {"n":"Гонконг",     "c":"Гонконг (КНР)",  "la":22.3,"lo":114.2,"r":"Азия"},
    "VMMC": {"n":"Макао",       "c":"Макао (КНР)",    "la":22.2,"lo":113.6,"r":"Азия"},
    "VIDP": {"n":"Дели",        "c":"Индия",          "la":28.6,"lo":77.2, "r":"Азия"},
    "VAGO": {"n":"Гоа",         "c":"Индия",          "la":15.3,"lo":73.9, "r":"Азия"},
    "VOMM": {"n":"Ченнаи",      "c":"Индия",          "la":13.0,"lo":80.2, "r":"Азия"},
    "VRMM": {"n":"Мале",        "c":"Мальдивы",       "la":4.2, "lo":73.5, "r":"Азия"},
    "VCBI": {"n":"Коломбо",     "c":"Шри-Ланка",      "la":6.9, "lo":79.9, "r":"Азия"},
    "ZMUB": {"n":"Улан-Батор",  "c":"Монголия",       "la":47.9,"lo":106.9,"r":"Азия"},
    "ZKPY": {"n":"Пхеньян",     "c":"КНДР",           "la":39.0,"lo":125.8,"r":"Азия"},
    "OAIX": {"n":"Кабул",       "c":"Афганистан",     "la":34.5,"lo":69.2, "r":"Азия"},
    "WMKK": {"n":"Куала-Лумпур","c":"Малайзия",       "la":2.7, "lo":101.7,"r":"Азия"},
    "WSSS": {"n":"Сингапур",    "c":"Сингапур",       "la":1.4, "lo":103.9,"r":"Азия"},
    # Африка
    "HECA": {"n":"Каир",           "c":"Египет",   "la":30.1,"lo":31.2, "r":"Африка"},
    "HEGN": {"n":"Хургада",        "c":"Египет",   "la":27.3,"lo":33.8, "r":"Африка"},
    "HESH": {"n":"Шарм-эш-Шейх",   "c":"Египет",   "la":27.9,"lo":34.3, "r":"Африка"},
    "HAAB": {"n":"Аддис-Абеба",    "c":"Эфиопия",  "la":9.0, "lo":38.7, "r":"Африка"},
    "DAAG": {"n":"Алжир",          "c":"Алжир",    "la":36.7,"lo":3.1,  "r":"Африка"},
    "GMMN": {"n":"Касабланка",     "c":"Марокко",  "la":33.6,"lo":-7.6, "r":"Африка"},
    "FSIA": {"n":"Маэ",            "c":"Сейшелы",  "la":-4.6,"lo":55.5, "r":"Африка"},
    # Латинская Америка
    "SVMI": {"n":"Каракас","c":"Венесуэла","la":10.5,"lo":-66.9,"r":"ЛА"},
    "MUHA": {"n":"Гавана", "c":"Куба",      "la":23.1,"lo":-82.4,"r":"ЛА"},
}


def get_departures(icao: str, begin_ts: int, end_ts: int, session: requests.Session) -> list:
    """Запрашивает вылеты из аэропорта за заданный период."""
    url = "https://opensky-network.org/api/flights/departure"
    params = {"airport": icao, "begin": begin_ts, "end": end_ts}
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json() or []
            elif r.status_code == 404:
                return []  # нет данных за этот период
            elif r.status_code == 429:
                print(f"    Rate limit, ждём 60 сек...")
                time.sleep(60)
            else:
                print(f"    HTTP {r.status_code} для {icao}, период {begin_ts}-{end_ts}")
                return []
        except Exception as e:
            print(f"    Ошибка: {e}, попытка {attempt+1}/3")
            time.sleep(10)
    return []


def main():
    session = requests.Session()
    if OPENSKY_USER and OPENSKY_PASS:
        session.auth = (OPENSKY_USER, OPENSKY_PASS)
        print("✓ Авторизованный доступ к OpenSky")
    else:
        print("! Анонимный доступ к OpenSky (ограниченный)")

    now = datetime.now(timezone.utc)

    # 4 окна по 7 дней = последние 28 дней
    windows = []
    for i in range(4):
        end = now - timedelta(days=i * 7)
        begin = end - timedelta(days=7)
        windows.append((int(begin.timestamp()), int(end.timestamp())))

    # route_counts[город_рф][icao_назначения] = кол-во рейсов
    route_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    total_airports = len(RU_AIRPORTS)
    for idx, (icao, city) in enumerate(RU_AIRPORTS.items(), 1):
        print(f"[{idx}/{total_airports}] {city} ({icao})")
        for begin_ts, end_ts in windows:
            flights = get_departures(icao, begin_ts, end_ts, session)
            for f in flights:
                arr = (f.get("estArrivalAirport") or "").strip().upper()
                if not arr or len(arr) != 4:
                    continue
                # Пропускаем внутренние рейсы
                if arr[:2] in RU_ICAO_PREFIXES:
                    continue
                # Пропускаем неизвестные аэропорты
                if arr not in DEST_INFO:
                    continue
                route_counts[city][arr] += 1
            time.sleep(2)  # Пауза между запросами
        time.sleep(3)
        print(f"    → {sum(route_counts[city].values())} вылетов найдено")

    # Строим маршруты: минимум 2 рейса за 28 дней
    MIN_FLIGHTS = 2
    routes: dict[str, list[str]] = {}
    used_icao: set[str] = set()

    for city, dest_counts in route_counts.items():
        city_dests = []
        for arr_icao, count in sorted(dest_counts.items(), key=lambda x: -x[1]):
            if count >= MIN_FLIGHTS:
                dest_name = DEST_INFO[arr_icao]["n"]
                if dest_name not in city_dests:  # дедупликация (напр. два аэропорта Стамбула)
                    city_dests.append(dest_name)
                    used_icao.add(arr_icao)
        if city_dests:
            # Объединяем маршруты нескольких аэропортов одного города (Москва)
            existing = routes.get(city, [])
            merged = list(dict.fromkeys(existing + city_dests))
            routes[city] = merged

    # Метаданные только использованных направлений (дедупликация по имени города)
    destinations: dict[str, dict] = {}
    for icao in used_icao:
        info = DEST_INFO[icao]
        name = info["n"]
        if name not in destinations:
            destinations[name] = {
                "la": info["la"], "lo": info["lo"],
                "r":  info["r"],  "c":  info["c"]
            }

    output = {
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "routes": routes,
        "destinations": destinations
    }

    with open("routes.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_routes = sum(len(v) for v in routes.values())
    print(f"\n✓ Готово: {len(routes)} городов РФ, {total_routes} маршрутов")
    print(f"  Обновлено: {output['updated']}")
    print(f"  Сохранено в routes.json")


if __name__ == "__main__":
    main()
