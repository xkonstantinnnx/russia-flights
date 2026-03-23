#!/usr/bin/env python3
"""
Обновление маршрутов из России.

Цепочка источников:
  1. OpenSky Network (OAuth2) — реальные ADS-B вылеты за 14 дней
  2. AirLabs Routes API       — расписание маршрутов (fallback)

Логика переключения:
  - Если у OpenSky нет кредитов изначально → сразу AirLabs для всех.
  - Если кредиты OpenSky закончились в процессе → AirLabs только для
    аэропортов, которые OpenSky не успел проверить ИЛИ вернул 0 вылетов.
  - При любом сбое источника routes.json не трогается.
"""

import os, json, time, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ─── Credentials ─────────────────────────────────────────────────────────────
OPENSKY_CLIENT_ID     = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
AIRLABS_KEY           = os.environ.get("AIRLABS_KEY", "")
# AVIATION_EDGE_KEY = os.environ.get("AVIATION_EDGE_KEY", "")  # закомментирован — нет бесплатного плана

TOKEN_URL = (
    "https://auth.opensky-network.org"
    "/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

# ─── Российские аэропорты ─────────────────────────────────────────────────────
RU_AIRPORTS = {
    "UUEE": "Москва",            # Шереметьево
    "UUDD": "Москва",            # Домодедово
    "UUWW": "Москва",            # Внуково
    "ULLI": "Санкт-Петербург",
    "URSS": "Сочи",
    "URKK": "Краснодар",
    "URMM": "Мин. Воды",
    "USSS": "Екатеринбург",
    "USCC": "Челябинск",
    "UWOO": "Оренбург",
    "UWUU": "Уфа",
    "UWKD": "Казань",
    "UWGG": "Нижний Новгород",
    "USPP": "Пермь",
    "UWWW": "Самара",
    "URWW": "Волгоград",
    "UWSS": "Саратов",
    "USTR": "Тюмень",
    "UNOO": "Омск",
    "USRR": "Сургут",
    "USNN": "Нижневартовск",
    "UNNT": "Новосибирск",
    "UNKL": "Красноярск",
    "UIII": "Иркутск",
    "UIUU": "Улан-Удэ",
    "UHHH": "Хабаровск",
    "UHWW": "Владивосток",
    "UHBB": "Благовещенск",
    "ULMM": "Мурманск",
    "UIAA": "Чита",
    "UHSS": "Южно-Сахалинск",
}

RU_ICAO_PREFIXES = ("UU", "UI", "UH", "UK", "UL", "UM", "UN", "UR", "US", "UW", "UE", "UO")

# ─── Аэропорты назначений ─────────────────────────────────────────────────────
DEST_INFO = {
    "UMMS": {"n":"Минск",            "c":"Беларусь",            "la":53.9, "lo":27.6,  "r":"СНГ"},
    "UDYZ": {"n":"Ереван",           "c":"Армения",             "la":40.2, "lo":44.5,  "r":"СНГ"},
    "UGTB": {"n":"Тбилиси",          "c":"Грузия",              "la":41.7, "lo":44.8,  "r":"СНГ"},
    "UGKO": {"n":"Батуми",           "c":"Грузия",              "la":41.6, "lo":41.6,  "r":"СНГ"},
    "UBBB": {"n":"Баку",             "c":"Азербайджан",         "la":40.4, "lo":49.9,  "r":"СНГ"},
    "UGSS": {"n":"Сухум",            "c":"Абхазия",             "la":43.0, "lo":41.0,  "r":"СНГ"},
    "UTTT": {"n":"Ташкент",          "c":"Узбекистан",          "la":41.3, "lo":69.3,  "r":"СНГ"},
    "UTSS": {"n":"Самарканд",        "c":"Узбекистан",          "la":39.7, "lo":67.0,  "r":"СНГ"},
    "UCFM": {"n":"Бишкек",           "c":"Кыргызстан",          "la":42.9, "lo":74.6,  "r":"СНГ"},
    "UAFO": {"n":"Ош",               "c":"Кыргызстан",          "la":40.5, "lo":72.8,  "r":"СНГ"},
    "UTDD": {"n":"Душанбе",          "c":"Таджикистан",         "la":38.6, "lo":68.8,  "r":"СНГ"},
    "UAAA": {"n":"Алматы",           "c":"Казахстан",           "la":43.3, "lo":77.0,  "r":"СНГ"},
    "UACC": {"n":"Астана",           "c":"Казахстан",           "la":51.2, "lo":71.5,  "r":"СНГ"},
    "UTAA": {"n":"Ашхабад",          "c":"Туркменистан",        "la":38.0, "lo":58.4,  "r":"СНГ"},
    "LYBE": {"n":"Белград",          "c":"Сербия",              "la":44.8, "lo":20.5,  "r":"Европа"},
    "LTFM": {"n":"Стамбул",          "c":"Турция",              "la":41.0, "lo":29.0,  "r":"БВ"},
    "LTBA": {"n":"Стамбул",          "c":"Турция",              "la":41.0, "lo":29.0,  "r":"БВ"},
    "LTAI": {"n":"Анталья",          "c":"Турция",              "la":36.9, "lo":30.7,  "r":"БВ"},
    "LTBS": {"n":"Даламан",          "c":"Турция",              "la":36.8, "lo":28.8,  "r":"БВ"},
    "LTBJ": {"n":"Измир",            "c":"Турция",              "la":38.3, "lo":27.2,  "r":"БВ"},
    "OMDB": {"n":"Дубай",            "c":"ОАЭ",                 "la":25.2, "lo":55.3,  "r":"БВ"},
    "OMAA": {"n":"Абу-Даби",         "c":"ОАЭ",                 "la":24.5, "lo":54.4,  "r":"БВ"},
    "OTHH": {"n":"Доха",             "c":"Катар",               "la":25.3, "lo":51.5,  "r":"БВ"},
    "OERK": {"n":"Эр-Рияд",          "c":"Саудовская Аравия",   "la":24.7, "lo":46.7,  "r":"БВ"},
    "OEDF": {"n":"Даммам",           "c":"Саудовская Аравия",   "la":26.5, "lo":49.8,  "r":"БВ"},
    "OOMS": {"n":"Маскат",           "c":"Оман",                "la":23.6, "lo":58.4,  "r":"БВ"},
    "OOSA": {"n":"Салала",           "c":"Оман",                "la":17.0, "lo":54.1,  "r":"БВ"},
    "OBBI": {"n":"Манама",           "c":"Бахрейн",             "la":26.2, "lo":50.6,  "r":"БВ"},
    "OKBK": {"n":"Эль-Кувейт",       "c":"Кувейт",              "la":29.4, "lo":48.0,  "r":"БВ"},
    "LLBG": {"n":"Тель-Авив",        "c":"Израиль",             "la":32.1, "lo":34.8,  "r":"БВ"},
    "OJAM": {"n":"Амман",            "c":"Иордания",            "la":31.9, "lo":35.9,  "r":"БВ"},
    "OIIE": {"n":"Тегеран",          "c":"Иран",                "la":35.7, "lo":51.4,  "r":"БВ"},
    "ORBI": {"n":"Багдад",           "c":"Ирак",                "la":33.3, "lo":44.2,  "r":"БВ"},
    "VTBS": {"n":"Бангкок",          "c":"Таиланд",             "la":13.8, "lo":100.5, "r":"Азия"},
    "VTBD": {"n":"Бангкок",          "c":"Таиланд",             "la":13.8, "lo":100.5, "r":"Азия"},
    "VTSP": {"n":"Пхукет",           "c":"Таиланд",             "la":7.9,  "lo":98.4,  "r":"Азия"},
    "VTSK": {"n":"Краби",            "c":"Таиланд",             "la":8.1,  "lo":98.9,  "r":"Азия"},
    "VVTS": {"n":"Хошимин",          "c":"Вьетнам",             "la":10.8, "lo":106.6, "r":"Азия"},
    "VVCR": {"n":"Нячанг",           "c":"Вьетнам",             "la":12.2, "lo":109.2, "r":"Азия"},
    "VVNB": {"n":"Ханой",            "c":"Вьетнам",             "la":21.0, "lo":105.8, "r":"Азия"},
    "VVDN": {"n":"Дананг",           "c":"Вьетнам",             "la":16.1, "lo":108.2, "r":"Азия"},
    "VVPQ": {"n":"Фукуок",           "c":"Вьетнам",             "la":10.2, "lo":104.0, "r":"Азия"},
    "WADD": {"n":"Денпасар",         "c":"Индонезия (Бали)",    "la":-8.7, "lo":115.2, "r":"Азия"},
    "RPLL": {"n":"Манила",           "c":"Филиппины",           "la":14.6, "lo":121.0, "r":"Азия"},
    "ZBAA": {"n":"Пекин",            "c":"Китай",               "la":39.9, "lo":116.4, "r":"Азия"},
    "ZBAD": {"n":"Пекин",            "c":"Китай",               "la":39.5, "lo":116.4, "r":"Азия"},
    "ZSPD": {"n":"Шанхай",           "c":"Китай",               "la":31.2, "lo":121.5, "r":"Азия"},
    "ZSSS": {"n":"Шанхай",           "c":"Китай",               "la":31.2, "lo":121.3, "r":"Азия"},
    "ZUUU": {"n":"Чэнду",            "c":"Китай",               "la":30.6, "lo":104.1, "r":"Азия"},
    "ZJSY": {"n":"Санья",            "c":"Китай",               "la":18.3, "lo":109.5, "r":"Азия"},
    "ZYHB": {"n":"Харбин",           "c":"Китай",               "la":45.8, "lo":126.6, "r":"Азия"},
    "ZYTL": {"n":"Далянь",           "c":"Китай",               "la":38.9, "lo":121.6, "r":"Азия"},
    "ZGGG": {"n":"Гуанчжоу",         "c":"Китай",               "la":23.4, "lo":113.3, "r":"Азия"},
    "VHHH": {"n":"Гонконг",          "c":"Гонконг (КНР)",       "la":22.3, "lo":114.2, "r":"Азия"},
    "VMMC": {"n":"Макао",            "c":"Макао (КНР)",         "la":22.2, "lo":113.6, "r":"Азия"},
    "VIDP": {"n":"Дели",             "c":"Индия",               "la":28.6, "lo":77.2,  "r":"Азия"},
    "VAGO": {"n":"Гоа",              "c":"Индия",               "la":15.3, "lo":73.9,  "r":"Азия"},
    "VRMM": {"n":"Мале",             "c":"Мальдивы",            "la":4.2,  "lo":73.5,  "r":"Азия"},
    "VCBI": {"n":"Коломбо",          "c":"Шри-Ланка",           "la":6.9,  "lo":79.9,  "r":"Азия"},
    "ZMUB": {"n":"Улан-Батор",       "c":"Монголия",            "la":47.9, "lo":106.9, "r":"Азия"},
    "ZKPY": {"n":"Пхеньян",          "c":"КНДР",                "la":39.0, "lo":125.8, "r":"Азия"},
    "OAIX": {"n":"Кабул",            "c":"Афганистан",          "la":34.5, "lo":69.2,  "r":"Азия"},
    "HECA": {"n":"Каир",             "c":"Египет",              "la":30.1, "lo":31.2,  "r":"Африка"},
    "HEGN": {"n":"Хургада",          "c":"Египет",              "la":27.3, "lo":33.8,  "r":"Африка"},
    "HESH": {"n":"Шарм-эш-Шейх",    "c":"Египет",              "la":27.9, "lo":34.3,  "r":"Африка"},
    "HAAB": {"n":"Аддис-Абеба",      "c":"Эфиопия",             "la":9.0,  "lo":38.7,  "r":"Африка"},
    "DAAG": {"n":"Алжир",            "c":"Алжир",               "la":36.7, "lo":3.1,   "r":"Африка"},
    "GMMN": {"n":"Касабланка",       "c":"Марокко",             "la":33.6, "lo":-7.6,  "r":"Африка"},
    "FSIA": {"n":"Маэ",              "c":"Сейшелы",             "la":-4.6, "lo":55.5,  "r":"Африка"},
    "SVMI": {"n":"Каракас",          "c":"Венесуэла",           "la":10.5, "lo":-66.9, "r":"ЛА"},
    "MUHA": {"n":"Гавана",           "c":"Куба",                "la":23.1, "lo":-82.4, "r":"ЛА"},
    # ── Дополнительные аэропорты (альтернативные коды и новые направления) ──
    "OMDW": {"n":"Дубай",        "c":"ОАЭ",           "la":24.9,  "lo":55.2,  "r":"БВ"},
    "UAFM": {"n":"Бишкек",       "c":"Кыргызстан",    "la":42.9,  "lo":74.6,  "r":"СНГ"},
    "UCFO": {"n":"Ош",           "c":"Кыргызстан",    "la":40.5,  "lo":72.8,  "r":"СНГ"},
    "UAII": {"n":"Шымкент",      "c":"Казахстан",     "la":42.3,  "lo":69.7,  "r":"СНГ"},
    "UGSB": {"n":"Кутаиси",      "c":"Грузия",        "la":42.2,  "lo":42.5,  "r":"СНГ"},
    "UTDL": {"n":"Навои",        "c":"Узбекистан",    "la":40.1,  "lo":65.2,  "r":"СНГ"},
    "UTFN": {"n":"Наманган",     "c":"Узбекистан",    "la":40.9,  "lo":71.6,  "r":"СНГ"},
    "UTKA": {"n":"Андижан",      "c":"Узбекистан",    "la":40.7,  "lo":72.3,  "r":"СНГ"},
    "UTKF": {"n":"Фергана",      "c":"Узбекистан",    "la":40.4,  "lo":71.7,  "r":"СНГ"},
    "UTSB": {"n":"Бухара",       "c":"Узбекистан",    "la":39.8,  "lo":64.5,  "r":"СНГ"},
    "ZBLA": {"n":"Хайлар",       "c":"Китай",         "la":49.1,  "lo":119.8, "r":"Азия"},
    "HEAL": {"n":"Александрия",  "c":"Египет",        "la":30.9,  "lo":29.7,  "r":"Африка"},
}


# ══════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════

def icao_to_dest_name(arr_icao: str) -> str | None:
    if arr_icao[:2] in RU_ICAO_PREFIXES:
        return None
    info = DEST_INFO.get(arr_icao)
    return info["n"] if info else None


def build_output(city_routes: dict, now: datetime, sources_used: list[str]) -> dict:
    used_icao: set[str] = set()
    for city, dests in city_routes.items():
        for name in dests:
            for icao, info in DEST_INFO.items():
                if info["n"] == name:
                    used_icao.add(icao)
                    break

    destinations: dict = {}
    for icao in used_icao:
        info = DEST_INFO[icao]
        name = info["n"]
        if name not in destinations:
            destinations[name] = {
                "la": info["la"], "lo": info["lo"],
                "r":  info["r"],  "c":  info["c"],
            }

    return {
        "updated":      now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":       " + ".join(sources_used),
        "routes":       city_routes,
        "destinations": destinations,
    }


# ══════════════════════════════════════════════════════════════
#  Источник 1 — OpenSky Network
# ══════════════════════════════════════════════════════════════

class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id     = client_id
        self.client_secret = client_secret
        self._token        = None
        self._expires_at   = 0.0

    def get_token(self) -> str:
        if time.time() >= self._expires_at - 300:
            self._refresh()
        return self._token

    def _refresh(self):
        print("  [auth] Получаем OAuth2 токен...", flush=True)
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=(10, 15),
        )
        r.raise_for_status()
        data             = r.json()
        self._token      = data["access_token"]
        expires_in       = data.get("expires_in", 1800)
        self._expires_at = time.time() + expires_in
        print(f"  [auth] Токен получен, действует {expires_in // 60} мин", flush=True)


def check_opensky_credits(token_mgr: TokenManager) -> int | None:
    try:
        r = requests.get(
            "https://opensky-network.org/api/flights/departure",
            params={"airport": "EDDF", "begin": 1717200000, "end": 1717286399},
            headers={"Authorization": f"Bearer {token_mgr.get_token()}"},
            timeout=(10, 15),
        )
        if r.status_code == 429:
            return 0
        remaining = r.headers.get("X-Rate-Limit-Remaining")
        if remaining is not None:
            val = int(remaining)
            print(f"=== Кредитов OpenSky: {val} ===", flush=True)
            return val
        print("=== Кредиты OpenSky: неизвестно (заголовок отсутствует) ===", flush=True)
        return None
    except Exception as e:
        print(f"=== Не удалось проверить кредиты: {e} ===", flush=True)
        return None


def fetch_opensky_departures(icao: str, begin_ts: int, end_ts: int,
                             token_mgr: TokenManager) -> list | None:
    url    = "https://opensky-network.org/api/flights/departure"
    params = {"airport": icao, "begin": begin_ts, "end": end_ts}
    print(f"    → GET departure airport={icao} begin={begin_ts} end={end_ts}", flush=True)

    for attempt in range(2):
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(10, 15))
            print(f"    ← HTTP {r.status_code}", flush=True)

            if r.status_code == 200:
                remaining = r.headers.get("X-Rate-Limit-Remaining")
                if remaining is not None:
                    print(f"    кредитов осталось: {remaining}", flush=True)
                    if int(remaining) == 0:
                        print("    ⚠ Кредиты исчерпаны", flush=True)
                        return None
                return r.json() or []
            elif r.status_code == 404:
                return []
            elif r.status_code in (403, 429):
                print(f"    ⚠ {r.status_code} — останавливаем OpenSky", flush=True)
                return None
            elif r.status_code == 401:
                token_mgr._expires_at = 0
                time.sleep(2)
            else:
                print(f"    HTTP {r.status_code}", flush=True)
                return []
        except requests.exceptions.Timeout:
            print(f"    Timeout, попытка {attempt + 1}/2", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"    Ошибка: {e}, попытка {attempt + 1}/2", flush=True)
            time.sleep(5)
    return []


def run_opensky(token_mgr: TokenManager,
                icaos_to_process: list[tuple[str, str]],
                now: datetime) -> tuple[dict, list]:
    """
    Возвращает (city_routes, need_fallback).
    need_fallback = аэропорты не обработанные + с 0 результатом.
    """
    DAYS_HISTORY = 14
    MIN_FLIGHTS  = 2

    windows = []
    for i in range(1, DAYS_HISTORY + 1):
        day   = (now - timedelta(days=i)).date()
        begin = int(datetime(day.year, day.month, day.day,  0,  0,  0, tzinfo=timezone.utc).timestamp())
        end   = int(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        windows.append((begin, end))

    print(f"  Период: {windows[-1][0]} → {windows[0][1]}", flush=True)

    route_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    credits_exhausted = False
    not_processed: list[tuple[str, str]] = []
    zero_results:  list[tuple[str, str]] = []

    total = len(icaos_to_process)
    for idx, (icao, city) in enumerate(icaos_to_process, 1):
        print(f"\n[{idx}/{total}] {city} ({icao})", flush=True)

        if credits_exhausted:
            not_processed.append((icao, city))
            continue

        stop_this_airport = False
        for begin_ts, end_ts in windows:
            result = fetch_opensky_departures(icao, begin_ts, end_ts, token_mgr)
            if result is None:
                credits_exhausted = True
                not_processed.append((icao, city))
                stop_this_airport = True
                break
            for f in result:
                arr = (f.get("estArrivalAirport") or "").strip().upper()
                if arr and len(arr) == 4 and arr[:2] not in RU_ICAO_PREFIXES and arr in DEST_INFO:
                    route_counts[city][arr] += 1
            time.sleep(5)

        if stop_this_airport:
            break

        total_hits = sum(route_counts[city].values())
        print(f"    → {total_hits} засечённых вылетов", flush=True)
        if total_hits == 0:
            zero_results.append((icao, city))
        time.sleep(3)

    # Строим city_routes
    city_routes: dict[str, list[str]] = {}
    for city, dest_counts in route_counts.items():
        dests = []
        seen  = set()
        for arr_icao, count in sorted(dest_counts.items(), key=lambda x: -x[1]):
            if count >= MIN_FLIGHTS:
                name = DEST_INFO[arr_icao]["n"]
                if name not in seen:
                    dests.append(name)
                    seen.add(name)
        if dests:
            existing = city_routes.get(city, [])
            city_routes[city] = list(dict.fromkeys(existing + dests))

    # Fallback нужен для: не обработанных + обработанных с 0 результатом
    need_fallback = not_processed + [
        (icao, city) for (icao, city) in zero_results
        if city not in city_routes
    ]

    if credits_exhausted:
        print(f"\n  ⚠ OpenSky: кредиты закончились на {len(not_processed)} аэропортах", flush=True)
        print(f"  Городов с маршрутами: {len(city_routes)}", flush=True)
        print(f"  Аэропортов для fallback: {len(need_fallback)}", flush=True)
    else:
        print(f"\n  OpenSky завершён: {len(city_routes)} городов с маршрутами", flush=True)
        print(f"  Аэропортов с 0 результатом → fallback: {len(need_fallback)}", flush=True)

    return city_routes, need_fallback


# ══════════════════════════════════════════════════════════════
#  Источник 2 — AirLabs
# ══════════════════════════════════════════════════════════════

def fetch_airlabs_routes(icao: str, api_key: str) -> list | None:
    print(f"    → AirLabs routes dep_icao={icao}", flush=True)
    try:
        r = requests.get(
            "https://airlabs.co/api/v9/routes",
            params={"dep_icao": icao, "api_key": api_key},
            timeout=(10, 20),
        )
        print(f"    ← HTTP {r.status_code}", flush=True)
        if r.status_code == 200:
            data = r.json()
            if data.get("error"):
                print(f"    AirLabs error: {data['error']}", flush=True)
                return None
            routes = data.get("response", [])
            return [f.get("arr_icao", "").upper() for f in routes if f.get("arr_icao")]
        elif r.status_code == 429:
            print("    AirLabs: лимит исчерпан", flush=True)
            return None
        else:
            print(f"    AirLabs HTTP {r.status_code}", flush=True)
            return []
    except Exception as e:
        print(f"    AirLabs ошибка: {e}", flush=True)
        return []


def run_airlabs(api_key: str,
                icaos_to_process: list[tuple[str, str]],
                existing_routes: dict) -> tuple[dict, list]:
    city_routes = dict(existing_routes)
    city_buf: dict[str, list[str]] = defaultdict(list)
    remaining: list[tuple[str, str]] = []

    total = len(icaos_to_process)
    for idx, (icao, city) in enumerate(icaos_to_process, 1):
        print(f"\n[{idx}/{total}] {city} ({icao})", flush=True)
        arr_icaos = fetch_airlabs_routes(icao, api_key)
        if arr_icaos is None:
            remaining.extend(icaos_to_process[idx - 1:])
            break
        city_buf[city].extend(arr_icaos)
        print(f"    → {len(arr_icaos)} маршрутов от AirLabs", flush=True)
        time.sleep(2)

    for city, arr_icaos in city_buf.items():
        dests = []
        seen  = set()
        for arr_icao in arr_icaos:
            name = icao_to_dest_name(arr_icao)
            if name and name not in seen:
                dests.append(name)
                seen.add(name)
        if dests:
            if city not in city_routes:
                city_routes[city] = dests
            else:
                existing_set = set(city_routes[city])
                extra = [d for d in dests if d not in existing_set]
                city_routes[city] = city_routes[city] + extra

    print(f"\n  AirLabs завершён: {len(city_routes)} городов итого", flush=True)
    return city_routes, remaining


# ══════════════════════════════════════════════════════════════
#  Источник 3 — Aviation Edge (закомментирован)
#  Бесплатных ключей больше нет. Раскомментируйте при наличии
#  платного ключа и добавьте AVIATION_EDGE_KEY в секреты GitHub.
# ══════════════════════════════════════════════════════════════
#
# def fetch_aviation_edge_routes(icao, api_key): ...
# def run_aviation_edge(api_key, icaos_to_process, existing_routes): ...
#
# Полный код доступен в git-истории репозитория.


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("=== Script started ===", flush=True)

    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        print("✗ OPENSKY_CLIENT_ID и OPENSKY_CLIENT_SECRET не заданы", flush=True)
        sys.exit(1)

    print(f"=== CLIENT_ID length: {len(OPENSKY_CLIENT_ID)}, "
          f"CLIENT_SECRET length: {len(OPENSKY_CLIENT_SECRET)} ===", flush=True)
    print(f"=== AirLabs:      {'доступен' if AIRLABS_KEY else 'не настроен'} ===", flush=True)

    token_mgr = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
    try:
        print("=== Requesting token... ===", flush=True)
        token_mgr.get_token()
    except Exception as e:
        print(f"✗ Не удалось получить токен: {e}", flush=True)
        sys.exit(1)

    now          = datetime.now(timezone.utc)
    all_airports = list(RU_AIRPORTS.items())

    print("=== Проверка кредитов OpenSky... ===", flush=True)
    credits = check_opensky_credits(token_mgr)
    opensky_has_credits = (credits is None or credits > 0)

    city_routes:   dict[str, list[str]] = {}
    need_fallback: list[tuple[str, str]] = []
    sources_used:  list[str] = []

    # ── Шаг 1: OpenSky ───────────────────────────────────────────────────────
    if opensky_has_credits:
        print("\n=== Шаг 1: OpenSky Network ===", flush=True)
        city_routes, need_fallback = run_opensky(token_mgr, all_airports, now)
        if city_routes:
            sources_used.append("OpenSky Network")
        if not need_fallback:
            print("\n✓ OpenSky покрыл все аэропорты", flush=True)
        else:
            print(f"\n  Для fallback: {len(need_fallback)} аэропортов", flush=True)
    else:
        print("\n=== OpenSky: нет кредитов — сразу AirLabs ===", flush=True)
        need_fallback = all_airports

    # ── Шаг 2: AirLabs ───────────────────────────────────────────────────────
    if need_fallback:
        if not AIRLABS_KEY:
            print(f"\n⚠ AirLabs не настроен. {len(need_fallback)} аэропортов без данных.", flush=True)
        else:
            print(f"\n=== Шаг 2: AirLabs ({len(need_fallback)} аэропортов) ===", flush=True)
            city_routes, need_fallback = run_airlabs(AIRLABS_KEY, need_fallback, city_routes)
            sources_used.append("AirLabs")

    # ── Шаг 3: Aviation Edge — отключён (нет бесплатного плана) ─────────────────
    # if need_fallback and AVIATION_EDGE_KEY:
    #     city_routes = run_aviation_edge(AVIATION_EDGE_KEY, need_fallback, city_routes)
    if need_fallback:
        print(f"\n⚠ {len(need_fallback)} аэропортов остались без данных (все источники исчерпаны).", flush=True)

    # ── Сохраняем ────────────────────────────────────────────────────────────
    if not city_routes:
        print("\n⚠ Маршруты не получены, routes.json НЕ обновляется.", flush=True)
        sys.exit(0)

    output = build_output(city_routes, now, sources_used)

    with open("routes.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_routes = sum(len(v) for v in city_routes.values())
    print(f"\n✓ Готово [{output['source']}]: {len(city_routes)} городов РФ, {total_routes} маршрутов")
    print(f"  Обновлено: {output['updated']}")


if __name__ == "__main__":
    main()
