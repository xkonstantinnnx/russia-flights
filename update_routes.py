#!/usr/bin/env python3
"""
Обновление маршрутов из России.
Основной источник: OpenSky Network API (OAuth2).
Резервный источник: AirLabs Routes API (если кредиты OpenSky исчерпаны).

Запускается еженедельно через GitHub Actions.
"""

import os, json, time, sys, requests
import socket
socket.setdefaulttimeout(20)
from datetime import datetime, timezone, timedelta
from collections import defaultdict

CLIENT_ID     = os.environ.get("OPENSKY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
AIRLABS_KEY   = os.environ.get("AIRLABS_KEY", "")

TOKEN_URL = (
    "https://auth.opensky-network.org"
    "/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

# ─── Российские аэропорты (ICAO → город) ─────────────────────────────────────
# Три московских аэропорта объединяем в "Москва"
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

# Префиксы ICAO российских аэропортов — для отсева внутренних рейсов
RU_ICAO_PREFIXES = ("UU", "UI", "UH", "UK", "UL", "UM", "UN", "UR", "US", "UW")

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
}


# ══════════════════════════════════════════════════════════════
#  OAuth2 — получение и обновление токена
# ══════════════════════════════════════════════════════════════

class TokenManager:
    """Получает токен OAuth2 и автоматически обновляет его за 5 минут до истечения."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id     = client_id
        self.client_secret = client_secret
        self._token        = None
        self._expires_at   = 0.0

    def get_token(self) -> str:
        # Обновляем, если до истечения осталось менее 5 минут
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
            timeout=(10, 15),  # (connect_timeout, read_timeout)
        )
        print(f"  [auth] HTTP {r.status_code}", flush=True)
        r.raise_for_status()
        data             = r.json()
        self._token      = data["access_token"]
        expires_in       = data.get("expires_in", 1800)  # обычно 1800 сек (30 мин)
        self._expires_at = time.time() + expires_in
        print(f"  [auth] Токен получен, действует {expires_in // 60} мин", flush=True)


# ══════════════════════════════════════════════════════════════
#  OpenSky — получение вылетов из аэропорта за один день
# ══════════════════════════════════════════════════════════════

def get_departures(icao: str, begin_ts: int, end_ts: int,
                   token_mgr: TokenManager) -> list | None:
    """
    Запрашивает вылеты из аэропорта за период [begin_ts, end_ts].
    Возвращает список рейсов, [] если рейсов нет,
    None при критической ошибке (403, 429, кредиты = 0) — сигнал переключиться на AirLabs.
    """
    url = "https://opensky-network.org/api/flights/departure"
    params = {"airport": icao, "begin": begin_ts, "end": end_ts}
    print(f"    → GET departure airport={icao} begin={begin_ts} end={end_ts}", flush=True)
    for attempt in range(3):
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(10, 15))
            print(f"    ← HTTP {r.status_code}", flush=True)

            if r.status_code == 200:
                remaining = r.headers.get("X-Rate-Limit-Remaining")
                if remaining is not None:
                    print(f"    кредитов осталось: {remaining}", flush=True)
                    if int(remaining) == 0:
                        print(f"    ⚠ Кредиты исчерпаны на сегодня, останавливаемся", flush=True)
                        return None  # None = сигнал основному циклу остановить всё
                return r.json() or []
            elif r.status_code == 404:
                return []  # нет рейсов за этот период — норма
            elif r.status_code == 403:
                print(f"    ⚠ 403 Forbidden для {icao} — OpenSky заблокировал AWS IP", flush=True)
                return None  # сигнал для основного цикла: прекратить запросы
            elif r.status_code == 401:
                print(f"    токен истёк, принудительно обновляем...", flush=True)
                token_mgr._expires_at = 0  # форсируем обновление
                time.sleep(5)
            elif r.status_code == 429:
                print(f"    Rate limit (429) — кредиты исчерпаны, останавливаемся", flush=True)
                return None  # None останавливает весь скрипт
            else:
                print(f"    HTTP {r.status_code} для {icao}", flush=True)
                return []

        except requests.exceptions.Timeout:
            print(f"    Timeout для {icao}, попытка {attempt + 1}/3", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"    Ошибка: {e}, попытка {attempt + 1}/3", flush=True)
            time.sleep(10)

    return []


# ══════════════════════════════════════════════════════════════
#  AirLabs — получение маршрутов аэропорта (резервный источник)
# ══════════════════════════════════════════════════════════════

def get_routes_airlabs(icao: str, api_key: str) -> list | None:
    """
    Возвращает список ICAO-кодов аэропортов назначения для данного аэропорта.
    Один запрос на аэропорт — не зависит от дат и кредитов OpenSky.
    Возвращает None при ошибке.
    """
    url    = "https://airlabs.co/api/v9/routes"
    params = {"dep_icao": icao, "api_key": api_key}
    print(f"    → AirLabs routes dep_icao={icao}", flush=True)
    try:
        r = requests.get(url, params=params, timeout=(10, 20))
        print(f"    ← HTTP {r.status_code}", flush=True)
        if r.status_code == 200:
            data = r.json()
            if data.get("error"):
                print(f"    AirLabs error: {data['error']}", flush=True)
                return None
            routes = data.get("response", [])
            return [f.get("arr_icao", "").upper() for f in routes if f.get("arr_icao")]
        elif r.status_code == 429:
            print(f"    AirLabs: лимит запросов исчерпан", flush=True)
            return None
        else:
            print(f"    AirLabs HTTP {r.status_code}", flush=True)
            return None
    except Exception as e:
        print(f"    AirLabs ошибка: {e}", flush=True)
        return None


def build_from_airlabs(airlabs_results: dict) -> tuple[dict, dict]:
    """
    airlabs_results: {город_рф: [icao_назначения, ...]}
    Возвращает (routes, destinations).
    """
    routes:    dict[str, list[str]] = {}
    used_icao: set[str]             = set()

    for city, arr_icaos in airlabs_results.items():
        city_dests: list[str] = []
        seen_names: set[str]  = set()
        for arr_icao in arr_icaos:
            if arr_icao[:2] in RU_ICAO_PREFIXES:
                continue
            if arr_icao not in DEST_INFO:
                continue
            dest_name = DEST_INFO[arr_icao]["n"]
            if dest_name not in seen_names:
                city_dests.append(dest_name)
                seen_names.add(dest_name)
                used_icao.add(arr_icao)
        if city_dests:
            routes[city] = city_dests

    destinations: dict[str, dict] = {}
    for icao in used_icao:
        info = DEST_INFO[icao]
        name = info["n"]
        if name not in destinations:
            destinations[name] = {
                "la": info["la"], "lo": info["lo"],
                "r":  info["r"],  "c":  info["c"],
            }

    return routes, destinations


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("=== Script started ===", flush=True)
    if not CLIENT_ID or not CLIENT_SECRET:
        print("✗ OPENSKY_CLIENT_ID и OPENSKY_CLIENT_SECRET не заданы", flush=True)
        sys.exit(1)

    print(f"=== CLIENT_ID length: {len(CLIENT_ID)}, CLIENT_SECRET length: {len(CLIENT_SECRET)} ===", flush=True)
    print(f"=== AirLabs fallback: {'доступен' if AIRLABS_KEY else 'не настроен'} ===", flush=True)

    token_mgr = TokenManager(CLIENT_ID, CLIENT_SECRET)

    try:
        print("=== Requesting token... ===", flush=True)
        token_mgr.get_token()
    except Exception as e:
        print(f"✗ Не удалось получить токен: {e}", flush=True)
        sys.exit(1)

    # ── Проверяем остаток кредитов одним лёгким запросом ────────────────────
    opensky_ok = True
    try:
        print("=== Проверка кредитов... ===", flush=True)
        r = requests.get(
            "https://opensky-network.org/api/flights/departure",
            params={"airport": "EDDF", "begin": 1717200000, "end": 1717286399},
            headers={"Authorization": f"Bearer {token_mgr.get_token()}"},
            timeout=(10, 15),
        )
        remaining = r.headers.get("X-Rate-Limit-Remaining", "неизвестно")
        limit     = r.headers.get("X-Rate-Limit-Limit", "неизвестно")
        print(f"=== Кредитов осталось: {remaining} из {limit} ===", flush=True)
        if r.status_code == 429:
            print("=== Кредиты исчерпаны, переходим на AirLabs ===", flush=True)
            opensky_ok = False
        elif remaining != "неизвестно" and int(remaining) == 0:
            print("=== Кредиты на нуле, переходим на AirLabs ===", flush=True)
            opensky_ok = False
    except Exception as e:
        print(f"=== Не удалось проверить кредиты: {e} ===", flush=True)

    now = datetime.now(timezone.utc)
    routes:       dict[str, list[str]] = {}
    destinations: dict[str, dict]      = {}
    source = ""

    # ══════════════════════════════════════════════════════════
    #  Путь 1 — OpenSky
    # ══════════════════════════════════════════════════════════
    if opensky_ok:
        print("\n=== Используем OpenSky Network ===", flush=True)

        # Лимит API: begin и end должны лежать в одном UTC-календарном дне.
        # Решение: один запрос на каждый полный UTC-день.
        # 14 дней × 31 аэропорт = 434 запроса.
        DAYS_HISTORY = 14
        windows = []
        for i in range(1, DAYS_HISTORY + 1):
            day   = (now - timedelta(days=i)).date()
            begin = int(datetime(day.year, day.month, day.day,  0,  0,  0, tzinfo=timezone.utc).timestamp())
            end   = int(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
            windows.append((begin, end))

        print(f"Период: {windows[-1][0]} → {windows[0][1]}", flush=True)
        print(f"Аэропортов: {len(RU_AIRPORTS)}, дней истории: {len(windows)}", flush=True)
        print(flush=True)

        route_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        opensky_failed = False

        total = len(RU_AIRPORTS)
        for idx, (icao, city) in enumerate(RU_AIRPORTS.items(), 1):
            if opensky_failed:
                break
            print(f"[{idx}/{total}] {city} ({icao})", flush=True)

            for begin_ts, end_ts in windows:
                result = get_departures(icao, begin_ts, end_ts, token_mgr)

                if result is None:
                    opensky_failed = True
                    break

                for f in result:
                    arr = (f.get("estArrivalAirport") or "").strip().upper()
                    if not arr or len(arr) != 4:
                        continue
                    if arr[:2] in RU_ICAO_PREFIXES:
                        continue
                    if arr not in DEST_INFO:
                        continue
                    route_counts[city][arr] += 1

                time.sleep(5)  # пауза между запросами

            total_for_city = sum(route_counts[city].values())
            print(f"    → {total_for_city} засечённых вылетов", flush=True)
            time.sleep(3)

        if opensky_failed:
            print("\n⚠ OpenSky недоступен (кредиты / AWS блокировка).", flush=True)
            opensky_ok = False
        else:
            # Строим маршруты из данных OpenSky
            MIN_FLIGHTS = 2  # минимум рейсов за 14 дней для включения маршрута
            used_icao: set[str] = set()
            for city, dest_counts in route_counts.items():
                city_dests = []
                for arr_icao, count in sorted(dest_counts.items(), key=lambda x: -x[1]):
                    if count >= MIN_FLIGHTS:
                        dest_name = DEST_INFO[arr_icao]["n"]
                        if dest_name not in city_dests:  # дедупликация
                            city_dests.append(dest_name)
                            used_icao.add(arr_icao)
                if city_dests:
                    existing = routes.get(city, [])
                    routes[city] = list(dict.fromkeys(existing + city_dests))
            for icao in used_icao:
                info = DEST_INFO[icao]
                name = info["n"]
                if name not in destinations:
                    destinations[name] = {
                        "la": info["la"], "lo": info["lo"],
                        "r":  info["r"],  "c":  info["c"],
                    }
            source = "OpenSky Network"

    # ══════════════════════════════════════════════════════════
    #  Путь 2 — AirLabs (fallback)
    # ══════════════════════════════════════════════════════════
    if not opensky_ok:
        if not AIRLABS_KEY:
            print("\n✗ AirLabs не настроен (AIRLABS_KEY отсутствует).", flush=True)
            print("  routes.json НЕ обновляется — оставляем предыдущую версию.", flush=True)
            sys.exit(0)

        print("\n=== Переключаемся на AirLabs Routes API ===", flush=True)
        airlabs_results: dict[str, list[str]] = {}
        total = len(RU_AIRPORTS)

        for idx, (icao, city) in enumerate(RU_AIRPORTS.items(), 1):
            print(f"\n[{idx}/{total}] {city} ({icao})", flush=True)
            arr_icaos = get_routes_airlabs(icao, AIRLABS_KEY)
            if arr_icaos is None:
                print("  AirLabs недоступен, останавливаемся.", flush=True)
                print("  routes.json НЕ обновляется.", flush=True)
                sys.exit(0)
            # Объединяем несколько аэропортов одного города (Москва)
            existing = airlabs_results.get(city, [])
            airlabs_results[city] = existing + arr_icaos
            print(f"    → {len(arr_icaos)} маршрутов от AirLabs", flush=True)
            time.sleep(2)

        routes, destinations = build_from_airlabs(airlabs_results)
        source = "AirLabs Routes API"

    # ══════════════════════════════════════════════════════════
    #  Сохраняем результат
    # ══════════════════════════════════════════════════════════
    if not routes:
        print("\n⚠ Маршруты не получены, routes.json НЕ обновляется.", flush=True)
        sys.exit(0)

    output = {
        "updated":      now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":       source,
        "routes":       routes,
        "destinations": destinations,
    }

    with open("routes.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_routes = sum(len(v) for v in routes.values())
    print(f"\n✓ Готово [{source}]: {len(routes)} городов РФ, {total_routes} маршрутов")
    print(f"  Обновлено: {output['updated']}")


if __name__ == "__main__":
    main()
