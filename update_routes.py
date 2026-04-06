#!/usr/bin/env python3
"""
Обновление маршрутов из России.

Источники (по приоритету):
  1. AirLabs Routes API — первичный: расписание маршрутов, только прямые рейсы
  2. OpenSky Network   — обогащение: реальные ADS-B данные за 14 дней,
                         работает ТОЛЬКО в режиме добавления (никогда не удаляет)

Логика:
  - AirLabs обрабатывает все аэропорты → базовый confirmed
  - OpenSky Stage 1 → Stage 2, только добавляет, останавливается при 429
  - Направление, найденное хотя бы одним источником, включается в итог
  - Направление, не найденное ни одним источником, удаляется

Экономия кредитов OpenSky:
  - Stage 1 (города с уже известными маршрутами) идут первыми
  - Ранний выход из дней когда все маршруты города уже верифицированы
  - RU-префиксы отсеиваются до проверки в DEST_INFO
"""

import os, json, time, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

OPENSKY_CLIENT_ID     = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
AIRLABS_KEY           = os.environ.get("AIRLABS_KEY", "")

TOKEN_URL = (
    "https://auth.opensky-network.org"
    "/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

ROUTES_FILE = Path("routes.json")

RU_AIRPORTS = {
    "UUEE": "Москва", "UUDD": "Москва", "UUWW": "Москва",
    "ULLI": "Санкт-Петербург", "URSS": "Сочи", "URKK": "Краснодар",
    "URMM": "Мин. Воды", "USSS": "Екатеринбург", "USCC": "Челябинск",
    "UWOO": "Оренбург", "UWUU": "Уфа", "UWKD": "Казань",
    "UWGG": "Нижний Новгород", "USPP": "Пермь", "UWWW": "Самара",
    "URWW": "Волгоград", "UWSS": "Саратов", "USTR": "Тюмень",
    "UNOO": "Омск", "USRR": "Сургут", "USNN": "Нижневартовск",
    "UNNT": "Новосибирск", "UNKL": "Красноярск", "UIII": "Иркутск",
    "UIUU": "Улан-Удэ", "UHHH": "Хабаровск", "UHWW": "Владивосток",
    "UHBB": "Благовещенск", "ULMM": "Мурманск", "UIAA": "Чита",
    "UHSS": "Южно-Сахалинск",
}

RU_ICAO_PREFIXES = ("UU","UI","UH","UK","UL","UM","UN","UR","US","UW","UE","UO")

DEST_INFO = {
    "UMMS":{"n":"Минск","c":"Беларусь","la":53.9,"lo":27.6,"r":"СНГ"},
    "UDYZ":{"n":"Ереван","c":"Армения","la":40.2,"lo":44.5,"r":"СНГ"},
    "UGTB":{"n":"Тбилиси","c":"Грузия","la":41.7,"lo":44.8,"r":"СНГ"},
    "UGKO":{"n":"Батуми","c":"Грузия","la":41.6,"lo":41.6,"r":"СНГ"},
    "UGSB":{"n":"Кутаиси","c":"Грузия","la":42.2,"lo":42.5,"r":"СНГ"},
    "UBBB":{"n":"Баку","c":"Азербайджан","la":40.4,"lo":49.9,"r":"СНГ"},
    "UGSS":{"n":"Сухум","c":"Абхазия","la":43.0,"lo":41.0,"r":"СНГ"},
    "UTTT":{"n":"Ташкент","c":"Узбекистан","la":41.3,"lo":69.3,"r":"СНГ"},
    "UTSS":{"n":"Самарканд","c":"Узбекистан","la":39.7,"lo":67.0,"r":"СНГ"},
    "UTDL":{"n":"Навои","c":"Узбекистан","la":40.1,"lo":65.2,"r":"СНГ"},
    "UTFN":{"n":"Наманган","c":"Узбекистан","la":40.9,"lo":71.6,"r":"СНГ"},
    "UTKA":{"n":"Андижан","c":"Узбекистан","la":40.7,"lo":72.3,"r":"СНГ"},
    "UTKF":{"n":"Фергана","c":"Узбекистан","la":40.4,"lo":71.7,"r":"СНГ"},
    "UTSB":{"n":"Бухара","c":"Узбекистан","la":39.8,"lo":64.5,"r":"СНГ"},
    "UCFM":{"n":"Бишкек","c":"Кыргызстан","la":42.9,"lo":74.6,"r":"СНГ"},
    "UAFM":{"n":"Бишкек","c":"Кыргызстан","la":42.9,"lo":74.6,"r":"СНГ"},
    "UAFO":{"n":"Ош","c":"Кыргызстан","la":40.5,"lo":72.8,"r":"СНГ"},
    "UCFO":{"n":"Ош","c":"Кыргызстан","la":40.5,"lo":72.8,"r":"СНГ"},
    "UTDD":{"n":"Душанбе","c":"Таджикистан","la":38.6,"lo":68.8,"r":"СНГ"},
    "UAAA":{"n":"Алматы","c":"Казахстан","la":43.3,"lo":77.0,"r":"СНГ"},
    "UACC":{"n":"Астана","c":"Казахстан","la":51.2,"lo":71.5,"r":"СНГ"},
    "UAII":{"n":"Шымкент","c":"Казахстан","la":42.3,"lo":69.7,"r":"СНГ"},
    "UTAA":{"n":"Ашхабад","c":"Туркменистан","la":38.0,"lo":58.4,"r":"БВ"},
    "LYBE":{"n":"Белград","c":"Сербия","la":44.8,"lo":20.5,"r":"Европа"},
    "LTFM":{"n":"Стамбул","c":"Турция","la":41.0,"lo":29.0,"r":"БВ"},
    "LTBA":{"n":"Стамбул","c":"Турция","la":41.0,"lo":29.0,"r":"БВ"},
    "LTAI":{"n":"Анталья","c":"Турция","la":36.9,"lo":30.7,"r":"БВ"},
    "LTBS":{"n":"Даламан","c":"Турция","la":36.8,"lo":28.8,"r":"БВ"},
    "LTBJ":{"n":"Измир","c":"Турция","la":38.3,"lo":27.2,"r":"БВ"},
    "OMDB":{"n":"Дубай","c":"ОАЭ","la":25.2,"lo":55.3,"r":"БВ"},
    "OMDW":{"n":"Дубай","c":"ОАЭ","la":24.9,"lo":55.2,"r":"БВ"},
    "OMAA":{"n":"Абу-Даби","c":"ОАЭ","la":24.5,"lo":54.4,"r":"БВ"},
    "OTHH":{"n":"Доха","c":"Катар","la":25.3,"lo":51.5,"r":"БВ"},
    "OERK":{"n":"Эр-Рияд","c":"Саудовская Аравия","la":24.7,"lo":46.7,"r":"БВ"},
    "OEDF":{"n":"Даммам","c":"Саудовская Аравия","la":26.5,"lo":49.8,"r":"БВ"},
    "OOMS":{"n":"Маскат","c":"Оман","la":23.6,"lo":58.4,"r":"БВ"},
    "OOSA":{"n":"Салала","c":"Оман","la":17.0,"lo":54.1,"r":"БВ"},
    "OBBI":{"n":"Манама","c":"Бахрейн","la":26.2,"lo":50.6,"r":"БВ"},
    "OKBK":{"n":"Эль-Кувейт","c":"Кувейт","la":29.4,"lo":48.0,"r":"БВ"},
    "LLBG":{"n":"Тель-Авив","c":"Израиль","la":32.1,"lo":34.8,"r":"БВ"},
    "OJAM":{"n":"Амман","c":"Иордания","la":31.9,"lo":35.9,"r":"БВ"},
    "OIIE":{"n":"Тегеран","c":"Иран","la":35.7,"lo":51.4,"r":"БВ"},
    "ORBI":{"n":"Багдад","c":"Ирак","la":33.3,"lo":44.2,"r":"БВ"},
    "VTBS":{"n":"Бангкок","c":"Таиланд","la":13.8,"lo":100.5,"r":"Азия"},
    "VTBD":{"n":"Бангкок","c":"Таиланд","la":13.8,"lo":100.5,"r":"Азия"},
    "VTSP":{"n":"Пхукет","c":"Таиланд","la":7.9,"lo":98.4,"r":"Азия"},
    "VTSK":{"n":"Краби","c":"Таиланд","la":8.1,"lo":98.9,"r":"Азия"},
    "VVTS":{"n":"Хошимин","c":"Вьетнам","la":10.8,"lo":106.6,"r":"Азия"},
    "VVCR":{"n":"Нячанг","c":"Вьетнам","la":12.2,"lo":109.2,"r":"Азия"},
    "VVNB":{"n":"Ханой","c":"Вьетнам","la":21.0,"lo":105.8,"r":"Азия"},
    "VVDN":{"n":"Дананг","c":"Вьетнам","la":16.1,"lo":108.2,"r":"Азия"},
    "VVPQ":{"n":"Фукуок","c":"Вьетнам","la":10.2,"lo":104.0,"r":"Азия"},
    "WADD":{"n":"Денпасар","c":"Индонезия (Бали)","la":-8.7,"lo":115.2,"r":"Азия"},
    "RPLL":{"n":"Манила","c":"Филиппины","la":14.6,"lo":121.0,"r":"Азия"},
    "ZBAA":{"n":"Пекин","c":"Китай","la":39.9,"lo":116.4,"r":"Азия"},
    "ZBAD":{"n":"Пекин","c":"Китай","la":39.5,"lo":116.4,"r":"Азия"},
    "ZSPD":{"n":"Шанхай","c":"Китай","la":31.2,"lo":121.5,"r":"Азия"},
    "ZSSS":{"n":"Шанхай","c":"Китай","la":31.2,"lo":121.3,"r":"Азия"},
    "ZUUU":{"n":"Чэнду","c":"Китай","la":30.6,"lo":104.1,"r":"Азия"},
    "ZJSY":{"n":"Санья","c":"Китай","la":18.3,"lo":109.5,"r":"Азия"},
    "ZYHB":{"n":"Харбин","c":"Китай","la":45.8,"lo":126.6,"r":"Азия"},
    "ZYTL":{"n":"Далянь","c":"Китай","la":38.9,"lo":121.6,"r":"Азия"},
    "ZGGG":{"n":"Гуанчжоу","c":"Китай","la":23.4,"lo":113.3,"r":"Азия"},
    "ZBLA":{"n":"Хайлар","c":"Китай","la":49.1,"lo":119.8,"r":"Азия"},
    "VHHH":{"n":"Гонконг","c":"Гонконг (КНР)","la":22.3,"lo":114.2,"r":"Азия"},
    "VMMC":{"n":"Макао","c":"Макао (КНР)","la":22.2,"lo":113.6,"r":"Азия"},
    "VIDP":{"n":"Дели","c":"Индия","la":28.6,"lo":77.2,"r":"Азия"},
    "VAGO":{"n":"Гоа","c":"Индия","la":15.3,"lo":73.9,"r":"Азия"},
    "VRMM":{"n":"Мале","c":"Мальдивы","la":4.2,"lo":73.5,"r":"Азия"},
    "VCBI":{"n":"Коломбо","c":"Шри-Ланка","la":6.9,"lo":79.9,"r":"Азия"},
    "ZMUB":{"n":"Улан-Батор","c":"Монголия","la":47.9,"lo":106.9,"r":"Азия"},
    "ZKPY":{"n":"Пхеньян","c":"КНДР","la":39.0,"lo":125.8,"r":"Азия"},
    "OAIX":{"n":"Кабул","c":"Афганистан","la":34.5,"lo":69.2,"r":"Азия"},
    "HECA":{"n":"Каир","c":"Египет","la":30.1,"lo":31.2,"r":"Африка"},
    "HEGN":{"n":"Хургада","c":"Египет","la":27.3,"lo":33.8,"r":"Африка"},
    "HESH":{"n":"Шарм-эш-Шейх","c":"Египет","la":27.9,"lo":34.3,"r":"Африка"},
    "HAAB":{"n":"Аддис-Абеба","c":"Эфиопия","la":9.0,"lo":38.7,"r":"Африка"},
    "DAAG":{"n":"Алжир","c":"Алжир","la":36.7,"lo":3.1,"r":"Африка"},
    "GMMN":{"n":"Касабланка","c":"Марокко","la":33.6,"lo":-7.6,"r":"Африка"},
    "FSIA":{"n":"Маэ","c":"Сейшелы","la":-4.6,"lo":55.5,"r":"Африка"},
    "HEAL":{"n":"Александрия","c":"Египет","la":30.9,"lo":29.7,"r":"Африка"},
    "SVMI":{"n":"Каракас","c":"Венесуэла","la":10.5,"lo":-66.9,"r":"ЛА"},
    "MUHA":{"n":"Гавана","c":"Куба","la":23.1,"lo":-82.4,"r":"ЛА"},
}

DEST_NAME_TO_ICAOS: dict[str, list[str]] = defaultdict(list)
for _icao, _info in DEST_INFO.items():
    DEST_NAME_TO_ICAOS[_info["n"]].append(_icao)


def icao_to_dest_name(arr_icao: str) -> str | None:
    if arr_icao[:2] in RU_ICAO_PREFIXES:
        return None
    info = DEST_INFO.get(arr_icao)
    return info["n"] if info else None


def load_current_routes() -> dict[str, list[str]]:
    if not ROUTES_FILE.exists():
        print("  routes.json не найден — начинаем с нуля", flush=True)
        return {}
    try:
        with open(ROUTES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        routes = data.get("routes", {})
        print(f"  Загружено: {len(routes)} городов, "
              f"{sum(len(v) for v in routes.values())} маршрутов", flush=True)
        return routes
    except Exception as e:
        print(f"  Ошибка чтения routes.json: {e}", flush=True)
        return {}


def make_airport_stages(current_routes: dict) -> tuple[list, list]:
    """
    Stage 1 — города с уже известными маршрутами (идут первыми в OpenSky).
    Stage 2 — новые города.
    Сортировка Stage 1 по убыванию количества маршрутов.
    """
    cities_with_routes = set(current_routes.keys())
    stage1, stage2 = [], []
    for icao, city in RU_AIRPORTS.items():
        if city in cities_with_routes:
            stage1.append((icao, city))
        else:
            stage2.append((icao, city))
    stage1.sort(key=lambda x: -len(current_routes.get(x[1], [])))
    print(f"  Stage 1 (известные города): {len(stage1)} аэропортов", flush=True)
    print(f"  Stage 2 (новые города):     {len(stage2)} аэропортов", flush=True)
    return stage1, stage2


def build_output(confirmed: dict[str, set[str]], now: datetime,
                 sources_used: list[str]) -> dict:
    routes: dict[str, list[str]] = {}
    used_icao: set[str] = set()
    for city, dest_names in confirmed.items():
        if not dest_names:
            continue
        routes[city] = sorted(dest_names)
        for name in dest_names:
            for icao in DEST_NAME_TO_ICAOS.get(name, []):
                used_icao.add(icao)
    destinations: dict[str, dict] = {}
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
        "routes":       routes,
        "destinations": destinations,
    }


# ── 1. AirLabs Routes API (PRIMARY) ───────────────────────────────────────────

def fetch_airlabs_routes(icao: str, api_key: str) -> list | None:
    """None = лимит исчерпан. [] = нет маршрутов. list = маршруты."""
    print(f"    → AirLabs dep_icao={icao}", flush=True)
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


def run_airlabs_primary(api_key: str,
                        all_airports: list[tuple[str, str]]) -> dict[str, set[str]]:
    """
    Первичный источник. Обрабатывает ВСЕ аэропорты.
    При достижении лимита сохраняет всё найденное до этого момента.
    """
    confirmed: dict[str, set[str]] = defaultdict(set)
    total = len(all_airports)

    for idx, (icao, city) in enumerate(all_airports, 1):
        print(f"\n[{idx}/{total}] {city} ({icao}) [AirLabs]", flush=True)
        arr_icaos = fetch_airlabs_routes(icao, api_key)
        if arr_icaos is None:
            print("  ⚠ AirLabs лимит — останавливаемся", flush=True)
            break

        new_dests: set[str] = set()
        for arr_icao in arr_icaos:
            name = icao_to_dest_name(arr_icao)
            if name:
                new_dests.add(name)

        existing = confirmed.get(city, set())
        merged   = existing | new_dests
        if merged:
            confirmed[city] = merged
        added = new_dests - existing
        if added:
            print(f"    + добавлено: {sorted(added)}", flush=True)
        print(f"    → итого для {city}: {len(confirmed.get(city, set()))} направлений",
              flush=True)
        time.sleep(2)

    total_routes = sum(len(v) for v in confirmed.values())
    print(f"\n  AirLabs завершён: {len(confirmed)} городов, {total_routes} маршрутов",
          flush=True)
    return dict(confirmed)


# ── 2. OpenSky Network (ADDITIVE ENRICHMENT) ──────────────────────────────────

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
        print("=== Кредиты OpenSky: неизвестно ===", flush=True)
        return None
    except Exception as e:
        print(f"=== Проверка кредитов не удалась: {e} ===", flush=True)
        return None


def fetch_opensky_day(icao: str, begin_ts: int, end_ts: int,
                      token_mgr: TokenManager) -> list | None:
    url    = "https://opensky-network.org/api/flights/departure"
    params = {"airport": icao, "begin": begin_ts, "end": end_ts}
    for attempt in range(2):
        try:
            r = requests.get(
                url, params=params,
                headers={"Authorization": f"Bearer {token_mgr.get_token()}"},
                timeout=(10, 15),
            )
            if r.status_code == 200:
                remaining = r.headers.get("X-Rate-Limit-Remaining")
                if remaining is not None and int(remaining) == 0:
                    print("    ⚠ Кредиты исчерпаны", flush=True)
                    return None
                return r.json() or []
            elif r.status_code == 404:
                return []
            elif r.status_code in (403, 429):
                print(f"    ⚠ HTTP {r.status_code} — останавливаем OpenSky", flush=True)
                return None
            elif r.status_code == 401:
                token_mgr._expires_at = 0
                time.sleep(2)
            else:
                return []
        except requests.exceptions.Timeout:
            time.sleep(5)
        except Exception as e:
            print(f"    Ошибка: {e}", flush=True)
            time.sleep(5)
    return []


def run_opensky_additive(token_mgr: TokenManager,
                         stage1: list, stage2: list,
                         confirmed: dict[str, set[str]],
                         now: datetime) -> dict[str, set[str]]:
    """
    Обогащение через реальные ADS-B данные. ТОЛЬКО ДОБАВЛЯЕТ маршруты.
    Никогда не удаляет то, что нашёл AirLabs.

    Stage 1 первым: ранний выход при верификации известных маршрутов экономит кредиты.
    """
    DAYS_HISTORY = 14
    MIN_FLIGHTS  = 2

    windows = []
    for i in range(1, DAYS_HISTORY + 1):
        day   = (now - timedelta(days=i)).date()
        begin = int(datetime(day.year, day.month, day.day,  0, 0, 0, tzinfo=timezone.utc).timestamp())
        end   = int(datetime(day.year, day.month, day.day, 23,59,59, tzinfo=timezone.utc).timestamp())
        windows.append((begin, end))

    result = {c: set(d) for c, d in confirmed.items()}
    route_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    added_total = 0

    all_airports = [("S1", icao, city) for icao, city in stage1] + \
                   [("S2", icao, city) for icao, city in stage2]
    total      = len(all_airports)
    prev_stage = None

    for seq_idx, (stage_tag, icao, city) in enumerate(all_airports, 1):
        if stage_tag != prev_stage:
            label = "Stage 1 (известные города)" if stage_tag == "S1" else "Stage 2 (новые города)"
            print(f"\n{'─'*42}\n  OpenSky: {label}\n{'─'*42}", flush=True)
            prev_stage = stage_tag

        print(f"\n[{seq_idx}/{total}] {city} ({icao}) [OpenSky]", flush=True)

        already_known = result.get(city, set())

        for begin_ts, end_ts in windows:
            result_day = fetch_opensky_day(icao, begin_ts, end_ts, token_mgr)
            if result_day is None:
                _apply_opensky_counts(route_counts, result, MIN_FLIGHTS)
                print(f"\n  OpenSky: лимит на [{seq_idx}/{total}], "
                      f"добавлено {added_total} направлений всего", flush=True)
                return result

            for f in result_day:
                arr = (f.get("estArrivalAirport") or "").strip().upper()
                if (arr and len(arr) == 4
                        and arr[:2] not in RU_ICAO_PREFIXES
                        and arr in DEST_INFO):
                    route_counts[city][arr] += 1

            # Stage 1: ранний выход если OpenSky уже подтвердил все known-маршруты
            if stage_tag == "S1" and already_known:
                verified = {
                    DEST_INFO[a]["n"]
                    for a, cnt in route_counts[city].items()
                    if cnt >= MIN_FLIGHTS
                }
                if already_known.issubset(verified):
                    print(f"    ✓ Все {len(already_known)} направлений верифицированы, "
                          f"прерываем дни", flush=True)
                    break
            time.sleep(5)

        new_for_city = {
            DEST_INFO[a]["n"]
            for a, cnt in route_counts[city].items()
            if cnt >= MIN_FLIGHTS
        }
        added_for_city = new_for_city - already_known
        if added_for_city:
            result[city] = already_known | new_for_city
            added_total += len(added_for_city)
            print(f"    + OpenSky добавил: {sorted(added_for_city)}", flush=True)
        else:
            print(f"    → OpenSky: {len(new_for_city)} найдено, новых нет", flush=True)
        time.sleep(3)

    print(f"\n  OpenSky завершён: добавлено {added_total} направлений", flush=True)
    return result


def _apply_opensky_counts(route_counts, result, min_flights):
    """Применяет накопленные counts к result при досрочном выходе из OpenSky."""
    for city, counts in route_counts.items():
        new_dests = {DEST_INFO[a]["n"] for a, cnt in counts.items() if cnt >= min_flights}
        existing  = result.get(city, set())
        added     = new_dests - existing
        if added:
            result[city] = existing | new_dests


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== Script started ===", flush=True)
    print(f"=== AirLabs: {'доступен' if AIRLABS_KEY else 'НЕ НАСТРОЕН ⚠'} ===",
          flush=True)
    print(f"=== OpenSky: {'доступен' if OPENSKY_CLIENT_ID else 'не настроен'} ===",
          flush=True)

    if not AIRLABS_KEY:
        print("✗ AIRLABS_KEY не задан — первичный источник недоступен", flush=True)
        sys.exit(1)

    now = datetime.now(timezone.utc)

    print("\n=== Загружаем текущий routes.json ===", flush=True)
    current_routes = load_current_routes()

    print("\n=== Подготовка этапов ===", flush=True)
    stage1, stage2 = make_airport_stages(current_routes)
    all_airports = stage1 + stage2

    sources_used: list[str] = []

    # ── 1. AirLabs — первичный источник ──────────────────────────────────────
    print(f"\n{'='*52}", flush=True)
    print(f"  1/2  AirLabs — первичный источник ({len(all_airports)} аэропортов)",
          flush=True)
    print(f"{'='*52}", flush=True)
    confirmed = run_airlabs_primary(AIRLABS_KEY, all_airports)
    if confirmed:
        sources_used.append("AirLabs")

    # ── 2. OpenSky — обогащение (только добавление) ───────────────────────────
    if OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET:
        print(f"\n{'='*52}", flush=True)
        print(f"  2/2  OpenSky — обогащение ({len(all_airports)} аэропортов)",
              flush=True)
        print(f"{'='*52}", flush=True)
        token_mgr = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
        try:
            token_mgr.get_token()
            credits = check_opensky_credits(token_mgr)
            if credits is None or credits > 0:
                before = sum(len(v) for v in confirmed.values())
                confirmed = run_opensky_additive(
                    token_mgr, stage1, stage2, confirmed, now
                )
                after = sum(len(v) for v in confirmed.values())
                if after > before:
                    sources_used.append("OpenSky Network")
            else:
                print("  OpenSky: кредиты исчерпаны, пропускаем", flush=True)
        except Exception as e:
            print(f"  ⚠ OpenSky: ошибка авторизации ({e}), пропускаем", flush=True)
    else:
        print("\n  2/2  OpenSky: не настроен, пропускаем", flush=True)

    # ── Сохранение ───────────────────────────────────────────────────────────
    if not confirmed:
        print("\n⚠ Ни одного направления не найдено, routes.json НЕ обновляется.",
              flush=True)
        sys.exit(0)

    output = build_output(confirmed, now, sources_used)

    with open(ROUTES_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_routes = sum(len(v) for v in output["routes"].values())
    prev_total   = sum(len(v) for v in current_routes.values())
    print(f"\n✓ Готово [{output['source']}]: "
          f"{len(output['routes'])} городов, {total_routes} маршрутов "
          f"(было {prev_total})")
    print(f"  Обновлено: {output['updated']}")


if __name__ == "__main__":
    main()
