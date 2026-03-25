#!/usr/bin/env python3
"""
Обновление маршрутов из России — версия с Aviasales Data API.

Логика двух этапов:
  Stage 1: аэропорты городов у которых УЖЕ есть маршруты в routes.json
           → верифицируем существующие направления + ищем новые
  Stage 2: аэропорты городов без маршрутов в routes.json
           → ищем новые направления

Источники (по приоритету):
  1. OpenSky Network (OAuth2, ADS-B данные за 14 дней)
  2. Aviasales Data API (кэш поисков за 7 дней, бесплатно)
  3. AirLabs Routes API (если оба предыдущих исчерпаны/не дали результата)

Правила:
  - Направление подтверждено хотя бы одним источником → включается в итог
  - Направление не найдено ни одним источником → удаляется из итога
  - Если OpenSky исчерпан на Stage 1 → Aviasales + AirLabs завершают Stage 1, затем Stage 2
  - Найденное одним источником НЕ перепроверяется вторым
  - Aviasales: если кэш пуст для направления → уходит в AirLabs (не означает отсутствие рейса)

Экономия кредитов:
  - Stage 1 обрабатывается первым
  - Ранний выход из дней когда все текущие маршруты города уже подтверждены
  - RU-префиксы отсеиваются до проверки в DEST_INFO
"""

import os, json, time, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

OPENSKY_CLIENT_ID     = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
AIRLABS_KEY           = os.environ.get("AIRLABS_KEY", "")
TRAVELPAYOUTS_TOKEN   = os.environ.get("TRAVELPAYOUTS_TOKEN", "")

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

# ── Aviasales: ICAO → IATA для российских аэропортов ──────────────────────────
RU_AIRPORT_IATA: dict[str, str] = {
    "UUEE": "SVO", "UUDD": "DME", "UUWW": "VKO",
    "ULLI": "LED", "URSS": "AER", "URKK": "KRR",
    "URMM": "MRV", "USSS": "SVX", "USCC": "CEK",
    "UWOO": "REN", "UWUU": "UFA", "UWKD": "KZN",
    "UWGG": "GOJ", "USPP": "PEE", "UWWW": "KUF",
    "URWW": "VOG", "UWSS": "RTW", "USTR": "TJM",
    "UNOO": "OMS", "USRR": "SGC", "USNN": "NJC",
    "UNNT": "OVB", "UNKL": "KJA", "UIII": "IKT",
    "UIUU": "UUD", "UHHH": "KHV", "UHWW": "VVO",
    "UHBB": "BQS", "ULMM": "MMK", "UIAA": "HTA",
    "UHSS": "UUS",
}

# ── Aviasales: IATA направления → имя города (как в DEST_INFO) ────────────────
DEST_IATA_TO_NAME: dict[str, str] = {
    "MSQ": "Минск",    "EVN": "Ереван",   "TBS": "Тбилиси",
    "BUS": "Батуми",   "KUT": "Кутаиси",  "GYD": "Баку",
    "TAS": "Ташкент",  "SKD": "Самарканд","NVI": "Навои",
    "NMA": "Наманган", "AZN": "Андижан",  "FEG": "Фергана",
    "BHK": "Бухара",   "FRU": "Бишкек",   "OSS": "Ош",
    "DYU": "Душанбе",  "ALA": "Алматы",   "NQZ": "Астана",
    "CIT": "Шымкент",  "ASB": "Ашхабад",  "BEG": "Белград",
    "IST": "Стамбул",  "SAW": "Стамбул",  "AYT": "Анталья",
    "DLM": "Даламан",  "ADB": "Измир",
    "DXB": "Дубай",    "DWC": "Дубай",    "AUH": "Абу-Даби",
    "DOH": "Доха",     "RUH": "Эр-Рияд",  "DMM": "Даммам",
    "MCT": "Маскат",   "SLL": "Салала",   "BAH": "Манама",
    "KWI": "Эль-Кувейт","TLV": "Тель-Авив","AMM": "Амман",
    "IKA": "Тегеран",  "BGW": "Багдад",
    "BKK": "Бангкок",  "DMK": "Бангкок",  "HKT": "Пхукет",
    "KBV": "Краби",    "SGN": "Хошимин",  "CXR": "Нячанг",
    "HAN": "Ханой",    "DAD": "Дананг",   "PQC": "Фукуок",
    "DPS": "Денпасар", "MNL": "Манила",
    "PEK": "Пекин",    "PKX": "Пекин",    "PVG": "Шанхай",
    "SHA": "Шанхай",   "CTU": "Чэнду",    "SYX": "Санья",
    "HRB": "Харбин",   "DLC": "Далянь",   "CAN": "Гуанчжоу",
    "HLD": "Хайлар",   "HKG": "Гонконг",  "MFM": "Макао",
    "DEL": "Дели",     "GOI": "Гоа",      "MLE": "Мале",
    "CMB": "Коломбо",  "ULN": "Улан-Батор","FNJ": "Пхеньян",
    "KBL": "Кабул",
    "CAI": "Каир",     "HRG": "Хургада",  "SSH": "Шарм-эш-Шейх",
    "ADD": "Аддис-Абеба","ALG": "Алжир",  "CMN": "Касабланка",
    "SEZ": "Маэ",      "HBE": "Александрия",
    "CCS": "Каракас",  "HAV": "Гавана",
}


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
    cities_with_routes = set(current_routes.keys())
    stage1, stage2 = [], []
    for icao, city in RU_AIRPORTS.items():
        if city in cities_with_routes:
            stage1.append((icao, city))
        else:
            stage2.append((icao, city))
    # Города с большим количеством маршрутов — в приоритете
    stage1.sort(key=lambda x: -len(current_routes.get(x[1], [])))
    print(f"  Stage 1 (верификация): {len(stage1)} аэропортов", flush=True)
    print(f"  Stage 2 (поиск новых): {len(stage2)} аэропортов", flush=True)
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


def run_opensky(token_mgr: TokenManager,
                stage1: list, stage2: list,
                current_routes: dict, now: datetime) -> tuple[dict, list]:
    DAYS_HISTORY = 14
    MIN_FLIGHTS  = 2

    windows = []
    for i in range(1, DAYS_HISTORY + 1):
        day   = (now - timedelta(days=i)).date()
        begin = int(datetime(day.year, day.month, day.day,  0, 0, 0, tzinfo=timezone.utc).timestamp())
        end   = int(datetime(day.year, day.month, day.day, 23,59,59, tzinfo=timezone.utc).timestamp())
        windows.append((begin, end))

    confirmed:    dict[str, set[str]]       = defaultdict(set)
    route_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    credits_gone  = False
    fallback:     list[tuple[str, str]]     = []

    all_airports = [("S1", icao, city) for icao, city in stage1] + \
                   [("S2", icao, city) for icao, city in stage2]

    total      = len(all_airports)
    prev_stage = None

    for seq_idx, (stage_tag, icao, city) in enumerate(all_airports, 1):
        if stage_tag != prev_stage:
            label = "Верификация текущих маршрутов" if stage_tag == "S1" else "Поиск новых маршрутов"
            print(f"\n{'─'*42}\n  {label} (OpenSky)\n{'─'*42}", flush=True)
            prev_stage = stage_tag

        print(f"\n[{seq_idx}/{total}] {city} ({icao}) [{stage_tag}]", flush=True)

        if credits_gone:
            fallback.append((icao, city))
            continue

        current_dests = set(current_routes.get(city, []))

        for begin_ts, end_ts in windows:
            result = fetch_opensky_day(icao, begin_ts, end_ts, token_mgr)
            if result is None:
                credits_gone = True
                fallback.append((icao, city))
                break
            for f in result:
                arr = (f.get("estArrivalAirport") or "").strip().upper()
                if arr and len(arr) == 4 and arr[:2] not in RU_ICAO_PREFIXES and arr in DEST_INFO:
                    route_counts[city][arr] += 1
            # Ранний выход: все текущие направления подтверждены
            if stage_tag == "S1" and current_dests:
                already = {
                    DEST_INFO[a]["n"]
                    for a, cnt in route_counts[city].items()
                    if cnt >= MIN_FLIGHTS
                }
                if current_dests.issubset(already):
                    print(f"    ✓ Все {len(current_dests)} текущих подтверждены, "
                          f"прерываем", flush=True)
                    break
            time.sleep(5)

        if credits_gone:
            for _, r_icao, r_city in all_airports[seq_idx:]:
                fallback.append((r_icao, r_city))
            break

        for arr_icao, count in route_counts[city].items():
            if count >= MIN_FLIGHTS:
                confirmed[city].add(DEST_INFO[arr_icao]["n"])

        found = len(confirmed[city])
        was   = len(current_dests) if stage_tag == "S1" else 0
        suffix = f"(было {was})" if stage_tag == "S1" else "(новый город)"
        print(f"    → найдено: {found} направлений {suffix}", flush=True)

        # Stage1 с 0 результатом → AirLabs
        if stage_tag == "S1" and found == 0:
            fallback.append((icao, city))

        time.sleep(3)

    print(f"\n  OpenSky: {len(confirmed)} городов с маршрутами, "
          f"{len(fallback)} в fallback", flush=True)
    return dict(confirmed), fallback


# ── Aviasales Data API ─────────────────────────────────────────────────────────

def fetch_aviasales_directions(origin_iata: str, token: str) -> set[str]:
    """
    Возвращает множество IATA-кодов направлений из кэша Aviasales
    (данные за последние 7 дней поисков пользователей).
    Пустой результат означает, что кэша нет — не то, что рейсов нет.
    """
    try:
        r = requests.get(
            "https://api.travelpayouts.com/aviasales/v3/get_popular_directions",
            params={
                "origin":   origin_iata,
                "currency": "RUB",
                "locale":   "ru",
                "limit":    100,
                "token":    token,
            },
            timeout=(10, 15),
        )
        if r.status_code == 200:
            data = r.json()
            # Структура ответа: {"data": {"destination": [...], "origin": {...}}}
            dest_list = (
                data.get("data", {}).get("destination")
                or data.get("data", {}).get("origin", [])
            )
            if isinstance(dest_list, list):
                return {
                    item.get("city_iata", "").upper()
                    for item in dest_list
                    if item.get("city_iata")
                }
        elif r.status_code == 429:
            print("    Aviasales: лимит запросов", flush=True)
        else:
            print(f"    Aviasales HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"    Aviasales ошибка: {e}", flush=True)
    return set()


def run_aviasales_data(
    token: str,
    airports: list[tuple[str, str]],
    confirmed: dict[str, set[str]],
) -> tuple[dict[str, set[str]], list[tuple[str, str]]]:
    """
    Проверяет маршруты через Aviasales Data API.
    Возвращает:
      - обновлённый confirmed (объединение с OpenSky-результатами)
      - remaining: аэропорты без результата в кэше → уходят в AirLabs
    """
    result:    dict[str, set[str]]   = {c: set(d) for c, d in confirmed.items()}
    remaining: list[tuple[str, str]] = []
    total = len(airports)

    for idx, (icao, city) in enumerate(airports, 1):
        iata = RU_AIRPORT_IATA.get(icao)
        if not iata:
            print(f"\n[{idx}/{total}] {city} ({icao}) — нет IATA-маппинга, → AirLabs",
                  flush=True)
            remaining.append((icao, city))
            continue

        print(f"\n[{idx}/{total}] {city} ({icao}/{iata}) [Aviasales]", flush=True)
        dest_iatas = fetch_aviasales_directions(iata, token)

        new_dests = {
            DEST_IATA_TO_NAME[d]
            for d in dest_iatas
            if d in DEST_IATA_TO_NAME
        }

        if not new_dests:
            print("    → кэш пуст, → AirLabs", flush=True)
            remaining.append((icao, city))
            time.sleep(1)
            continue

        existing = result.get(city, set())
        merged   = existing | new_dests
        result[city] = merged
        added = new_dests - existing
        if added:
            print(f"    + Aviasales добавил: {sorted(added)}", flush=True)
        print(f"    → {len(new_dests)} направлений из кэша", flush=True)
        time.sleep(1)   # лимит 600 req/час — 1 сек достаточно

    print(f"\n  Aviasales: покрыто {len(result) - len(confirmed)} новых городов, "
          f"{len(remaining)} → AirLabs", flush=True)
    return result, remaining


# ── AirLabs Routes API ─────────────────────────────────────────────────────────

def fetch_airlabs_routes(icao: str, api_key: str) -> list | None:
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


def run_airlabs(api_key: str, fallback: list,
                confirmed: dict[str, set[str]],
                current_routes: dict) -> dict[str, set[str]]:
    """
    Обрабатывает fallback через AirLabs.
    Объединяет с confirmed — уже найденное НЕ заменяется.
    """
    result   = {city: set(dests) for city, dests in confirmed.items()}
    city_buf: dict[str, list[str]] = defaultdict(list)

    total = len(fallback)
    for idx, (icao, city) in enumerate(fallback, 1):
        print(f"\n[{idx}/{total}] {city} ({icao}) [AirLabs]", flush=True)
        arr_icaos = fetch_airlabs_routes(icao, api_key)
        if arr_icaos is None:
            print("  AirLabs лимит — останавливаемся", flush=True)
            break
        city_buf[city].extend(arr_icaos)
        print(f"    → {len(arr_icaos)} маршрутов", flush=True)
        time.sleep(2)

    for city, arr_icaos in city_buf.items():
        new_dests: set[str] = set()
        seen: set[str]      = set()
        for arr_icao in arr_icaos:
            name = icao_to_dest_name(arr_icao)
            if name and name not in seen:
                new_dests.add(name)
                seen.add(name)
        existing = result.get(city, set())
        merged   = existing | new_dests
        if merged:
            result[city] = merged
            added = new_dests - existing
            if added:
                print(f"    + AirLabs добавил для {city}: {sorted(added)}", flush=True)

    print(f"\n  AirLabs завершён: итого {len(result)} городов", flush=True)
    return result


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== Script started ===", flush=True)

    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        print("✗ OPENSKY_CLIENT_ID и OPENSKY_CLIENT_SECRET не заданы", flush=True)
        sys.exit(1)

    print(f"=== CLIENT_ID length: {len(OPENSKY_CLIENT_ID)}, "
          f"CLIENT_SECRET length: {len(OPENSKY_CLIENT_SECRET)} ===", flush=True)
    print(f"=== Aviasales: {'доступен' if TRAVELPAYOUTS_TOKEN else 'не настроен'} ===",
          flush=True)
    print(f"=== AirLabs: {'доступен' if AIRLABS_KEY else 'не настроен'} ===", flush=True)

    now = datetime.now(timezone.utc)

    print("\n=== Загружаем текущий routes.json ===", flush=True)
    current_routes = load_current_routes()

    print("\n=== Подготовка этапов ===", flush=True)
    stage1, stage2 = make_airport_stages(current_routes)

    token_mgr = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
    try:
        print("\n=== Requesting token... ===", flush=True)
        token_mgr.get_token()
    except Exception as e:
        print(f"✗ Не удалось получить токен: {e}", flush=True)
        sys.exit(1)

    print("=== Проверка кредитов OpenSky... ===", flush=True)
    credits = check_opensky_credits(token_mgr)
    opensky_has_credits = (credits is None or credits > 0)

    confirmed:    dict[str, set[str]]   = {}
    fallback:     list[tuple[str, str]] = []
    sources_used: list[str]             = []

    if opensky_has_credits:
        print("\n=== OpenSky Network ===", flush=True)
        confirmed, fallback = run_opensky(
            token_mgr, stage1, stage2, current_routes, now
        )
        if confirmed:
            sources_used.append("OpenSky Network")
    else:
        print("\n=== OpenSky: нет кредитов — всё в следующие источники ===", flush=True)
        fallback = stage1 + stage2

    # ── Aviasales Data API (промежуточный фильтр перед AirLabs) ───────────────
    airlabs_fallback = fallback

    if fallback and TRAVELPAYOUTS_TOKEN:
        print(f"\n=== Aviasales Data API ({len(fallback)} аэропортов) ===", flush=True)
        confirmed, airlabs_fallback = run_aviasales_data(
            TRAVELPAYOUTS_TOKEN, fallback, confirmed
        )
        if len(airlabs_fallback) < len(fallback):
            sources_used.append("Aviasales Data API")
    elif fallback and not TRAVELPAYOUTS_TOKEN:
        print("\n  Aviasales: токен не задан (TRAVELPAYOUTS_TOKEN), пропускаем",
              flush=True)

    # ── AirLabs (финальный fallback) ──────────────────────────────────────────
    if airlabs_fallback:
        if not AIRLABS_KEY:
            print(f"\n⚠ AirLabs не настроен, {len(airlabs_fallback)} аэропортов "
                  f"остаются без данных.", flush=True)
        else:
            print(f"\n=== AirLabs ({len(airlabs_fallback)} аэропортов) ===", flush=True)
            confirmed = run_airlabs(
                AIRLABS_KEY, airlabs_fallback, confirmed, current_routes
            )
            sources_used.append("AirLabs")

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
