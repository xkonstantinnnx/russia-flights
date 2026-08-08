#!/usr/bin/env python3
"""Генерирует CITY_INFO для карточки направления в index.html.

Что собирает по каждому городу назначения:
  q     — идентификатор Wikidata (для ссылки «Wikidata» в карточке);
  pop   — население (Wikidata P1082, свежайшее значение);
  tz    — часовой пояс в виде UTC±H (из IANA-зоны аэропорта, снэпшот Jonty);
  blurb — первые два предложения статьи ru.wikipedia.

Защита от промаха по названию. Русское название города из routes.json может
вести на страницу значений («Санья») или на однофамильца — поэтому у каждого
кандидата проверяются координаты Wikidata (P625) против координат
направления из routes.json: расходятся больше чем на 2° — кандидат
отбрасывается и пробуется следующий вариант заголовка.

Не часть автопайплайна: население и описания меняются медленно, запускать
вручную после появления новых направлений.

Запуск: python3 scripts/gen_city_info.py [--jonty путь] [--only Город,Город]
"""
import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

WIKI_API = "https://ru.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
JONTY_URL = "https://raw.githubusercontent.com/Jonty/airline-route-data/main/airline_routes.json"
UA = "russia-flights/1.0 (https://russia-flights.ru; gen_city_info.py)"

MAX_COORD_DIFF = 2.0  # градусов — грубая проверка «та ли это точка на карте»


def api_get(url: str, params: dict) -> dict:
    params = dict(params, format="json")
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params),
                                 headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.loads(r.read().decode("utf-8"))


def wiki_pages(titles: list[str]) -> dict[str, dict]:
    """{заголовок: {q, extract}} по списку заголовков ru.wikipedia."""
    out: dict[str, dict] = {}
    for i in range(0, len(titles), 20):
        chunk = titles[i:i + 20]
        data = api_get(WIKI_API, {
            "action": "query", "prop": "pageprops|extracts", "exintro": 1,
            "explaintext": 1, "exsentences": 5, "exlimit": "max", "redirects": 1,
            "titles": "|".join(chunk),
        })
        query = data.get("query", {})
        # redirects/normalized переводят исходный заголовок в конечный —
        # держим обратное отображение, чтобы вернуть результат по запросу.
        alias = {}
        for key in ("normalized", "redirects"):
            for item in query.get(key, []):
                alias[item["to"]] = alias.get(item["from"], item["from"])
        for page in query.get("pages", {}).values():
            title = page.get("title", "")
            src = alias.get(title, title)
            out[src] = {
                "q": (page.get("pageprops") or {}).get("wikibase_item"),
                "extract": (page.get("extract") or "").strip(),
            }
        time.sleep(0.3)
    return out


def wikidata_entities(qids: list[str]) -> dict[str, dict]:
    """{Q-id: {lat, lon, pop}} — координаты и население."""
    out: dict[str, dict] = {}
    for i in range(0, len(qids), 40):
        chunk = qids[i:i + 40]
        data = api_get(WIKIDATA_API, {
            "action": "wbgetentities", "ids": "|".join(chunk), "props": "claims",
        })
        for qid, ent in (data.get("entities") or {}).items():
            claims = ent.get("claims") or {}
            info: dict = {}
            coord = claims.get("P625")
            if coord:
                val = coord[0].get("mainsnak", {}).get("datavalue", {}).get("value") or {}
                if "latitude" in val:
                    info["lat"], info["lon"] = val["latitude"], val["longitude"]
            pops = []
            for c in claims.get("P1082", []):
                val = c.get("mainsnak", {}).get("datavalue", {}).get("value") or {}
                amount = val.get("amount")
                if not amount:
                    continue
                # Год из квалификатора P585 — нужен, чтобы взять свежую перепись.
                year = 0
                for q in (c.get("qualifiers") or {}).get("P585", []):
                    t = (q.get("datavalue") or {}).get("value", {}).get("time", "")
                    m = re.match(r"[+-](\d{4})", t)
                    if m:
                        year = int(m.group(1))
                pops.append((year, int(float(amount))))
            if pops:
                info["pop"] = max(pops)[1]
            out[qid] = info
        time.sleep(0.3)
    return out


ABBREV_TAIL = re.compile(r"(?:^|\s)[a-zа-яё]{1,5}\.$", re.IGNORECASE)


def clean_blurb(text: str, sentences: int = 2) -> str:
    """Первые предложения преамбулы статьи без транслитераций в скобках.

    Преамбулы ru.wikipedia начинаются с «Стамбу́л (тур. …, ранее …) — …»:
    скобки занимают половину фразы и обрываются на сокращении вроде «греч.»,
    поэтому сначала выкидываем скобочные вставки и ударения, и только потом
    режем на предложения.
    """
    text = text or ""
    while True:                                   # скобки бывают вложенными:
        stripped = re.sub(r"\([^()]*\)", "", text)   # «Византий (греч. …)» внутри
        if stripped == text:                      # внешней вставки «(тур. …)»
            break
        text = stripped
    text = text.replace("́", "").replace("\xa0", " ")
    text = re.sub(r"\s+([,.;:])", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()

    out: list[str] = []
    for part in re.split(r"(?<=\.)\s+", text):
        if not part:
            continue
        out.append(part)
        # Сокращение в конце («ок.», «им.») — предложение ещё не кончилось.
        if len(out) >= sentences and not ABBREV_TAIL.search(part):
            break
    blurb = " ".join(out).strip()
    # В карточке блок «О ГОРОДЕ» — абзац на пару строк, а не выжимка статьи:
    # если два предложения вышли длиннее ~320 символов, оставляем одно.
    if len(blurb) > 320 and len(out) > 1:
        blurb = out[0].strip()
    return blurb


def utc_offset(iana: str) -> str | None:
    try:
        off = datetime.now(ZoneInfo(iana)).utcoffset()
    except Exception:
        return None
    if off is None:
        return None
    total = int(off.total_seconds() // 60)
    sign = "+" if total >= 0 else "−"
    h, m = divmod(abs(total), 60)
    return f"UTC{sign}{h}" + (f":{m:02d}" if m else "")


def extract_js_map(html: str, name: str) -> dict:
    m = re.search(rf"const {name} = \{{(.*?)\n\}};", html, re.DOTALL)
    if not m:
        raise SystemExit(f"const {name} не найден в index.html")
    return dict(re.findall(r'"([^"]+)"\s*:\s*"([^"]+)"', m.group(1)))


def replace_between(text: str, start: str, end: str, content: str) -> str:
    pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start!r}/{end!r} не найдены в index.html")
    return pattern.sub(lambda _m: f"{start}\n{content}\n{end}", text, count=1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jonty", help="локальная копия airline_routes.json (для часовых поясов)")
    ap.add_argument("--only", help="ограничить список направлений (через запятую)")
    args = ap.parse_args()

    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    destinations = routes_data["destinations"]
    html = INDEX_PATH.read_text(encoding="utf-8")
    dest_iata = extract_js_map(html, "DEST_AIRPORT_IATA")

    names = sorted(destinations)
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        names = [n for n in names if n in wanted]

    # ── часовые пояса: IANA-зона аэропорта из снэпшота Jonty ──
    tz_by_dest: dict[str, str] = {}
    if args.jonty or True:
        try:
            if args.jonty:
                jonty = json.loads(Path(args.jonty).read_text(encoding="utf-8"))
            else:
                print("Скачиваем снэпшот Jonty для часовых поясов...", file=sys.stderr, flush=True)
                with urllib.request.urlopen(JONTY_URL, timeout=120) as r:
                    jonty = json.loads(r.read().decode("utf-8"))
        except Exception as e:
            print(f"  Jonty недоступен ({e}) — часовые пояса пропускаем", file=sys.stderr)
            jonty = {}
        for name in names:
            entry = jonty.get(dest_iata.get(name, ""), {})
            off = utc_offset(entry.get("timezone", "")) if entry else None
            if off:
                tz_by_dest[name] = off

    # ── Wikidata/Wikipedia: пробуем варианты заголовка, проверяем координаты ──
    result: dict[str, dict] = {}
    unresolved: list[str] = []
    pending = {n: [n, f"{n} (город)"] for n in names}

    for attempt in range(2):
        titles = [cands[attempt] for cands in pending.values() if len(cands) > attempt]
        if not titles:
            break
        pages = wiki_pages(titles)
        qids = [p["q"] for p in pages.values() if p.get("q")]
        ents = wikidata_entities(qids) if qids else {}

        still: dict[str, list[str]] = {}
        for name, cands in pending.items():
            if len(cands) <= attempt:
                continue
            page = pages.get(cands[attempt]) or {}
            qid = page.get("q")
            ent = ents.get(qid or "", {})
            meta = destinations[name]
            ok = (qid and "lat" in ent
                  and abs(ent["lat"] - meta["la"]) <= MAX_COORD_DIFF
                  and abs(ent["lon"] - meta["lo"]) <= MAX_COORD_DIFF)
            if not ok:
                still[name] = cands
                continue
            info: dict = {"q": qid}
            if ent.get("pop"):
                info["pop"] = ent["pop"]
            if tz_by_dest.get(name):
                info["tz"] = tz_by_dest[name]
            blurb = clean_blurb(page.get("extract") or "")
            if blurb and len(blurb) > 20:
                info["blurb"] = blurb
            result[name] = info
        pending = still

    unresolved = sorted(pending)
    for name in unresolved:                      # часовой пояс полезен и без статьи
        if tz_by_dest.get(name):
            result[name] = {"tz": tz_by_dest[name]}

    block = "\n".join([
        "// Карточки городов назначения: Wikidata (Q-id, население),",
        "// ru.wikipedia (описание), часовой пояс — из IANA-зоны аэропорта.",
        "// Пересобирается скриптом scripts/gen_city_info.py — не править вручную.",
        "const CITY_INFO = " + json.dumps(result, ensure_ascii=False, sort_keys=True) + ";",
    ])
    html = replace_between(html, "// CITY_INFO_START", "// CITY_INFO_END", block)
    INDEX_PATH.write_text(html, encoding="utf-8")

    with_pop = sum(1 for v in result.values() if v.get("pop"))
    with_blurb = sum(1 for v in result.values() if v.get("blurb"))
    with_tz = sum(1 for v in result.values() if v.get("tz"))
    print(f"CITY_INFO: {len(result)} из {len(names)} направлений "
          f"(население {with_pop}, описание {with_blurb}, часовой пояс {with_tz})")
    if unresolved:
        print(f"  не сопоставлены с Wikidata ({len(unresolved)}): {', '.join(unresolved)}")


if __name__ == "__main__":
    main()
