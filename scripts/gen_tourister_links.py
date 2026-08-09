#!/usr/bin/env python3
"""Собирает прямые ссылки на страницы городов назначения на Туристер.Ру.

Зачем отдельный скрипт: URL страницы вида
https://www.tourister.ru/world/africa/tanzania-united-republic-of/region/zanzibar
из названия города не строится — там свои слаги континента, страны и региона.
Поэтому адрес ищется через поиск по сайту (?poisk=<город>&c=all) и проверяется
по самой странице: название города — в <title> или в слаге, страна из
routes.json — в тексте страницы. Не прошедшие проверку в блок не попадают — в карточке
для них останется ссылка на поиск по сайту (см. index.html, «УЗНАТЬ БОЛЬШЕ»).

Не часть автопайплайна: страницы Туристера живут годами, запускать вручную
после появления новых направлений.

Запуск: python3 scripts/gen_tourister_links.py [--only Город,Город] [--check]
  --check   только проверить уже сохранённые ссылки, ничего не искать заново.
"""
import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

SEARCH_URL = "https://www.tourister.ru/search?poisk={q}&c=all"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 russia-flights/gen_tourister_links.py")
PAUSE = 1.0                      # пауза между запросами к Туристеру
MAX_CANDIDATES = 6               # сколько результатов поиска проверять открытием

# Страна в routes.json и на Туристере называется по-разному — здесь варианты,
# любой из которых в <title> считается подтверждением.
COUNTRY_ALIASES = {
    "ОАЭ": ["ОАЭ", "Объединенные Арабские Эмираты", "Объединённые Арабские Эмираты", "Эмираты"],
    "Гонконг (КНР)": ["Гонконг", "Китай"],
    "Индонезия (Бали)": ["Индонезия", "Бали"],
    "Кыргызстан": ["Киргизия", "Кыргызстан"],
    "КНДР": ["КНДР", "Северная Корея", "Корея"],
    "Беларусь": ["Белоруссия", "Беларусь"],
    "Абхазия": ["Абхазия", "Грузия"],
    "Шри-Ланка": ["Шри-Ланка", "Шри Ланка"],
    "Сейшелы": ["Сейшелы", "Сейшельские Острова", "Сейшельские острова"],
    "Туркменистан": ["Туркмения", "Туркменистан"],
}

RESULT_RE = re.compile(
    r'<div class="rpoisk2"><a[^>]*href="(https://www\.tourister\.ru/[^"]+)"[^>]*>(.*?)</a>',
    re.S)
PAGE_RE = re.compile(r'^https://www\.tourister\.ru/world/[^/]+/[^/]+/(city|region)/[^/?#]+$')
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "replace")


def unescape(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s)
    for a, b in (("&mdash;", "—"), ("&ndash;", "–"), ("&nbsp;", " "),
                 ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'")):
        s = s.replace(a, b)
    return " ".join(s.split())


def norm(s: str) -> str:
    """Для сравнения названий: без ё, дефисов и регистра."""
    return s.lower().replace("ё", "е").replace("-", " ").replace("  ", " ").strip()


def stem(city: str) -> str:
    """Основа названия без последней буквы: в заголовках Туристера город часто
    стоит в косвенном падеже («Отдых в Дубае», «Что посмотреть в Анталье»)."""
    base = norm(city)
    return base[:-1] if len(base) >= 5 else base


def translit(city: str) -> str:
    table = {"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z",
             "и":"i","й":"i","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r",
             "с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"c","ч":"ch","ш":"sh","щ":"sch",
             "ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya","-":"_"," ":"_"}
    return "".join(table.get(c, c) for c in city.lower())


def page_matches(html: str, url: str, city: str, country: str) -> tuple[bool, str]:
    """Проверка, что открытая страница — про нужный город.

    Название в <title> Туристера часто стоит в косвенном падеже и не всегда
    совпадает с нашим написанием («Отдых в Анталии» против «Анталья»), поэтому
    город подтверждается либо основой названия в начале заголовка, либо
    совпадением слага с транслитерацией. Страна проверяется по тексту страницы целиком: в
    заголовке её может не быть («Отдых в Дубае»), а в хлебных крошках и
    боковых блоках она есть всегда. Без проверки страны поиск подсовывает
    однофамильцев — «Дуба, Саудовская Аравия» на запрос «Дубай».
    """
    m = TITLE_RE.search(html)
    title = unescape(m.group(1)) if m else ""
    slug = url.rsplit("/", 1)[-1]
    # Название ищем только в начале заголовка — до тире или запятой, за
    # которыми идёт страна. Иначе «Тольга, Алжир» проходит проверку как
    # «Алжир», а «Тайпо, Гонконг» — как «Гонконг».
    head = norm(re.split(r"[—,|:]", title)[0])
    city_ok = stem(city) in head or slug == translit(city)
    text = re.sub(r"<[^>]+>", " ", html)
    country_ok = any(a in text for a in COUNTRY_ALIASES.get(country, [country]))
    return (city_ok and country_ok), title


def country_paths(result: dict, dests: dict) -> dict:
    """Слаг континента и страны («asia/china») по уже найденным ссылкам —
    нужен, чтобы пробовать адрес напрямую, когда поиск по сайту промахнулся."""
    paths = {}
    for name, url in result.items():
        c = dests.get(name, {}).get("c")
        m = re.match(r"https://www\.tourister\.ru/world/([^/]+/[^/]+)/", url)
        if c and m:
            paths.setdefault(c, m.group(1))
    return paths


def probe_direct(city: str, country: str, paths: dict, log) -> str | None:
    """Запасной путь: собрать адрес из транслитерации названия и слага страны.
    Поиск Туристера иногда не находит город вовсе («Ош», «Санья») — но
    страница при этом существует по предсказуемому адресу."""
    base = paths.get(country)
    if not base:
        return None
    for kind in ("city", "region"):
        url = f"https://www.tourister.ru/world/{base}/{kind}/{translit(city)}"
        time.sleep(PAUSE)
        try:
            ok, title = page_matches(fetch(url), url, city, country)
        except Exception:                        # noqa: BLE001 — 404 это норма
            continue
        if ok:
            log(f"  ✓ {url}  (прямой адрес: {title[:50]})")
            return url
    return None


def find_link(city: str, country: str, log) -> str | None:
    try:
        html = fetch(SEARCH_URL.format(q=urllib.parse.quote(city)))
    except Exception as e:                       # noqa: BLE001 — сеть, не наша логика
        log(f"  поиск не удался: {e}")
        return None

    seen, cands = set(), []
    for url, raw_title in RESULT_RE.findall(html):
        if not PAGE_RE.match(url) or url in seen:
            continue
        seen.add(url)
        title = unescape(raw_title)
        # Ранжирование: совпадение слага с транслитерацией названия — самый
        # надёжный признак, дальше упоминание города в заголовке и тип страницы.
        slug = url.rsplit("/", 1)[-1]
        slug_hit = slug == translit(city)
        title_hit = stem(city) in norm(title)
        is_city = "/city/" in url
        cands.append((not slug_hit, not title_hit, not is_city, len(url), url, title))
    cands.sort()

    for *_, url, title in cands[:MAX_CANDIDATES]:
        time.sleep(PAUSE)
        try:
            ok, page_title = page_matches(fetch(url), url, city, country)
        except Exception as e:                   # noqa: BLE001
            log(f"  {url} — не открылась: {e}")
            continue
        if ok:
            log(f"  ✓ {url}  ({page_title[:60]})")
            return url
        log(f"  ✗ {url} — не подтверждается: {page_title[:60]}")
    return None


def replace_between(html: str, start: str, end: str, block: str) -> str:
    i, j = html.find(start), html.find(end)
    if i < 0 or j < 0:
        sys.exit(f"В index.html не найдены маркеры {start} / {end}")
    return html[:i] + start + "\n" + block + "\n" + html[j:]


def read_existing(html: str) -> dict:
    m = re.search(r"const TOURISTER = (\{.*?\});", html, re.S)
    return json.loads(m.group(1)) if m else {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="список городов через запятую")
    ap.add_argument("--check", action="store_true",
                    help="только проверить сохранённые ссылки")
    args = ap.parse_args()

    dests = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))["destinations"]
    html = INDEX_PATH.read_text(encoding="utf-8")
    result = read_existing(html)

    names = sorted(dests)
    if args.only:
        names = [n.strip() for n in args.only.split(",") if n.strip()]

    if args.check:
        bad = []
        for name in names:
            url = result.get(name)
            if not url:
                continue
            time.sleep(PAUSE)
            try:
                ok, title = page_matches(fetch(url), url, name, dests[name]["c"])
            except Exception as e:               # noqa: BLE001
                title, ok = str(e), False
            print(f"{'OK ' if ok else 'BAD'} {name}: {url} — {title[:70]}")
            if not ok:
                bad.append(name)
        print(f"\nпроверено {len(names)}, проблемных {len(bad)}"
              + (f": {', '.join(bad)}" if bad else ""))
        return

    for name in names:
        if name in result and not args.only:
            continue
        print(f"{name} ({dests[name]['c']}):")
        country = dests[name]["c"]
        url = (find_link(name, country, print)
               or probe_direct(name, country, country_paths(result, dests), print))
        if url:
            result[name] = url
        else:
            result.pop(name, None)
            print("  не найдено — останется ссылка на поиск по сайту")
        time.sleep(PAUSE)

    result = {k: v for k, v in sorted(result.items()) if k in dests}
    block = "\n".join([
        "// Страницы городов на Туристер.Ру для ссылки в карточке направления.",
        "// Пересобирается скриптом scripts/gen_tourister_links.py — не править вручную.",
        "const TOURISTER = " + json.dumps(result, ensure_ascii=False, sort_keys=True) + ";",
    ])
    html = replace_between(html, "// TOURISTER_START", "// TOURISTER_END", block)
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"\nTOURISTER: {len(result)} ссылок из {len(dests)} направлений")
    missing = [n for n in sorted(dests) if n not in result]
    if missing:
        print(f"  без прямой ссылки ({len(missing)}): {', '.join(missing)}")


if __name__ == "__main__":
    main()
